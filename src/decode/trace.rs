//! Decode the recursive call tree returned by `starknet_traceTransaction`.
//!
//! Maps `starknet::core::types::TransactionTrace` to a snbeat-flavored
//! `DecodedTrace` that mirrors the RPC tree but with each node enriched
//! with ABI-resolved function names, definitions, and decoded events.
//!
//! Unlike the multicall-decode path (which has to resolve every contract's
//! class hash at the tx's block), each `FunctionInvocation` node already
//! carries its own `class_hash`, so we pre-warm ABIs by class hash directly.

use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::StreamExt;
use starknet::core::types::{
    CallType, EntryPointType, ExecuteInvocation, Felt, FunctionInvocation, OrderedEvent,
    OrderedMessage, TransactionTrace,
};

use super::AbiRegistry;
use super::abi::{FunctionDef, ParsedAbi};
use super::events::{DecodedEvent, decode_event};
use crate::data::types::SnEvent;

/// One node in the decoded call tree. Mirrors `FunctionInvocation` with
/// extra ABI-resolved fields. Field naming matches `RawCall` where possible
/// so existing render helpers (`param_display`, `price`) work unchanged.
#[derive(Debug, Clone)]
pub struct DecodedTraceCall {
    pub contract_address: Felt,
    pub class_hash: Felt,
    pub caller_address: Felt,
    pub entry_point_selector: Felt,
    pub entry_point_type: EntryPointType,
    pub call_type: CallType,
    pub calldata: Vec<Felt>,
    pub result: Vec<Felt>,
    pub is_reverted: bool,
    pub function_name: Option<String>,
    pub function_def: Option<FunctionDef>,
    pub contract_abi: Option<Arc<ParsedAbi>>,
    pub events: Vec<DecodedEvent>,
    pub messages: Vec<OrderedMessage>,
    pub inner: Vec<DecodedTraceCall>,
}

/// Top-level decoded trace, structured per tx kind.
#[derive(Debug, Clone, Default)]
pub struct DecodedTrace {
    pub validate: Option<DecodedTraceCall>,
    pub execute: Option<DecodedTraceCall>,
    pub constructor: Option<DecodedTraceCall>,
    pub fee_transfer: Option<DecodedTraceCall>,
    pub l1_handler: Option<DecodedTraceCall>,
    pub revert_reason: Option<String>,
    /// Total nodes in the tree (validate + execute + fee_transfer + nested),
    /// computed once at decode time so the tabs bar doesn't walk the whole
    /// tree on every frame just to render the count.
    pub total_nodes: usize,
}

impl DecodedTrace {
    /// Iterate over the present root invocations in display order:
    /// validate → execute (or constructor / l1_handler) → fee_transfer.
    pub fn roots(&self) -> Vec<(&'static str, &DecodedTraceCall)> {
        let mut out: Vec<(&'static str, &DecodedTraceCall)> = Vec::new();
        if let Some(v) = &self.validate {
            out.push(("validate", v));
        }
        if let Some(c) = &self.constructor {
            out.push(("constructor", c));
        }
        if let Some(e) = &self.execute {
            out.push(("execute", e));
        }
        if let Some(h) = &self.l1_handler {
            out.push(("l1_handler", h));
        }
        if let Some(f) = &self.fee_transfer {
            out.push(("fee_transfer", f));
        }
        out
    }

    /// Walk every node in the tree (depth-first, in display order).
    pub fn for_each_call<F: FnMut(&DecodedTraceCall)>(&self, mut f: F) {
        fn walk<F: FnMut(&DecodedTraceCall)>(n: &DecodedTraceCall, f: &mut F) {
            f(n);
            for c in &n.inner {
                walk(c, f);
            }
        }
        for (_, root) in self.roots() {
            walk(root, &mut f);
        }
    }
}

/// Collect every unique class hash referenced in the trace tree.
fn collect_class_hashes(inv: &FunctionInvocation, out: &mut HashSet<Felt>) {
    out.insert(inv.class_hash);
    for child in &inv.calls {
        collect_class_hashes(child, out);
    }
}

/// Collect every root `FunctionInvocation` reference present in a trace.
fn root_invocations(trace: &TransactionTrace) -> Vec<&FunctionInvocation> {
    let mut out: Vec<&FunctionInvocation> = Vec::new();
    match trace {
        TransactionTrace::Invoke(t) => {
            if let Some(v) = t.validate_invocation.as_ref() {
                out.push(v);
            }
            if let ExecuteInvocation::Success(ref e) = t.execute_invocation {
                out.push(e);
            }
            if let Some(f) = t.fee_transfer_invocation.as_ref() {
                out.push(f);
            }
        }
        TransactionTrace::DeployAccount(t) => {
            out.push(&t.constructor_invocation);
            if let Some(v) = t.validate_invocation.as_ref() {
                out.push(v);
            }
            if let Some(f) = t.fee_transfer_invocation.as_ref() {
                out.push(f);
            }
        }
        TransactionTrace::Declare(t) => {
            if let Some(v) = t.validate_invocation.as_ref() {
                out.push(v);
            }
            if let Some(f) = t.fee_transfer_invocation.as_ref() {
                out.push(f);
            }
        }
        TransactionTrace::L1Handler(t) => {
            if let ExecuteInvocation::Success(ref e) = t.function_invocation {
                out.push(e);
            }
        }
    }
    out
}

/// Pre-warm parsed ABIs for every class hash in the trace, with bounded
/// concurrency so a pathological tx (e.g. cross-protocol, many novel
/// classes) doesn't issue dozens of parallel `getClass` RPCs at once and
/// trip provider rate limits or jitter.
async fn prewarm_trace_abis(trace: &TransactionTrace, abi_reg: &Arc<AbiRegistry>) {
    /// Cap on parallel ABI fetches. ~8 covers typical traces (which dedupe
    /// to a handful of unique classes anyway, and most are cache hits) while
    /// staying well below provider per-second limits.
    const PREWARM_CONCURRENCY: usize = 8;

    let mut classes: HashSet<Felt> = HashSet::new();
    for root in root_invocations(trace) {
        collect_class_hashes(root, &mut classes);
    }

    futures::stream::iter(classes.into_iter().map(|ch| {
        let abi_reg = Arc::clone(abi_reg);
        async move {
            let _ = abi_reg.get_abi_for_class(&ch).await;
        }
    }))
    .buffer_unordered(PREWARM_CONCURRENCY)
    .for_each(|_| async {})
    .await;
}

/// Decode one invocation node by looking up its class's ABI and decoding events.
async fn decode_invocation(
    inv: &FunctionInvocation,
    tx_hash: Felt,
    block: u64,
    abi_reg: &Arc<AbiRegistry>,
) -> DecodedTraceCall {
    // Resolve ABI by class hash (already prewarmed).
    let abi = abi_reg.get_abi_for_class(&inv.class_hash).await;

    // Function metadata. Selector → name lookup falls back to the persistent
    // selector table when the ABI's own name didn't make it (legacy classes).
    let mut function_name = abi_reg.get_selector_name(&inv.entry_point_selector);
    let function_def = abi
        .as_deref()
        .and_then(|a| a.get_function(&inv.entry_point_selector));
    if let Some(f) = function_def
        && function_name.is_none()
    {
        function_name = Some(f.name.clone());
    }
    let function_def = function_def.cloned();

    // Decode events emitted directly by this invocation. The trace gives us
    // OrderedEvent (no from_address) — synthesize an SnEvent for `decode_event`.
    let mut events = Vec::with_capacity(inv.events.len());
    for (idx, e) in inv.events.iter().enumerate() {
        let synth = synth_event(inv.contract_address, e, tx_hash, block, idx as u64);
        events.push(decode_event(&synth, abi.as_deref()));
    }

    // Recurse for inner calls. Box::pin to break the recursion in async.
    let mut inner = Vec::with_capacity(inv.calls.len());
    for child in &inv.calls {
        inner.push(Box::pin(decode_invocation(child, tx_hash, block, abi_reg)).await);
    }

    DecodedTraceCall {
        contract_address: inv.contract_address,
        class_hash: inv.class_hash,
        caller_address: inv.caller_address,
        entry_point_selector: inv.entry_point_selector,
        entry_point_type: inv.entry_point_type,
        call_type: inv.call_type,
        calldata: inv.calldata.clone(),
        result: inv.result.clone(),
        is_reverted: false,
        function_name,
        function_def,
        contract_abi: abi,
        events,
        messages: inv.messages.clone(),
        inner,
    }
}

fn synth_event(
    from_address: Felt,
    e: &OrderedEvent,
    tx_hash: Felt,
    block_number: u64,
    event_index: u64,
) -> SnEvent {
    SnEvent {
        from_address,
        keys: e.keys.clone(),
        data: e.data.clone(),
        transaction_hash: tx_hash,
        block_number,
        event_index,
    }
}

/// Decode the full transaction trace tree.
///
/// Pre-warms ABIs for every unique class hash in the tree in one parallel
/// round-trip, then walks the tree to build `DecodedTraceCall` nodes.
pub async fn decode_trace(
    trace: &TransactionTrace,
    tx_hash: Felt,
    block: u64,
    abi_reg: &Arc<AbiRegistry>,
) -> DecodedTrace {
    prewarm_trace_abis(trace, abi_reg).await;

    let mut out = DecodedTrace::default();
    match trace {
        TransactionTrace::Invoke(t) => {
            if let Some(v) = &t.validate_invocation {
                out.validate = Some(decode_invocation(v, tx_hash, block, abi_reg).await);
            }
            match t.execute_invocation {
                ExecuteInvocation::Success(ref e) => {
                    out.execute = Some(decode_invocation(e, tx_hash, block, abi_reg).await);
                }
                ExecuteInvocation::Reverted(ref r) => {
                    out.revert_reason = Some(r.revert_reason.clone());
                }
            }
            if let Some(f) = &t.fee_transfer_invocation {
                out.fee_transfer = Some(decode_invocation(f, tx_hash, block, abi_reg).await);
            }
        }
        TransactionTrace::DeployAccount(t) => {
            if let Some(v) = &t.validate_invocation {
                out.validate = Some(decode_invocation(v, tx_hash, block, abi_reg).await);
            }
            out.constructor =
                Some(decode_invocation(&t.constructor_invocation, tx_hash, block, abi_reg).await);
            if let Some(f) = &t.fee_transfer_invocation {
                out.fee_transfer = Some(decode_invocation(f, tx_hash, block, abi_reg).await);
            }
        }
        TransactionTrace::Declare(t) => {
            if let Some(v) = &t.validate_invocation {
                out.validate = Some(decode_invocation(v, tx_hash, block, abi_reg).await);
            }
            if let Some(f) = &t.fee_transfer_invocation {
                out.fee_transfer = Some(decode_invocation(f, tx_hash, block, abi_reg).await);
            }
        }
        TransactionTrace::L1Handler(t) => match t.function_invocation {
            ExecuteInvocation::Success(ref e) => {
                out.l1_handler = Some(decode_invocation(e, tx_hash, block, abi_reg).await);
            }
            ExecuteInvocation::Reverted(ref r) => {
                out.revert_reason = Some(r.revert_reason.clone());
            }
        },
    }
    // Cache the total node count once so the UI doesn't walk the tree per frame.
    let mut n = 0usize;
    out.for_each_call(|_| n += 1);
    out.total_nodes = n;
    out
}
