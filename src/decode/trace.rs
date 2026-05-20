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
use crate::utils::felt_to_u128;

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

    /// Extract every ERC20 `Transfer` event from the trace, in execution order,
    /// grouped by which top-level invocation produced them. Multicall execute
    /// roots split their transfers per inner call so the UI can render
    /// `Call 1`, `Call 2`, etc. The fee transfer lives in its own bucket.
    pub fn collect_transfers(&self) -> TransferGroups {
        let mut groups = TransferGroups::default();
        if let Some(v) = &self.validate {
            collect_transfers_subtree(v, &mut groups.validate);
        }
        if let Some(c) = &self.constructor {
            collect_transfers_subtree(c, &mut groups.constructor);
        }
        if let Some(e) = &self.execute {
            // Events emitted directly on the execute root (e.g. account-level
            // events) go into `execute_top`; per-call subtrees become their own
            // groups in `execute_calls`.
            collect_transfers_local(e, &mut groups.execute_top);
            for (idx, child) in e.inner.iter().enumerate() {
                let mut transfers = Vec::new();
                collect_transfers_subtree(child, &mut transfers);
                groups.execute_calls.push(MulticallGroup {
                    index: idx + 1,
                    contract: child.contract_address,
                    function_name: child.function_name.clone(),
                    transfers,
                });
            }
        }
        if let Some(h) = &self.l1_handler {
            collect_transfers_subtree(h, &mut groups.l1_handler);
        }
        if let Some(f) = &self.fee_transfer {
            collect_transfers_subtree(f, &mut groups.fee);
        }

        groups.total = groups.validate.len()
            + groups.constructor.len()
            + groups.execute_top.len()
            + groups
                .execute_calls
                .iter()
                .map(|g| g.transfers.len())
                .sum::<usize>()
            + groups.l1_handler.len()
            + groups.fee.len();
        groups
    }
}

/// Transfer-row breakdown of a decoded trace. Empty groups stay empty so the
/// renderer can decide which sections to show.
#[derive(Debug, Clone, Default)]
pub struct TransferGroups {
    pub validate: Vec<TransferRow>,
    pub constructor: Vec<TransferRow>,
    /// Transfers emitted directly on the execute root (not in any inner call).
    pub execute_top: Vec<TransferRow>,
    /// One entry per inner call of the execute root; mirrors the multicall layout.
    pub execute_calls: Vec<MulticallGroup>,
    pub l1_handler: Vec<TransferRow>,
    pub fee: Vec<TransferRow>,
    pub total: usize,
}

/// One inner call of the multicall and the transfers emitted under it.
#[derive(Debug, Clone)]
pub struct MulticallGroup {
    /// 1-based index in the user's __execute__ array.
    pub index: usize,
    pub contract: Felt,
    pub function_name: Option<String>,
    pub transfers: Vec<TransferRow>,
}

/// One decoded ERC20 transfer.
#[derive(Debug, Clone)]
pub struct TransferRow {
    /// Token contract that emitted the event.
    pub token: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Low 128 bits of the u256 amount.
    pub value_low: Felt,
    /// High 128 bits of the u256 amount.
    pub value_high: Felt,
}

/// Net delta of one (address, token) pair across every transfer in a tx.
#[derive(Debug, Clone)]
pub struct TokenDelta {
    pub token: Felt,
    /// Signed net amount (received − sent), low 128 bits. `None` when any
    /// contributing transfer carried `value_high != 0` or a running total
    /// exceeded the u128/i128 range — caller should fall back to raw display.
    pub net: Option<i128>,
    /// True when `net` could not be computed cleanly (u256 overflow or sum
    /// saturation). Mutually exclusive with `net == Some(_)` in practice.
    pub overflow: bool,
    pub received_low: u128,
    pub received_high: u128,
    pub sent_low: u128,
    pub sent_high: u128,
}

/// Per-address net balance changes across every token it touched.
#[derive(Debug, Clone)]
pub struct AddressDelta {
    pub address: Felt,
    /// First-appearance order. Caller sorts for display.
    pub tokens: Vec<TokenDelta>,
}

/// Running totals for one (address, token) pair; collapsed into `TokenDelta` at finalize.
#[derive(Debug, Default)]
struct DeltaAccum {
    received_low: u128,
    received_high: u128,
    sent_low: u128,
    sent_high: u128,
    /// Set only when a running sum exceeds 2^256 — astronomically rare. Other
    /// overflow conditions (u256 net not representable as i128) are detected
    /// at `finalize()` time so the raw `received_*` / `sent_*` totals stay
    /// faithful to the actual sum even in the fallback display path.
    overflow: bool,
}

/// In-place u256 add: `(dst_low, dst_high) += (add_low, add_high)` with
/// proper carry from the low half into the high half. Sets `overflow_flag`
/// only when the high half itself overflows (sum > 2^256).
fn add_u256_inplace(
    add_low: u128,
    add_high: u128,
    dst_low: &mut u128,
    dst_high: &mut u128,
    overflow_flag: &mut bool,
) {
    let (new_low, carry_low) = dst_low.overflowing_add(add_low);
    *dst_low = new_low;
    let (mid_high, c1) = dst_high.overflowing_add(add_high);
    let (new_high, c2) = mid_high.overflowing_add(u128::from(carry_low));
    *dst_high = new_high;
    if c1 || c2 {
        *overflow_flag = true;
    }
}

impl DeltaAccum {
    fn add_received(&mut self, low: u128, high: u128) {
        add_u256_inplace(
            low,
            high,
            &mut self.received_low,
            &mut self.received_high,
            &mut self.overflow,
        );
    }
    fn add_sent(&mut self, low: u128, high: u128) {
        add_u256_inplace(
            low,
            high,
            &mut self.sent_low,
            &mut self.sent_high,
            &mut self.overflow,
        );
    }
    fn finalize(&self) -> (Option<i128>, bool) {
        // Either side carrying a non-zero high half means the u256 difference
        // can't be expressed cleanly as a low-128 signed delta — fall back to
        // raw display. (The u256 totals are still accurate thanks to carry.)
        if self.overflow || self.received_high != 0 || self.sent_high != 0 {
            return (None, true);
        }
        let r = i128::try_from(self.received_low).ok();
        let s = i128::try_from(self.sent_low).ok();
        match (r, s) {
            (Some(r), Some(s)) => match r.checked_sub(s) {
                Some(net) => (Some(net), false),
                None => (None, true),
            },
            _ => (None, true),
        }
    }
}

impl TransferGroups {
    /// Iterate every TransferRow in display/execution order:
    /// validate → constructor → execute_top → execute_calls[*] → l1_handler → fee.
    fn all_transfers(&self) -> impl Iterator<Item = &TransferRow> {
        self.validate
            .iter()
            .chain(self.constructor.iter())
            .chain(self.execute_top.iter())
            .chain(self.execute_calls.iter().flat_map(|g| g.transfers.iter()))
            .chain(self.l1_handler.iter())
            .chain(self.fee.iter())
    }

    /// Compute per-address per-token net balance changes across every transfer
    /// in the tx, fees included. Self-transfers (`from == to`) and zero net
    /// deltas are dropped. Addresses and their tokens are returned in
    /// first-appearance order; the caller sorts for display.
    pub fn balance_changes(&self) -> Vec<AddressDelta> {
        use std::collections::HashMap;

        let mut addr_order: Vec<Felt> = Vec::new();
        // address -> (token first-appearance order, accumulators per token)
        let mut per_addr: HashMap<Felt, (Vec<Felt>, HashMap<Felt, DeltaAccum>)> = HashMap::new();

        for row in self.all_transfers() {
            if row.from == row.to {
                continue;
            }
            let low = felt_to_u128(&row.value_low);
            let high = felt_to_u128(&row.value_high);

            // from-side: sent grows
            {
                let entry = per_addr.entry(row.from).or_insert_with(|| {
                    addr_order.push(row.from);
                    (Vec::new(), HashMap::new())
                });
                if !entry.1.contains_key(&row.token) {
                    entry.0.push(row.token);
                }
                entry.1.entry(row.token).or_default().add_sent(low, high);
            }

            // to-side: received grows
            {
                let entry = per_addr.entry(row.to).or_insert_with(|| {
                    addr_order.push(row.to);
                    (Vec::new(), HashMap::new())
                });
                if !entry.1.contains_key(&row.token) {
                    entry.0.push(row.token);
                }
                entry
                    .1
                    .entry(row.token)
                    .or_default()
                    .add_received(low, high);
            }
        }

        let mut out = Vec::with_capacity(addr_order.len());
        for addr in addr_order {
            let (token_order, mut accum_map) =
                per_addr.remove(&addr).expect("address tracked in order");
            let mut tokens = Vec::with_capacity(token_order.len());
            for tk in token_order {
                let accum = accum_map.remove(&tk).expect("token tracked in order");
                let (net, overflow) = accum.finalize();
                // Drop any (address, token) pair whose received and sent u256
                // totals are exactly equal — that's a true zero net even when
                // the sums saturated past i128/u128 and `net` had to fall
                // back to `None`. Subsumes the common `net == Some(0)` case.
                if accum.received_low == accum.sent_low && accum.received_high == accum.sent_high {
                    continue;
                }
                tokens.push(TokenDelta {
                    token: tk,
                    net,
                    overflow,
                    received_low: accum.received_low,
                    received_high: accum.received_high,
                    sent_low: accum.sent_low,
                    sent_high: accum.sent_high,
                });
            }
            if tokens.is_empty() {
                continue;
            }
            out.push(AddressDelta {
                address: addr,
                tokens,
            });
        }
        out
    }
}

/// Pre-order walk of one invocation's subtree, appending every Transfer event
/// to `out`. Pre-order matches actual emission order (parent's events fire
/// before children execute).
fn collect_transfers_subtree(call: &DecodedTraceCall, out: &mut Vec<TransferRow>) {
    collect_transfers_local(call, out);
    for child in &call.inner {
        collect_transfers_subtree(child, out);
    }
}

/// Append the Transfer events emitted directly by `call` (no recursion).
fn collect_transfers_local(call: &DecodedTraceCall, out: &mut Vec<TransferRow>) {
    for ev in &call.events {
        if let Some(row) = transfer_row_from_event(ev) {
            out.push(row);
        }
    }
}

/// Repackage an already-decoded ERC20 `Transfer` event as a `TransferRow`.
///
/// The trace decoder ran each event through the historical class ABI, so
/// `decoded_keys` and `decoded_data` already carry proper param names —
/// regardless of whether the contract marked from/to as `#[key]` (modern
/// OpenZeppelin) or stuffed everything into `data` (Cairo-0 and a few
/// Cairo-1 contracts including STRK on mainnet). Looking up by name avoids
/// duplicating that ABI knowledge here.
///
/// NFT-style `Transfer(from, to, token_id)` intentionally falls through:
/// `token_id` carries no amount, so it would have nothing useful to show.
fn transfer_row_from_event(ev: &DecodedEvent) -> Option<TransferRow> {
    if ev.event_name.as_deref() != Some("Transfer") {
        return None;
    }
    let by_name = |n: &str| {
        ev.decoded_keys
            .iter()
            .chain(ev.decoded_data.iter())
            .find(|p| p.name.as_deref() == Some(n))
    };
    let from = by_name("from")?;
    let to = by_name("to")?;
    let value = by_name("value").or_else(|| by_name("amount"))?;
    Some(TransferRow {
        token: ev.contract_address,
        from: from.value,
        to: to.value,
        value_low: value.value,
        value_high: value.value_high.unwrap_or(Felt::ZERO),
    })
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

    // Recurse for inner calls in parallel. Box::pin breaks the recursion in
    // async, and `join_all` lets sibling subtrees decode concurrently — useful
    // for wide call trees (e.g. router multicalls with many independent legs)
    // where each branch has its own ABI fetches.
    let inner: Vec<DecodedTraceCall> = futures::future::join_all(
        inv.calls
            .iter()
            .map(|child| Box::pin(decode_invocation(child, tx_hash, block, abi_reg))),
    )
    .await;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::SnEvent;
    use crate::decode::events::{DecodedEvent, DecodedParam};

    /// Public ETH token contract on Starknet mainnet — safe for fixtures.
    const ETH: &str = "0x49d36570d4e46f48e99674bd3fcc8463d4dd6dad9d8d4e9b3eb38e3f4eba7e0";
    /// Public AVNU exchange router — safe for fixtures.
    const AVNU: &str = "0x04270219d365d6b017231b52e92b3fb5d7c8378b05e9abc97724537a80e93b0f";
    /// Public Sequencer fee receiver placeholder for tests.
    const SEQ: &str = "0x01176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8";
    /// Public sender placeholder (well-known EVM-style zero-suffix). Tests
    /// must not use any address that could be linked to real activity.
    const ALICE: &str = "0x00000000000000000000000000000000000000000000000000000000deadbeef";

    fn felt(hex: &str) -> Felt {
        Felt::from_hex(hex).unwrap()
    }

    fn transfer_event(token: Felt, from: Felt, to: Felt, value: u128) -> DecodedEvent {
        DecodedEvent {
            contract_address: token,
            event_name: Some("Transfer".to_string()),
            decoded_keys: vec![
                DecodedParam {
                    name: Some("from".into()),
                    type_name: Some("ContractAddress".into()),
                    value: from,
                    value_high: None,
                },
                DecodedParam {
                    name: Some("to".into()),
                    type_name: Some("ContractAddress".into()),
                    value: to,
                    value_high: None,
                },
            ],
            decoded_data: vec![DecodedParam {
                name: Some("value".into()),
                type_name: Some("u256".into()),
                value: Felt::from(value),
                value_high: Some(Felt::ZERO),
            }],
            raw: SnEvent {
                from_address: token,
                keys: Vec::new(),
                data: Vec::new(),
                transaction_hash: Felt::ZERO,
                block_number: 0,
                event_index: 0,
            },
        }
    }

    /// Transfer event with all params packed into `data` (no `#[key]` on
    /// from/to). This is how STRK on Starknet mainnet — and Cairo-0 ERC20s —
    /// emit Transfer.
    fn transfer_event_data_only(
        token: Felt,
        from: Felt,
        to: Felt,
        value: u128,
        type_tag: &str,
    ) -> DecodedEvent {
        DecodedEvent {
            contract_address: token,
            event_name: Some("Transfer".to_string()),
            decoded_keys: vec![],
            decoded_data: vec![
                DecodedParam {
                    name: Some("from".into()),
                    type_name: Some("ContractAddress".into()),
                    value: from,
                    value_high: None,
                },
                DecodedParam {
                    name: Some("to".into()),
                    type_name: Some("ContractAddress".into()),
                    value: to,
                    value_high: None,
                },
                DecodedParam {
                    name: Some("value".into()),
                    type_name: Some(type_tag.into()),
                    value: Felt::from(value),
                    value_high: Some(Felt::ZERO),
                },
            ],
            raw: SnEvent {
                from_address: token,
                keys: Vec::new(),
                data: Vec::new(),
                transaction_hash: Felt::ZERO,
                block_number: 0,
                event_index: 0,
            },
        }
    }

    fn other_event(name: &str, contract: Felt) -> DecodedEvent {
        DecodedEvent {
            contract_address: contract,
            event_name: Some(name.to_string()),
            decoded_keys: vec![],
            decoded_data: vec![],
            raw: SnEvent {
                from_address: contract,
                keys: Vec::new(),
                data: Vec::new(),
                transaction_hash: Felt::ZERO,
                block_number: 0,
                event_index: 0,
            },
        }
    }

    fn leaf_call(contract: Felt, fn_name: &str, events: Vec<DecodedEvent>) -> DecodedTraceCall {
        DecodedTraceCall {
            contract_address: contract,
            class_hash: Felt::ZERO,
            caller_address: Felt::ZERO,
            entry_point_selector: Felt::ZERO,
            entry_point_type: EntryPointType::External,
            call_type: CallType::Call,
            calldata: Vec::new(),
            result: Vec::new(),
            is_reverted: false,
            function_name: Some(fn_name.into()),
            function_def: None,
            contract_abi: None,
            events,
            messages: Vec::new(),
            inner: Vec::new(),
        }
    }

    #[test]
    fn collect_transfers_groups_by_multicall_call_and_separates_fee() {
        let eth = felt(ETH);
        let avnu = felt(AVNU);
        let alice = felt(ALICE);
        let seq = felt(SEQ);

        // Multicall with two execute children, each emitting one transfer.
        // The second child has a nested inner call whose transfer should be
        // attributed to call 2.
        let leg_a_inner = leaf_call(eth, "_inner", vec![transfer_event(eth, alice, avnu, 1_000)]);
        let leg_a = DecodedTraceCall {
            inner: vec![leg_a_inner],
            ..leaf_call(avnu, "swap_exact_in", Vec::new())
        };
        let leg_b = leaf_call(eth, "transfer", vec![transfer_event(eth, avnu, alice, 950)]);

        // A non-Transfer event on the execute root should be ignored.
        let execute = DecodedTraceCall {
            inner: vec![leg_a, leg_b],
            ..leaf_call(alice, "__execute__", vec![other_event("Approval", eth)])
        };

        let fee = leaf_call(eth, "transfer", vec![transfer_event(eth, alice, seq, 50)]);

        let trace = DecodedTrace {
            execute: Some(execute),
            fee_transfer: Some(fee),
            ..DecodedTrace::default()
        };

        let groups = trace.collect_transfers();
        assert_eq!(groups.execute_calls.len(), 2);
        assert_eq!(groups.execute_calls[0].index, 1);
        assert_eq!(groups.execute_calls[0].transfers.len(), 1);
        assert_eq!(groups.execute_calls[0].transfers[0].from, alice);
        assert_eq!(groups.execute_calls[0].transfers[0].to, avnu);
        assert_eq!(groups.execute_calls[1].index, 2);
        assert_eq!(groups.execute_calls[1].transfers.len(), 1);
        assert_eq!(groups.execute_calls[1].transfers[0].to, alice);
        assert!(groups.execute_top.is_empty());
        assert_eq!(groups.fee.len(), 1);
        assert_eq!(groups.fee[0].to, seq);
        assert_eq!(groups.total, 3);
    }

    #[test]
    fn collect_transfers_skips_nft_style_transfers_without_u256_data() {
        let eth = felt(ETH);
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        // NFT-style Transfer: keys carry from/to, but data is a felt token_id —
        // not a u256 amount. We can't render a meaningful amount, so skip it.
        let nft_event = DecodedEvent {
            contract_address: eth,
            event_name: Some("Transfer".into()),
            decoded_keys: vec![
                DecodedParam {
                    name: Some("from".into()),
                    type_name: Some("ContractAddress".into()),
                    value: alice,
                    value_high: None,
                },
                DecodedParam {
                    name: Some("to".into()),
                    type_name: Some("ContractAddress".into()),
                    value: avnu,
                    value_high: None,
                },
            ],
            decoded_data: vec![DecodedParam {
                name: Some("token_id".into()),
                type_name: Some("felt252".into()),
                value: Felt::from(7u32),
                value_high: None,
            }],
            raw: SnEvent {
                from_address: eth,
                keys: Vec::new(),
                data: Vec::new(),
                transaction_hash: Felt::ZERO,
                block_number: 0,
                event_index: 0,
            },
        };

        let execute = leaf_call(alice, "__execute__", vec![nft_event]);
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let groups = trace.collect_transfers();
        assert_eq!(groups.total, 0);
    }

    /// STRK and other tokens that emit Transfer with from/to in `data` (no
    /// `#[key]`) must still be detected. Regression test for tx
    /// 0x7d5543b0eb99a15ea173d3fe7ca389c60ec0c6b9a66054c0d3fce0fb04ac08
    /// where 9 transfer events on STRK previously rendered as `Transfers (0)`.
    #[test]
    fn collect_transfers_handles_data_only_layout_strk_style() {
        let strk = felt(SEQ); // any token contract; the layout is what's tested
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        let ev = transfer_event_data_only(strk, alice, avnu, 1_000, "core::integer::u256");
        let execute = leaf_call(alice, "__execute__", vec![ev]);
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let groups = trace.collect_transfers();
        // Event is on the execute root itself (no inner call), so it lands in execute_top.
        assert_eq!(groups.execute_top.len(), 1);
        assert_eq!(groups.execute_top[0].from, alice);
        assert_eq!(groups.execute_top[0].to, avnu);
        assert_eq!(groups.total, 1);
    }

    /// Cairo-0 contracts use `Uint256` (capitalized) as the type tag — must
    /// also be picked up.
    #[test]
    fn collect_transfers_handles_legacy_uint256_type_tag() {
        let token = felt(SEQ);
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        let ev = transfer_event_data_only(token, alice, avnu, 42, "Uint256");
        let execute = leaf_call(alice, "__execute__", vec![ev]);
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let groups = trace.collect_transfers();
        assert_eq!(groups.execute_top.len(), 1);
        assert_eq!(groups.total, 1);
    }

    /// A two-leg swap: alice sends token1 to avnu and receives token2 back.
    /// Expect both addresses with two-token deltas, opposite signs.
    #[test]
    fn balance_changes_pairs_swap_legs() {
        let token1 = felt(ETH);
        let token2 = felt(SEQ);
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        let execute = leaf_call(
            alice,
            "__execute__",
            vec![
                transfer_event(token1, alice, avnu, 1_000),
                transfer_event(token2, avnu, alice, 950),
            ],
        );
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let deltas = trace.collect_transfers().balance_changes();

        assert_eq!(deltas.len(), 2, "expected two participating addresses");
        let by_addr: std::collections::HashMap<Felt, &AddressDelta> =
            deltas.iter().map(|d| (d.address, d)).collect();

        let a = by_addr[&alice];
        assert_eq!(a.tokens.len(), 2);
        let a_t1 = a.tokens.iter().find(|t| t.token == token1).unwrap();
        let a_t2 = a.tokens.iter().find(|t| t.token == token2).unwrap();
        assert_eq!(a_t1.net, Some(-1_000));
        assert_eq!(a_t2.net, Some(950));

        let b = by_addr[&avnu];
        let b_t1 = b.tokens.iter().find(|t| t.token == token1).unwrap();
        let b_t2 = b.tokens.iter().find(|t| t.token == token2).unwrap();
        assert_eq!(b_t1.net, Some(1_000));
        assert_eq!(b_t2.net, Some(-950));
    }

    /// A round-trip transfer (A → B then B → A, same token, same amount)
    /// nets to zero on both sides and should drop from the summary entirely.
    #[test]
    fn balance_changes_drops_round_trip_to_zero() {
        let token = felt(ETH);
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        let execute = leaf_call(
            alice,
            "__execute__",
            vec![
                transfer_event(token, alice, avnu, 500),
                transfer_event(token, avnu, alice, 500),
            ],
        );
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let deltas = trace.collect_transfers().balance_changes();
        assert!(deltas.is_empty(), "expected no addresses; got {:?}", deltas);
    }

    /// A mint from the zero address shows up as +X on the recipient and −X
    /// on 0x0 — the renderer relabels 0x0 as mint/burn.
    #[test]
    fn balance_changes_records_mint_from_zero_address() {
        let token = felt(ETH);
        let alice = felt(ALICE);
        let zero = Felt::ZERO;

        let execute = leaf_call(
            alice,
            "__execute__",
            vec![transfer_event(token, zero, alice, 7_777)],
        );
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let deltas = trace.collect_transfers().balance_changes();
        let by_addr: std::collections::HashMap<Felt, &AddressDelta> =
            deltas.iter().map(|d| (d.address, d)).collect();

        assert_eq!(by_addr[&alice].tokens[0].net, Some(7_777));
        assert_eq!(by_addr[&zero].tokens[0].net, Some(-7_777));
    }

    /// Fee transfers must be included in the per-address net deltas.
    #[test]
    fn balance_changes_includes_fee_phase() {
        let token = felt(ETH);
        let alice = felt(ALICE);
        let seq = felt(SEQ);

        let fee = leaf_call(
            token,
            "transfer",
            vec![transfer_event(token, alice, seq, 42)],
        );
        let trace = DecodedTrace {
            fee_transfer: Some(fee),
            ..DecodedTrace::default()
        };
        let deltas = trace.collect_transfers().balance_changes();
        let by_addr: std::collections::HashMap<Felt, &AddressDelta> =
            deltas.iter().map(|d| (d.address, d)).collect();
        assert_eq!(by_addr[&alice].tokens[0].net, Some(-42));
        assert_eq!(by_addr[&seq].tokens[0].net, Some(42));
    }

    /// Sum of multiple in-range transfers can still exceed u128 on either
    /// side. The carry must propagate cleanly into the high half so the raw
    /// totals shown in the overflow-fallback row remain accurate.
    #[test]
    fn balance_changes_propagates_low_half_carry() {
        let token = felt(ETH);
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        let execute = leaf_call(
            alice,
            "__execute__",
            vec![
                transfer_event(token, avnu, alice, u128::MAX),
                transfer_event(token, avnu, alice, 1),
            ],
        );
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let deltas = trace.collect_transfers().balance_changes();
        let by_addr: std::collections::HashMap<Felt, &AddressDelta> =
            deltas.iter().map(|d| (d.address, d)).collect();

        // Alice received u128::MAX + 1 = 2^128, so low wraps to 0 and high = 1.
        let alice_td = &by_addr[&alice].tokens[0];
        assert!(alice_td.overflow);
        assert!(alice_td.net.is_none());
        assert_eq!(alice_td.received_low, 0, "low half must wrap to 0");
        assert_eq!(alice_td.received_high, 1, "carry must propagate into high");
        assert_eq!(alice_td.sent_low, 0);
        assert_eq!(alice_td.sent_high, 0);

        // Mirrored on the sender side.
        let avnu_td = &by_addr[&avnu].tokens[0];
        assert!(avnu_td.overflow);
        assert!(avnu_td.net.is_none());
        assert_eq!(avnu_td.sent_low, 0);
        assert_eq!(avnu_td.sent_high, 1);
        assert_eq!(avnu_td.received_low, 0);
        assert_eq!(avnu_td.received_high, 0);
    }

    /// A transfer whose value exceeds u128 (value_high != 0) must mark the
    /// affected delta as `overflow` so the renderer falls back to raw display.
    #[test]
    fn balance_changes_flags_u256_overflow() {
        let token = felt(ETH);
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        // value_high = 1 → amount > 2^128
        let ev = DecodedEvent {
            contract_address: token,
            event_name: Some("Transfer".into()),
            decoded_keys: vec![
                DecodedParam {
                    name: Some("from".into()),
                    type_name: Some("ContractAddress".into()),
                    value: alice,
                    value_high: None,
                },
                DecodedParam {
                    name: Some("to".into()),
                    type_name: Some("ContractAddress".into()),
                    value: avnu,
                    value_high: None,
                },
            ],
            decoded_data: vec![DecodedParam {
                name: Some("value".into()),
                type_name: Some("u256".into()),
                value: Felt::from(1u32),
                value_high: Some(Felt::from(1u32)),
            }],
            raw: SnEvent {
                from_address: token,
                keys: Vec::new(),
                data: Vec::new(),
                transaction_hash: Felt::ZERO,
                block_number: 0,
                event_index: 0,
            },
        };
        let execute = leaf_call(alice, "__execute__", vec![ev]);
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let deltas = trace.collect_transfers().balance_changes();
        for ad in &deltas {
            assert!(ad.tokens[0].overflow, "expected overflow flag set");
            assert!(ad.tokens[0].net.is_none());
        }
    }

    /// A round-trip transfer where each leg's amount exceeds u128 (so the
    /// per-address accumulators end up `overflow == true`) but received and
    /// sent u256 totals match exactly. Net is still zero, so the row must
    /// drop instead of rendering as a misleading overflow-fallback line.
    #[test]
    fn balance_changes_drops_overflow_round_trip() {
        let token = felt(ETH);
        let alice = felt(ALICE);
        let avnu = felt(AVNU);

        // value_high = 1 on both legs → each amount is > 2^128, but the
        // received/sent totals on each side end up identical.
        let make_ev = |from: Felt, to: Felt| DecodedEvent {
            contract_address: token,
            event_name: Some("Transfer".into()),
            decoded_keys: vec![
                DecodedParam {
                    name: Some("from".into()),
                    type_name: Some("ContractAddress".into()),
                    value: from,
                    value_high: None,
                },
                DecodedParam {
                    name: Some("to".into()),
                    type_name: Some("ContractAddress".into()),
                    value: to,
                    value_high: None,
                },
            ],
            decoded_data: vec![DecodedParam {
                name: Some("value".into()),
                type_name: Some("u256".into()),
                value: Felt::from(1u32),
                value_high: Some(Felt::from(1u32)),
            }],
            raw: SnEvent {
                from_address: token,
                keys: Vec::new(),
                data: Vec::new(),
                transaction_hash: Felt::ZERO,
                block_number: 0,
                event_index: 0,
            },
        };

        let execute = leaf_call(
            alice,
            "__execute__",
            vec![make_ev(alice, avnu), make_ev(avnu, alice)],
        );
        let trace = DecodedTrace {
            execute: Some(execute),
            ..DecodedTrace::default()
        };
        let deltas = trace.collect_transfers().balance_changes();
        assert!(
            deltas.is_empty(),
            "overflow round-trip should drop entirely; got {:?}",
            deltas
        );
    }
}
