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
}
