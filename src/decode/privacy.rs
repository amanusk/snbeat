//! Starknet Privacy Pool tx summarization.
//!
//! Aggregates the publicly-observable parts of an `apply_actions` transaction:
//! action-type counts, public boundary flows (Deposit/Withdrawal/OpenNote*),
//! viewing-key registrations, the InvokeExternal target, and the pool fee.
//!
//! What we deliberately do NOT do here:
//!   * Decode encrypted blobs (`enc_user_addr`, `EncPrivateKey`,
//!     `packed_value`). They're auditor-only — surfacing the raw bytes would
//!     just be noise.
//!   * Correlate intra-tx Deposit ↔ EncNoteCreated ↔ NoteUsed ↔ Withdrawal.
//!     That is exactly what the protocol prevents; doing so in a public
//!     explorer would be misleading.
//!
//! Proof age is currently not extracted: `apply_actions` deserializes
//! `ProofFacts` from `tx_info` (paymaster_data / account_deployment_data /
//! signature tail), and the exact slot needs to be confirmed against
//! `packages/privacy/src/privacy.cairo`. v1 ships without proof age.
//!
//! Reference contract / class hashes (mainnet):
//!   * Pool address:        0x040337b1af3c663e86e333bab5a4b28da8d4652a15a69beee2b677776ffe812a
//!   * Ekubo helper class:  0x061047c201f235d66cab8a0c4768ea0ca9f900c64b478a90531fb2fb30e061dc
//!   * Vesu helper class:   0x02fec72887f6431e4a66090bec49ecf8bde30cf39a7045e4ed6ce57447704b24

use std::sync::LazyLock;

use starknet::core::types::Felt;
use starknet::core::utils::get_selector_from_name;

use super::events::DecodedEvent;
use super::functions::RawCall;
use super::outside_execution::OutsideExecutionInfo;
use crate::data::types::SnTransaction;
use crate::utils::felt_to_u128;

/// Mainnet Privacy Pool address.
pub static POOL_ADDRESS: LazyLock<Felt> = LazyLock::new(|| {
    Felt::from_hex("0x040337b1af3c663e86e333bab5a4b28da8d4652a15a69beee2b677776ffe812a").unwrap()
});

/// Pool event selectors. Computed lazily so we never pay the cost on
/// non-privacy txs.
struct EventSelectors {
    note_used: Felt,
    enc_note_created: Felt,
    open_note_created: Felt,
    open_note_deposited: Felt,
    deposit: Felt,
    withdrawal: Felt,
    viewing_key_set: Felt,
}

static EVENT_SELECTORS: LazyLock<EventSelectors> = LazyLock::new(|| EventSelectors {
    // Selectors observed on mainnet tx 0x90304ef5b180c520ce866161c0a14618a72af5aa841fe38a243220aa05bfa2.
    note_used: Felt::from_hex("0x0247fc60d782e0094e7f98c47f277d92a3345d07a436f1f56b27a9b62be2322e")
        .unwrap(),
    enc_note_created: Felt::from_hex(
        "0x023c20207be8b1ef4430c25eef8ce779c9745ebe04139555ae81bd4f8fdd6ec5",
    )
    .unwrap(),
    open_note_created: Felt::from_hex(
        "0x022330482fd296a27cf9096807b4a3622cd619d31cce42c1e55655914e8459ee",
    )
    .unwrap(),
    open_note_deposited: Felt::from_hex(
        "0x025b6da03c4858d11cb0708d5cb6be79b190fb32eb7a7ce83804e07cbbb9bead",
    )
    .unwrap(),
    withdrawal: Felt::from_hex(
        "0x002eed7e29b3502a726faf503ac4316b7101f3da813654e8df02c13449e03da8",
    )
    .unwrap(),
    // Computed from `Deposit` / `ViewingKeySet` event names (Cairo Poseidon-based).
    deposit: get_selector_from_name("Deposit").unwrap(),
    viewing_key_set: get_selector_from_name("ViewingKeySet").unwrap(),
});

/// Categorizes a Privacy Pool event by selector.
fn classify_event(event: &DecodedEvent) -> Option<PrivacyEventKind> {
    if event.contract_address != *POOL_ADDRESS {
        return None;
    }
    let selector = event.raw.keys.first()?;
    let s = &*EVENT_SELECTORS;
    if *selector == s.note_used {
        Some(PrivacyEventKind::NoteUsed)
    } else if *selector == s.enc_note_created {
        Some(PrivacyEventKind::EncNoteCreated)
    } else if *selector == s.open_note_created {
        Some(PrivacyEventKind::OpenNoteCreated)
    } else if *selector == s.open_note_deposited {
        Some(PrivacyEventKind::OpenNoteDeposited)
    } else if *selector == s.deposit {
        Some(PrivacyEventKind::Deposit)
    } else if *selector == s.withdrawal {
        Some(PrivacyEventKind::Withdrawal)
    } else if *selector == s.viewing_key_set {
        Some(PrivacyEventKind::ViewingKeySet)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrivacyEventKind {
    NoteUsed,
    EncNoteCreated,
    OpenNoteCreated,
    OpenNoteDeposited,
    Deposit,
    Withdrawal,
    ViewingKeySet,
}

/// One-line action-mix summary.
#[derive(Debug, Clone, Default)]
pub struct ActionMix {
    pub notes_used: usize,
    pub enc_notes_created: usize,
    pub open_notes_created: usize,
    pub open_notes_deposited: usize,
    pub deposits: usize,
    pub withdrawals: usize,
    pub viewing_keys_set: usize,
    pub invoke_external: usize,
}

impl ActionMix {
    pub fn total(&self) -> usize {
        self.notes_used
            + self.enc_notes_created
            + self.open_notes_created
            + self.open_notes_deposited
            + self.deposits
            + self.withdrawals
            + self.viewing_keys_set
            + self.invoke_external
    }
}

/// A public-side deposit event (`user_addr` + token + amount, all clear).
#[derive(Debug, Clone)]
pub struct PublicDeposit {
    pub user_addr: Felt,
    pub token: Felt,
    /// `u128` amount (low bits if the contract serializes a u128; we don't
    /// upcast).
    pub amount: u128,
}

/// A public-side withdrawal: recipient + token + amount are clear; the
/// withdrawing user is encrypted (auditor-only) and intentionally not
/// surfaced here.
#[derive(Debug, Clone)]
pub struct PublicWithdrawal {
    pub to_addr: Felt,
    pub token: Felt,
    pub amount: u128,
}

/// `OpenNoteCreated` — token + note id are clear; recipient address is
/// encrypted (auditor-only) and intentionally not surfaced.
#[derive(Debug, Clone)]
pub struct OpenNote {
    pub token: Felt,
    pub note_id: Felt,
}

/// `OpenNoteDeposited` — depositor + token + note id + amount, all clear.
#[derive(Debug, Clone)]
pub struct OpenNoteDeposit {
    pub depositor: Felt,
    pub token: Felt,
    pub note_id: Felt,
    pub amount: u128,
}

/// A user joined the pool (registered a viewing public key).
#[derive(Debug, Clone)]
pub struct ViewingKeyRegistration {
    pub user_addr: Felt,
    pub public_key: Felt,
}

/// Verdict for a user-supplied private viewing key against an on-chain
/// `ViewingKeySet` registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewingKeyStatus {
    /// `private_key * G == registered_public_key`. The supplied key is the
    /// genuine viewing key for this user.
    Valid,
    /// `private_key * G != registered_public_key`. Either a typo in the
    /// labels file or a stale key (the user has rotated theirs since).
    Mismatch,
}

/// Check a user-supplied private viewing key against an on-chain
/// `ViewingKeyRegistration` for the same user. Returns `None` when the
/// caller has no labelled key for this user.
pub fn validate_viewing_key(
    registration: &ViewingKeyRegistration,
    supplied: &crate::decode::privacy_crypto::types::SecretFelt,
) -> ViewingKeyStatus {
    let derived = crate::decode::privacy_crypto::keys::public_from_private(supplied);
    if derived == registration.public_key {
        ViewingKeyStatus::Valid
    } else {
        ViewingKeyStatus::Mismatch
    }
}

/// Aggregated summary of a Privacy Pool transaction.
#[derive(Debug, Clone)]
pub struct PrivacySummary {
    pub actions: ActionMix,
    pub deposits: Vec<PublicDeposit>,
    pub withdrawals: Vec<PublicWithdrawal>,
    pub open_notes_created: Vec<OpenNote>,
    pub open_notes_deposited: Vec<OpenNoteDeposit>,
    /// IDs of encrypted notes created in this tx. We don't surface their
    /// `packed_value` (encodes salt+amount) — that's auditor-only.
    pub enc_notes_created: Vec<Felt>,
    /// Nullifiers of notes spent in this tx.
    pub nullifiers: Vec<Felt>,
    pub viewing_keys_set: Vec<ViewingKeyRegistration>,
    /// Best-effort detection of an external helper call. Identified by walking
    /// `decoded_calls` for any non-pool top-level call (the `apply_actions`
    /// pipeline routes `InvokeExternal` through standard syscall, so it shows
    /// up as a sibling call rather than nested) and by checking if any
    /// known-helper class is referenced.
    pub invoke_external: Option<InvokeExternalRef>,
    /// Pool-fee paid in FRI. Detected as the residual STRK transfer from the
    /// pool *not* matched against any `Withdrawal` event recipient — i.e. the
    /// transfer that goes to the configured fee_collector rather than to a
    /// withdrawing user. Returns `None` when the pool emits no STRK transfers.
    pub pool_fee_fri: Option<u128>,
    /// Sponsorship signal. See [`PaymasterMode`] for what each variant means.
    pub paymaster: PaymasterMode,
    /// When the pool call is wrapped in an outside-execution (the AVNU
    /// gasless / SNIP-9 pattern), this is the user's account address (the
    /// signer of the OE intent). The on-chain `tx.sender` in that case is
    /// the relayer, *not* the user. Surface this so the privacy tab can
    /// label "Intender" instead of misleading "Sender".
    pub intender: Option<Felt>,
}

/// How (or whether) the chain-level fee for this tx is sponsored. None of
/// these variants is a strict guarantee — the InvokeTx struct doesn't carry
/// `paymaster_data` today, so we infer from on-chain shape only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymasterMode {
    /// No sponsorship signal detected (sender is not a known relayer, no
    /// outside-execution wraps the pool call, no top-level call to a known
    /// paymaster forwarder). Most likely the user paid directly.
    None,
    /// `tx.sender` is one of the known relayer / paymaster forwarder
    /// addresses we have bundled.
    KnownRelayer,
    /// The privacy pool call is wrapped in an outside-execution intent (the
    /// canonical SNIP-9 / AVNU gasless pattern). The intender (user) is
    /// distinct from `tx.sender` (relayer).
    OutsideExecution,
    /// A top-level multicall entry routes through a known paymaster
    /// forwarder (e.g. AVNU Forwarder) before/around the pool call. This
    /// catches the gasless-token-fee pattern where the user pays the relayer
    /// in STRK rather than ETH.
    PaymasterForwarder,
}

impl PaymasterMode {
    pub fn is_sponsored(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Reference to the contract called by an `InvokeExternal` server action.
#[derive(Debug, Clone)]
pub struct InvokeExternalRef {
    pub target: Felt,
    pub selector: Felt,
    pub function_name: Option<String>,
}

/// Summarize a transaction. Returns `None` for non-privacy txs.
///
/// Detects a privacy tx via *any* of: a top-level call targeting the pool,
/// an outside-execution inner call targeting the pool, or a pool-emitted
/// event in the receipt. The third signal catches OE-wrapped sponsored
/// txs where neither the top-level multicall nor the OE inner-call list is
/// where we'd naturally look (e.g. AVNU's paymaster v2 routes through an
/// intermediate forwarder contract before reaching the pool).
pub fn summarize(
    tx: &SnTransaction,
    decoded_calls: &[RawCall],
    decoded_events: &[DecodedEvent],
    outside_executions: &[OutsideExecutionInfo],
) -> Option<PrivacySummary> {
    let has_pool_call = decoded_calls
        .iter()
        .any(|c| c.contract_address == *POOL_ADDRESS);
    let has_pool_oe_inner = outside_executions.iter().any(|oe| {
        oe.inner_calls
            .iter()
            .any(|c| c.contract_address == *POOL_ADDRESS)
    });
    let has_pool_event = decoded_events
        .iter()
        .any(|e| e.contract_address == *POOL_ADDRESS);
    if !(has_pool_call || has_pool_oe_inner || has_pool_event) {
        return None;
    }

    let mut actions = ActionMix::default();
    let mut deposits = Vec::new();
    let mut withdrawals = Vec::new();
    let mut open_notes_created = Vec::new();
    let mut open_notes_deposited = Vec::new();
    let mut enc_notes_created = Vec::new();
    let mut nullifiers = Vec::new();
    let mut viewing_keys_set = Vec::new();

    for ev in decoded_events {
        let Some(kind) = classify_event(ev) else {
            continue;
        };
        match kind {
            PrivacyEventKind::NoteUsed => {
                actions.notes_used += 1;
                // keys[0]=selector, keys[1]=nullifier
                if let Some(nullifier) = ev.raw.keys.get(1).copied() {
                    nullifiers.push(nullifier);
                }
            }
            PrivacyEventKind::EncNoteCreated => {
                actions.enc_notes_created += 1;
                // keys[0]=selector, keys[1]=note_id
                if let Some(note_id) = ev.raw.keys.get(1).copied() {
                    enc_notes_created.push(note_id);
                }
            }
            PrivacyEventKind::OpenNoteCreated => {
                actions.open_notes_created += 1;
                // keys: selector, token, note_id; data: enc_recipient_addr (auditor-only)
                if let (Some(token), Some(note_id)) =
                    (ev.raw.keys.get(1).copied(), ev.raw.keys.get(2).copied())
                {
                    open_notes_created.push(OpenNote { token, note_id });
                }
            }
            PrivacyEventKind::OpenNoteDeposited => {
                actions.open_notes_deposited += 1;
                // keys: selector, depositor, token, note_id; data: amount (u128)
                if let (Some(depositor), Some(token), Some(note_id)) = (
                    ev.raw.keys.get(1).copied(),
                    ev.raw.keys.get(2).copied(),
                    ev.raw.keys.get(3).copied(),
                ) {
                    let amount = ev.raw.data.first().map(felt_to_u128).unwrap_or(0);
                    open_notes_deposited.push(OpenNoteDeposit {
                        depositor,
                        token,
                        note_id,
                        amount,
                    });
                }
            }
            PrivacyEventKind::Deposit => {
                actions.deposits += 1;
                // keys: selector, user_addr, token; data: amount (u128)
                if let (Some(user_addr), Some(token)) =
                    (ev.raw.keys.get(1).copied(), ev.raw.keys.get(2).copied())
                {
                    let amount = ev.raw.data.first().map(felt_to_u128).unwrap_or(0);
                    deposits.push(PublicDeposit {
                        user_addr,
                        token,
                        amount,
                    });
                }
            }
            PrivacyEventKind::Withdrawal => {
                actions.withdrawals += 1;
                // keys: selector, to_addr, token; data: enc_user_addr blobs + amount (u128)
                // The encrypted user-addr layout (`EncUserAddr` struct) makes the
                // amount land at the tail of `data`; index by length rather
                // than by a fixed offset so a struct-shape change in a future
                // pool revision doesn't silently zero out the amount.
                if let (Some(to_addr), Some(token)) =
                    (ev.raw.keys.get(1).copied(), ev.raw.keys.get(2).copied())
                {
                    let amount = ev.raw.data.last().map(felt_to_u128).unwrap_or(0);
                    withdrawals.push(PublicWithdrawal {
                        to_addr,
                        token,
                        amount,
                    });
                }
            }
            PrivacyEventKind::ViewingKeySet => {
                actions.viewing_keys_set += 1;
                // keys: selector, user_addr, public_key; data: enc_private_key
                if let (Some(user_addr), Some(public_key)) =
                    (ev.raw.keys.get(1).copied(), ev.raw.keys.get(2).copied())
                {
                    viewing_keys_set.push(ViewingKeyRegistration {
                        user_addr,
                        public_key,
                    });
                }
            }
        }
    }

    // Pool fee detection: each STRK transfer from the pool either
    //   (a) matches a `Withdrawal` event recipient/amount → it's the
    //       withdrawal payout (potentially to a paymaster, see
    //       `0x4b9cde1...` for the AVNU gasless privacy pattern), or
    //   (b) doesn't match → it's the pool fee transfer to fee_collector.
    // Sum the (b) bucket. If every pool→X transfer is matched against a
    // withdrawal, fee is `None` rather than `Some(0)` — we don't want to
    // imply a fee column when the chain shape can't tell us.
    let strk_token =
        Felt::from_hex("0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d")
            .unwrap();
    let erc20_transfer_selector =
        Felt::from_hex("0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9")
            .unwrap();
    let mut withdrawal_match: Vec<bool> = vec![false; withdrawals.len()];
    let mut fee_total: u128 = 0;
    let mut saw_pool_strk_transfer = false;
    for ev in decoded_events {
        if ev.contract_address != strk_token
            || ev.raw.keys.first() != Some(&erc20_transfer_selector)
            || ev.raw.keys.get(1) != Some(&*POOL_ADDRESS)
        {
            continue;
        }
        saw_pool_strk_transfer = true;
        let to = ev.raw.keys.get(2).copied().unwrap_or(Felt::ZERO);
        let amt = ev.raw.data.first().map(felt_to_u128).unwrap_or(0);
        // Try to match this transfer against an unmatched withdrawal of the
        // same token going to the same recipient with the same amount.
        let matched = withdrawals.iter().enumerate().position(|(i, w)| {
            !withdrawal_match[i] && w.token == strk_token && w.to_addr == to && w.amount == amt
        });
        if let Some(idx) = matched {
            withdrawal_match[idx] = true;
        } else {
            fee_total = fee_total.saturating_add(amt);
        }
    }
    let pool_fee_fri = if saw_pool_strk_transfer && fee_total > 0 {
        Some(fee_total)
    } else {
        None
    };

    // Best-effort InvokeExternal detection: prefer a non-pool inner call in
    // an outside-execution intent over a top-level non-pool call (the
    // multicall often contains plumbing — approve, AVNU forwarder — that's
    // *not* the helper). When neither is available, fall back to the first
    // non-pool top-level call.
    let invoke_external = outside_executions
        .iter()
        .flat_map(|oe| oe.inner_calls.iter())
        .find(|c| c.contract_address != *POOL_ADDRESS)
        .or_else(|| {
            decoded_calls
                .iter()
                .find(|c| c.contract_address != *POOL_ADDRESS)
        })
        .map(|c| InvokeExternalRef {
            target: c.contract_address,
            selector: c.selector,
            function_name: c.function_name.clone(),
        });

    // Paymaster mode (highest-fidelity signal first):
    //   1. OE wrapping the pool call — true SNIP-9 sponsorship.
    //   2. Top-level call to a known paymaster forwarder — AVNU gasless
    //      pattern where the user signs a multicall that routes through
    //      `0x12702...` to pay STRK fees.
    //   3. Sender is itself a known relayer.
    //   4. Otherwise: None.
    let paymaster = if !outside_executions.is_empty() {
        PaymasterMode::OutsideExecution
    } else if decoded_calls
        .iter()
        .any(|c| is_known_paymaster_forwarder(&c.contract_address))
    {
        PaymasterMode::PaymasterForwarder
    } else if is_known_paymaster_sender(&tx.sender()) {
        PaymasterMode::KnownRelayer
    } else {
        PaymasterMode::None
    };
    let intender = outside_executions.first().map(|oe| oe.intender);

    Some(PrivacySummary {
        actions,
        deposits,
        withdrawals,
        open_notes_created,
        open_notes_deposited,
        enc_notes_created,
        nullifiers,
        viewing_keys_set,
        invoke_external,
        pool_fee_fri,
        paymaster,
        intender,
    })
}

/// AVNU Forwarder + a few well-known paymaster relayers. Tx whose sender
/// matches one of these is overwhelmingly likely to be paymaster-sponsored.
fn is_known_paymaster_sender(sender: &Felt) -> bool {
    PAYMASTER_FORWARDERS.iter().any(|p| p == sender)
}

/// Known paymaster *forwarders* (contracts the user calls to delegate
/// fee-paying / relaying). Distinct from a paymaster *relayer account*
/// (which is what shows up as `tx.sender` and usually isn't easily
/// enumerable). For now both lists collapse into one — extend if a
/// distinction matters later.
fn is_known_paymaster_forwarder(addr: &Felt) -> bool {
    PAYMASTER_FORWARDERS.iter().any(|p| p == addr)
}

static PAYMASTER_FORWARDERS: LazyLock<Vec<Felt>> = LazyLock::new(|| {
    [
        // AVNU Forwarder (gasless v2 entry point)
        "0x0127021a1b5a52d3174c2ab077c2b043c80369250d29428cee956d76ee51584f",
        // AVNU Paymaster v2
        "0x02314f5a43e28fdffd953b2482749f9ed21ced41bfcda186dcbcd91cc61b4054",
    ]
    .iter()
    .filter_map(|h| Felt::from_hex(h).ok())
    .collect()
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::SnEvent;

    fn make_event(from: Felt, keys: Vec<Felt>, data: Vec<Felt>) -> DecodedEvent {
        DecodedEvent {
            contract_address: from,
            event_name: None,
            decoded_keys: Vec::new(),
            decoded_data: Vec::new(),
            raw: SnEvent {
                from_address: from,
                keys,
                data,
                transaction_hash: Felt::ZERO,
                block_number: 0,
                event_index: 0,
            },
        }
    }

    fn pool() -> Felt {
        *POOL_ADDRESS
    }

    fn pool_call() -> RawCall {
        RawCall {
            contract_address: pool(),
            selector: Felt::ZERO,
            data: Vec::new(),
            function_name: Some("apply_actions".to_string()),
            function_def: None,
            contract_abi: None,
        }
    }

    fn invoke_tx_with_sender(sender: Felt) -> SnTransaction {
        SnTransaction::Invoke(crate::data::types::InvokeTx {
            hash: Felt::ZERO,
            sender_address: sender,
            calldata: Vec::new(),
            nonce: None,
            version: Felt::from(3u64),
            actual_fee: None,
            execution_status: crate::data::types::ExecutionStatus::Succeeded,
            block_number: 0,
            index: 0,
            tip: 0,
            resource_bounds: None,
        })
    }

    #[test]
    fn validate_viewing_key_round_trips() {
        use crate::decode::privacy_crypto::keys::public_from_private;
        use crate::decode::privacy_crypto::types::SecretFelt;

        // Synthetic test scalar + arbitrary user address. Never use a
        // real viewing key in committed tests.
        let priv_key = SecretFelt::new(Felt::from(0xc0ffeeu64));
        let pub_key = public_from_private(&priv_key);
        let user = Felt::from(0xabcdu64);
        let reg = ViewingKeyRegistration {
            user_addr: user,
            public_key: pub_key,
        };
        assert_eq!(
            validate_viewing_key(&reg, &priv_key),
            ViewingKeyStatus::Valid
        );

        // A wrong registration with a perturbed public key must report Mismatch.
        let bad_reg = ViewingKeyRegistration {
            user_addr: user,
            public_key: pub_key + Felt::ONE,
        };
        assert_eq!(
            validate_viewing_key(&bad_reg, &priv_key),
            ViewingKeyStatus::Mismatch
        );
    }

    #[test]
    fn returns_none_for_non_privacy_tx() {
        let tx = invoke_tx_with_sender(Felt::from(1u64));
        let other_call = RawCall {
            contract_address: Felt::from_hex(
                "0x04270219d365d6b017231b52e92b3fb5d7c8378b05e9abc97724537a80e93b0f",
            )
            .unwrap(), // AVNU
            selector: Felt::ZERO,
            data: Vec::new(),
            function_name: None,
            function_def: None,
            contract_abi: None,
        };
        assert!(summarize(&tx, &[other_call], &[], &[]).is_none());
    }

    /// Mirrors the structure of mainnet tx
    /// 0x90304ef5b180c520ce866161c0a14618a72af5aa841fe38a243220aa05bfa2:
    /// 2 NoteUsed, 1 OpenNoteCreated, 1 Withdrawal, 1 OpenNoteDeposited.
    #[test]
    fn counts_action_mix_from_events() {
        let s = &*EVENT_SELECTORS;
        let tx = invoke_tx_with_sender(Felt::from(1u64));
        let p = pool();
        let null_a = Felt::from(0xAAu64);
        let null_b = Felt::from(0xBBu64);
        let token =
            Felt::from_hex("0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d")
                .unwrap();
        let note_id = Felt::from(0x100u64);
        let to_addr = Felt::from(0xCAFEu64);
        let depositor = Felt::from(0xBEEFu64);

        let events = vec![
            make_event(p, vec![s.note_used, null_a], vec![]),
            make_event(p, vec![s.note_used, null_b], vec![]),
            make_event(
                p,
                vec![s.open_note_created, token, note_id],
                vec![Felt::ZERO; 3], // enc_recipient_addr (3 felts)
            ),
            make_event(
                p,
                vec![s.withdrawal, to_addr, token],
                vec![
                    Felt::ZERO,
                    Felt::ZERO,
                    Felt::ZERO,
                    Felt::from(0xde0b6b3a7640000u128),
                ],
            ),
            make_event(
                p,
                vec![s.open_note_deposited, depositor, token, note_id],
                vec![Felt::from(0xde0b6b3a7640000u128), Felt::ZERO],
            ),
        ];

        let summary = summarize(&tx, &[pool_call()], &events, &[]).expect("privacy tx");
        assert_eq!(summary.actions.notes_used, 2);
        assert_eq!(summary.actions.open_notes_created, 1);
        assert_eq!(summary.actions.withdrawals, 1);
        assert_eq!(summary.actions.open_notes_deposited, 1);
        assert_eq!(summary.actions.total(), 5);
        assert_eq!(summary.nullifiers, vec![null_a, null_b]);
        assert_eq!(summary.withdrawals.len(), 1);
        assert_eq!(summary.withdrawals[0].to_addr, to_addr);
        assert_eq!(summary.withdrawals[0].token, token);
        assert_eq!(summary.withdrawals[0].amount, 0xde0b6b3a7640000u128);
        assert_eq!(summary.open_notes_deposited[0].depositor, depositor);
        assert_eq!(
            summary.open_notes_deposited[0].amount,
            0xde0b6b3a7640000u128
        );
    }

    #[test]
    fn detects_paymaster_sender() {
        let avnu =
            Felt::from_hex("0x0127021a1b5a52d3174c2ab077c2b043c80369250d29428cee956d76ee51584f")
                .unwrap();
        let tx = invoke_tx_with_sender(avnu);
        let summary = summarize(&tx, &[pool_call()], &[], &[]).unwrap();
        assert_eq!(summary.paymaster, PaymasterMode::KnownRelayer);
        assert!(summary.paymaster.is_sponsored());
    }

    /// Reproduces the shape of mainnet tx
    /// `0x4b9cde1b731130096209a2af5f947e3a57c290dcfa99c358bc958521ff3c35f`:
    /// the privacy pool call lives only inside an outside-execution intent;
    /// no top-level multicall entry targets the pool. The summarizer must
    /// still recognize it as a privacy tx (via the pool-emitted Withdrawal
    /// event) and report `OutsideExecution` sponsorship.
    #[test]
    fn detects_oe_wrapped_pool_call() {
        let s = &*EVENT_SELECTORS;
        let strk =
            Felt::from_hex("0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d")
                .unwrap();
        let avnu_forwarder =
            Felt::from_hex("0x0127021a1b5a52d3174c2ab077c2b043c80369250d29428cee956d76ee51584f")
                .unwrap();
        // Synthetic relayer account address (NOT in our known list, like
        // the real-world `0x22d28...` sender of this tx pattern).
        let relayer = Felt::from_hex("0x22d28").unwrap();
        let tx = invoke_tx_with_sender(relayer);

        // Top-level multicall: just a transfer + a forwarder call. No direct
        // pool reference — the pool only appears inside the OE inner_calls.
        let approve = RawCall {
            contract_address: strk,
            selector: Felt::ZERO,
            data: Vec::new(),
            function_name: Some("transfer".into()),
            function_def: None,
            contract_abi: None,
        };
        let forwarder_call = RawCall {
            contract_address: avnu_forwarder,
            selector: Felt::ZERO,
            data: Vec::new(),
            function_name: Some("execute_from_outside_v2".into()),
            function_def: None,
            contract_abi: None,
        };

        let user_addr = Felt::from(0xCAFEu64);
        let oe = OutsideExecutionInfo {
            intender: user_addr,
            caller: avnu_forwarder,
            nonce: Felt::ZERO,
            execute_after: 0,
            execute_before: u64::MAX,
            inner_calls: vec![RawCall {
                contract_address: pool(),
                selector: Felt::ZERO,
                data: Vec::new(),
                function_name: Some("apply_actions".into()),
                function_def: None,
                contract_abi: None,
            }],
            signature: Vec::new(),
            version: super::super::outside_execution::OutsideExecutionVersion::V2,
        };

        // Receipt: pool emits a Withdrawal sending 4 STRK to the AVNU
        // forwarder (paying the gasless service in private STRK).
        let pool_to_forwarder_amount: u128 = 0x3782dace9d900000;
        let events = vec![
            // ERC-20 Transfer pool→forwarder for the withdrawal payout.
            DecodedEvent {
                contract_address: strk,
                event_name: None,
                decoded_keys: Vec::new(),
                decoded_data: Vec::new(),
                raw: SnEvent {
                    from_address: strk,
                    keys: vec![
                        Felt::from_hex(
                            "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",
                        )
                        .unwrap(),
                        pool(),
                        avnu_forwarder,
                    ],
                    data: vec![Felt::from(pool_to_forwarder_amount), Felt::ZERO],
                    transaction_hash: Felt::ZERO,
                    block_number: 0,
                    event_index: 0,
                },
            },
            // Pool's matching Withdrawal event.
            make_event(
                pool(),
                vec![s.withdrawal, avnu_forwarder, strk],
                vec![
                    Felt::ZERO,
                    Felt::ZERO,
                    Felt::ZERO,
                    Felt::from(pool_to_forwarder_amount),
                ],
            ),
        ];

        let summary = summarize(&tx, &[approve, forwarder_call], &events, &[oe])
            .expect("OE-wrapped pool call should still be recognized as a privacy tx");
        assert_eq!(summary.actions.withdrawals, 1);
        assert!(
            summary.pool_fee_fri.is_none(),
            "withdrawal payout must not be counted as the pool fee"
        );
        assert_eq!(summary.paymaster, PaymasterMode::OutsideExecution);
        assert_eq!(summary.intender, Some(user_addr));
    }
}
