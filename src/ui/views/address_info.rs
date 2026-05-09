use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Tabs};

use starknet::core::types::Felt;

use crate::app::state::TxNavItem;
use crate::app::{AddressTab, App};
use crate::data::types::TokenBalance;
use crate::ui::theme;
use crate::ui::widgets::hex_display::{format_fri, format_strk_u128, tx_hash_cell};
use crate::ui::widgets::price;
use crate::ui::widgets::{search_bar, status_bar};
use crate::utils::{felt_to_u64, felt_to_u128};

/// How confident are we in the tab's count?
///
/// The address-view revamp locks down two display states:
///
/// - [`CountBound::Exact`] — the tab's total is known exactly (e.g. the
///   on-chain nonce for Txs, a Dune probe total for Calls). Renders as
///   `"N / total"` while we're filling, or bare `"N"` once `shown == total`.
/// - [`CountBound::LowerBound`] — "at least N; more may exist". Always
///   renders with an explicit denominator — `"N / hint+"` if a hint is
///   available and exceeds `shown`, otherwise `"N / N+"` because we
///   know at least `shown` items exist. This is the default when no
///   authoritative probe has completed — every tab starts here on
///   first paint.
///
/// Keeping this enum the single input to [`count_fragment`] prevents
/// display divergence between tabs: no tab accidentally renders bare `"N"`
/// when it only has a lower bound (the historical bug was Events/Calls
/// hiding the `"+"` once a scan paused).
enum CountBound {
    Exact(u64),
    LowerBound { hint: Option<u64> },
}

/// Render the parens content for a tab count given its [`CountBound`].
///
/// The single source of truth for what goes inside the `(...)` on each
/// tab — shared by the compact `draw_tabs` row and each per-tab body
/// title. Previously every site recomputed a fragment independently, and
/// display nits (like the MetaTxs body title lagging the compact row's
/// `"+"`) crept in.
///
/// Format rules:
/// - `Exact(N)`, `shown == N` → `"N"` (full knowledge, no `+`)
/// - `Exact(N)`, `shown < N` → `"shown / N"` (filling toward a known total)
/// - `LowerBound` with hint `H > shown` → `"shown / H+"` (inexact hint)
/// - `LowerBound` otherwise → `"shown / shown+"` (no better hint; we
///   still always render a denominator so "0/0+" on a cold tab beats
///   a lonely "0+")
fn count_fragment(shown: u64, bound: CountBound) -> String {
    match bound {
        CountBound::Exact(total) if shown < total => format!("{shown} / {total}"),
        CountBound::Exact(_) => shown.to_string(),
        CountBound::LowerBound { hint: Some(h) } if h > shown => format!("{shown} / {h}+"),
        CountBound::LowerBound { .. } => format!("{shown} / {shown}+"),
    }
}

/// Count fragment for the Txs tab.
///
/// Authoritative total is the on-chain nonce (one invoke per sender nonce),
/// so once nonce is known the bound is [`CountBound::Exact`]. Dune's event
/// count over-counts on hybrid accounts (it counts emitted events, not
/// sender txs) — not suitable here.
fn tx_count_fragment(app: &App) -> String {
    let count = app.address.txs.items.len() as u64;
    // The on-chain `nonce` is the *next* nonce to use. For a self-deployed
    // account (DEPLOY_ACCOUNT), the deploy itself consumed nonce 0 and is
    // pulled out of `txs.items` by `filter_deployment_txs`, so the Txs tab
    // ends up holding `nonce - 1` rows. For a UDC-deployed account the
    // "deployment" entry is the *deployer's* tx (sender != address), the
    // account's own nonce 0 is its first invoke and stays in `txs.items`,
    // so the tab holds the full `nonce` rows.
    //
    // Distinguish on `deployment.sender`: when it equals the account
    // address, the deploy was self-sent; otherwise it was UDC-style.
    let bound = match app.address.info.as_ref().map(|i| felt_to_u64(&i.nonce)) {
        Some(nonce) => {
            let self_deployed = matches!(
                (
                    app.address.context,
                    app.address.deployment.as_ref().and_then(|d| d.sender),
                ),
                (Some(addr), Some(sender)) if sender == addr
            );
            let total = if self_deployed {
                nonce.saturating_sub(1)
            } else {
                nonce
            };
            CountBound::Exact(total)
        }
        None => CountBound::LowerBound { hint: None },
    };
    count_fragment(count, bound)
}

/// Count fragment for the Calls tab.
///
/// Only the Dune probe total counts as "exact" here — pf-query-only
/// backfill can't promise completeness (misses calls to contracts that
/// don't emit events). Until Dune returns, we're in `LowerBound`. The
/// `event_window.min_searched > 0` signal becomes the lower-bound hint's
/// "+" indicator, not a claim of exactness.
fn call_count_fragment(app: &App) -> String {
    let count = app.address.calls.items.len() as u64;
    let dune_total = app
        .address
        .activity_probe
        .as_ref()
        .map(|p| p.callee_call_count);
    let bound = match dune_total {
        Some(total) => CountBound::Exact(total),
        None => CountBound::LowerBound { hint: None },
    };
    count_fragment(count, bound)
}

/// Count fragment for the Events tab.
///
/// Events are all `from_address == ADDR` logs, sourced via the unkeyed
/// event-window scan. Per the plan we can only claim [`CountBound::Exact`]
/// when the backwards scan has reached deploy block; until then we stay
/// [`CountBound::LowerBound`]. Shares the same `event_window.min_searched`
/// signal as MetaTxs — technically imprecise because a keyed scan on a
/// hybrid address could satisfy the floor check here even if the unkeyed
/// scan hasn't, but that's the only signal the UI has today.
fn events_count_fragment(app: &App) -> String {
    let count = app.address.events.items.len() as u64;
    let reached_floor = match (
        app.address.event_window.as_ref().map(|w| w.min_searched),
        app.address.deployment.as_ref().map(|d| d.block_number),
    ) {
        (Some(m), Some(d)) if m > 0 => m <= d,
        _ => false,
    };
    let bound = if reached_floor {
        CountBound::Exact(count)
    } else {
        CountBound::LowerBound { hint: None }
    };
    count_fragment(count, bound)
}

/// Count fragment for the MetaTxs tab.
///
/// Per the revamp spec we can only claim [`CountBound::Exact`] once the
/// backwards scan has reached (or crossed) the deploy block — only then
/// have we demonstrably seen every `execute_from_outside` targeting this
/// address. Until that holds we stay [`CountBound::LowerBound`], even if
/// `meta_tx_has_more` has flipped to false because the current fetch
/// returned no new rows: a dry page doesn't imply history exhausted.
///
/// The function now always returns a fragment — callers render the label
/// even on cold state so the tab doesn't silently hide behind a probe.
/// "0+" on first paint is strictly more informative than no count at all.
fn meta_tx_count_fragment(app: &App) -> String {
    let count = app.address.meta_txs.items.len() as u64;
    // Reached deploy-floor iff we know both values AND the scan has a
    // non-zero min_searched <= deploy_block. `min_searched == 0` is the
    // "never scanned" sentinel — never promote on that.
    let reached_floor = match (
        app.address.event_window.as_ref().map(|w| w.min_searched),
        app.address.deployment.as_ref().map(|d| d.block_number),
    ) {
        (Some(m), Some(d)) if m > 0 => m <= d,
        _ => false,
    };
    let bound = if reached_floor {
        CountBound::Exact(count)
    } else {
        CountBound::LowerBound { hint: None }
    };
    count_fragment(count, bound)
}

/// Title suffix describing any passive gap the event-window helper reported
/// on its last fetch. Shared between the Calls and MetaTxs tabs (Events tab
/// is intentionally excluded here — see task #13 for a follow-up review).
/// Returns an empty string when no gap is known, so callers can always
/// concatenate without a None-check.
fn event_window_gap_suffix(app: &App) -> String {
    let Some(hint) = app.address.event_window.as_ref() else {
        return String::new();
    };
    match hint.deferred_gap {
        Some((lo, hi)) => {
            let span = hi.saturating_sub(lo).saturating_add(1);
            format!(" — gap {lo}..{hi} ({span} blocks deferred) ")
        }
        None => String::new(),
    }
}

pub fn draw(f: &mut Frame, app: &mut App) {
    let has_deploy = app.address.deployment.is_some();
    let has_deployer = app
        .address
        .deployment
        .as_ref()
        .and_then(|d| d.sender)
        .is_some_and(|s| {
            app.address.info.as_ref().is_none_or(|i| s != i.address) && s != Felt::ZERO
        });
    // 2 borders + 2 base lines + 1 per deployment line (tx hash, deployer)
    let header_height = 4 + u16::from(has_deploy) + u16::from(has_deployer);
    let chunks = Layout::vertical([
        Constraint::Length(1),             // search bar
        Constraint::Length(header_height), // header
        Constraint::Length(1),             // tabs
        Constraint::Min(5),                // tab content
        Constraint::Length(1),             // status bar
    ])
    .split(f.area());

    search_bar::draw_input(f, app, chunks[0]);
    draw_header(f, app, chunks[1]);
    draw_tabs(f, app, chunks[2]);

    match app.address.tab {
        AddressTab::Transactions => draw_transactions_tab(f, app, chunks[3]),
        AddressTab::MetaTxs => draw_meta_txs_tab(f, app, chunks[3]),
        AddressTab::Calls => draw_calls_tab(f, app, chunks[3]),
        AddressTab::Balances => draw_balances_tab(f, app, chunks[3]),
        AddressTab::Events => draw_events_tab(f, app, chunks[3]),
        AddressTab::ClassHistory => draw_class_history_tab(f, app, chunks[3]),
    }

    status_bar::draw(f, app, chunks[4]);

    // Search dropdown overlay (last)
    search_bar::draw_dropdown(f, app, chunks[0]);
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let info = match &app.address.info {
        Some(i) => i,
        None => {
            // Show the address we're loading, if known
            let addr_str = app
                .address
                .context
                .map(|a| format!(" Loading {:#x}...", a))
                .unwrap_or_else(|| " Loading address...".to_string());
            f.render_widget(Paragraph::new(addr_str).style(theme::STATUS_LOADING), area);
            return;
        }
    };

    let label = app.format_address_full(&info.address);
    let class_hash_str = info
        .class_hash
        .map(|c| format!("{:#x}", c))
        .unwrap_or_else(|| "N/A".into());

    // Current visual selection
    let selected: Option<&TxNavItem> = if app.address.visual_mode {
        app.address.nav_items.get(app.address.nav_cursor)
    } else {
        None
    };

    let mut lines = vec![Line::from(vec![
        Span::styled(" ", theme::NORMAL_STYLE),
        Span::styled(&label, theme::LABEL_STYLE),
        Span::styled(format!("  {:#x}", info.address), theme::BLOCK_HASH_STYLE),
    ])];

    // Class line — show full hash, highlight if selected
    {
        let ch_selected = info
            .class_hash
            .is_some_and(|ch| matches!(selected, Some(TxNavItem::ClassHash(c)) if *c == ch));
        let ch_style = if ch_selected {
            theme::VISUAL_SELECTED_STYLE
        } else {
            theme::TX_HASH_STYLE
        };
        let marker = if ch_selected { "►" } else { " " };

        let mut class_line = vec![
            Span::styled(
                marker,
                if ch_selected {
                    theme::VISUAL_SELECTED_STYLE
                } else {
                    theme::NORMAL_STYLE
                },
            ),
            Span::styled("Class: ", theme::NORMAL_STYLE),
            Span::styled(&class_hash_str, ch_style),
        ];
        if let Some(vl) = app.voyager_labels.get(&info.address)
            && let Some(ca) = &vl.class_alias
            && !ca.is_empty()
        {
            class_line.push(Span::styled(format!(" [{}]", ca), theme::LABEL_STYLE));
        }
        class_line.push(Span::styled(
            format!("  Nonce: {}", felt_to_u64(&info.nonce)),
            theme::NORMAL_STYLE,
        ));
        lines.push(Line::from(class_line));
    }

    if let Some(deploy) = &app.address.deployment {
        let has_tx = deploy.hash != Felt::ZERO;

        // Deploy block — navigable
        let blk_selected = deploy.block_number > 0
            && matches!(selected, Some(TxNavItem::Block(b)) if *b == deploy.block_number);
        let blk_style = if blk_selected {
            theme::VISUAL_SELECTED_STYLE
        } else {
            theme::BLOCK_NUMBER_STYLE
        };

        if has_tx {
            // Full deploy info: tx hash + block
            let tx_selected =
                matches!(selected, Some(TxNavItem::Transaction(h)) if *h == deploy.hash);
            let tx_style = if tx_selected {
                theme::VISUAL_SELECTED_STYLE
            } else {
                theme::TX_HASH_STYLE
            };
            let tx_marker = if tx_selected { "►" } else { " " };
            lines.push(Line::from(vec![
                Span::styled(
                    tx_marker,
                    if tx_selected {
                        theme::VISUAL_SELECTED_STYLE
                    } else {
                        theme::NORMAL_STYLE
                    },
                ),
                Span::styled("Deployed at: ", theme::NORMAL_STYLE),
                Span::styled(format!("{:#x}", deploy.hash), tx_style),
                Span::styled("  Block: ", theme::NORMAL_STYLE),
                Span::styled(format!("{}", deploy.block_number), blk_style),
            ]));
        } else {
            // Partial deploy info (from class_history): block only
            let blk_marker = if blk_selected { "►" } else { " " };
            lines.push(Line::from(vec![
                Span::styled(
                    blk_marker,
                    if blk_selected {
                        theme::VISUAL_SELECTED_STYLE
                    } else {
                        theme::NORMAL_STYLE
                    },
                ),
                Span::styled("Deployed at: ", theme::NORMAL_STYLE),
                Span::styled("Block: ", theme::NORMAL_STYLE),
                Span::styled(format!("{}", deploy.block_number), blk_style),
            ]));
        }

        // Show deployer on its own line when it's a different address
        if let Some(sender) = deploy.sender
            && sender != info.address
            && sender != Felt::ZERO
        {
            let addr_selected = matches!(selected, Some(TxNavItem::Address(a)) if *a == sender);
            let addr_style = if addr_selected {
                theme::VISUAL_SELECTED_STYLE
            } else {
                theme::LABEL_STYLE
            };
            let addr_marker = if addr_selected { "►" } else { " " };
            lines.push(Line::from(vec![
                Span::styled(
                    addr_marker,
                    if addr_selected {
                        theme::VISUAL_SELECTED_STYLE
                    } else {
                        theme::NORMAL_STYLE
                    },
                ),
                Span::styled("Deployed by: ", theme::NORMAL_STYLE),
                Span::styled(format!("{:#x}", sender), addr_style),
                {
                    let label = app.format_address_full(&sender);
                    if label.starts_with("0x") {
                        Span::raw("")
                    } else {
                        Span::styled(format!("  {}", label), theme::LABEL_STYLE)
                    }
                },
            ]));
        }
    }

    let widget = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::BORDER_FOCUSED_STYLE)
            .title(Span::styled(" Address ", theme::TITLE_STYLE)),
    );
    f.render_widget(widget, area);
}

fn draw_tabs(f: &mut Frame, app: &App, area: Rect) {
    let bal_count = app
        .address
        .info
        .as_ref()
        .map(|i| i.token_balances.len())
        .unwrap_or(0);
    let class_count = app.address.class_history.len();

    let tx_label = format!(" Txs ({}) ", tx_count_fragment(app));
    let call_label = format!(" Calls ({}) ", call_count_fragment(app));
    // The fragment is always rendered — "0+" on cold state beats a bare
    // label that hides the tab's existence while the classifier spins up.
    let meta_label = format!(" MetaTxs ({}) ", meta_tx_count_fragment(app));

    // Per-token unspent-incoming count for the Balances tab. Suppressed
    // when the address has no viewing key (or no holdings yet) so the
    // tab label stays clean for the common non-privacy case.
    let prv_count = app
        .address
        .info
        .as_ref()
        .map(|i| compute_private_holdings(app, i.address).len())
        .unwrap_or(0);
    let balances_title: Line = if prv_count > 0 {
        Line::from(vec![
            Span::raw(format!(" Balances ({bal_count}) ")),
            Span::styled(format!("Prv ({prv_count}) "), theme::PRIVACY_STYLE),
        ])
    } else {
        Line::from(Span::raw(format!(" Balances ({bal_count}) ")))
    };

    let titles: Vec<Line> = vec![
        Line::from(Span::raw(tx_label)),
        Line::from(Span::raw(meta_label)),
        Line::from(Span::raw(call_label)),
        balances_title,
        Line::from(Span::raw(format!(
            " Events ({}) ",
            events_count_fragment(app)
        ))),
        Line::from(Span::raw(format!(" Class ({class_count}) "))),
    ];
    let selected = match app.address.tab {
        AddressTab::Transactions => 0,
        AddressTab::MetaTxs => 1,
        AddressTab::Calls => 2,
        AddressTab::Balances => 3,
        AddressTab::Events => 4,
        AddressTab::ClassHistory => 5,
    };
    let tabs = Tabs::new(titles)
        .select(selected)
        .highlight_style(theme::TITLE_STYLE.add_modifier(Modifier::UNDERLINED))
        .divider(Span::raw(" | "));
    f.render_widget(tabs, area);
}

fn draw_transactions_tab(f: &mut Frame, app: &mut App, area: Rect) {
    // Column headers
    let header_area = Rect { height: 1, ..area };
    let list_area = Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        ..area
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled("   Nonce     ", theme::SUGGESTION_STYLE),
        Span::styled("Type            ", theme::SUGGESTION_STYLE),
        Span::styled("Hash          ", theme::SUGGESTION_STYLE),
        Span::styled("Contracts                     ", theme::SUGGESTION_STYLE),
        Span::styled("Endpoint(s)                    ", theme::SUGGESTION_STYLE),
        Span::styled("Fee(STRK)        ", theme::SUGGESTION_STYLE),
        Span::styled("Tip              ", theme::SUGGESTION_STYLE),
        Span::styled("Block     ", theme::SUGGESTION_STYLE),
        Span::styled("St  ", theme::SUGGESTION_STYLE),
        Span::styled("Age  ", theme::SUGGESTION_STYLE),
    ]));
    f.render_widget(header, header_area);

    if app.address.txs.items.is_empty() {
        let msg = if app.is_loading {
            " Loading transactions..."
        } else {
            " No transactions found"
        };
        f.render_widget(
            Paragraph::new(msg).style(theme::SUGGESTION_STYLE).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::BORDER_STYLE),
            ),
            area,
        );
        return;
    }

    // (tx_idx, lo_nonce) for each gap, sorted by tx_idx ascending. Each gap
    // renders as its own ListItem above the lo_nonce tx.
    let gap_positions = app.address.gap_render_positions();
    let mut next_gap = gap_positions.iter().peekable();
    let gap_info_for_lo = |lo: u64| -> Option<(u64, bool)> {
        app.address
            .unfilled_gaps
            .iter()
            .find(|g| g.lo_nonce == lo)
            .map(|g| (g.missing_count, g.fill_dispatched))
    };

    let mut items: Vec<ListItem> =
        Vec::with_capacity(app.address.txs.items.len() + gap_positions.len());
    for (idx, tx) in app.address.txs.items.iter().enumerate() {
        while let Some(&&(p, lo)) = next_gap.peek()
            && p == idx
        {
            if let Some((missing, dispatched)) = gap_info_for_lo(lo) {
                let msg = if dispatched {
                    format!(" ── gap of {missing} txs — loading / press r to retry ──")
                } else {
                    format!(" ── {missing} txs hidden — press Enter to load ──")
                };
                items.push(ListItem::new(Line::from(Span::styled(
                    msg,
                    theme::SUGGESTION_STYLE,
                ))));
            }
            next_gap.next();
        }

        let fee_str = format_strk_u128(tx.total_fee_fri)
            .trim_end_matches(" STRK")
            .to_string();
        let tip_str = if tx.tip > 0 {
            format_fri(tx.tip as u128)
        } else {
            "0".to_string()
        };
        let age = format_age(tx.timestamp);
        let endpoint = if tx.endpoint_names.chars().count() > 30 {
            let truncated: String = tx.endpoint_names.chars().take(29).collect();
            format!("{truncated}…")
        } else {
            tx.endpoint_names.clone()
        };
        let contracts_display = format_called_contracts(app, &tx.called_contracts);

        let status_style = match tx.status.as_str() {
            "OK" => theme::STATUS_OK,
            "REV" => theme::STATUS_REVERTED,
            _ => theme::SUGGESTION_STYLE,
        };

        let type_style = match tx.tx_type.as_str() {
            "INVOKE" => theme::TX_TYPE_INVOKE,
            "DECLARE" => theme::TX_TYPE_DECLARE,
            "DEPLOY_ACCOUNT" | "DEPLOY" => theme::TX_TYPE_DEPLOY,
            "L1_HANDLER" => theme::TX_TYPE_L1HANDLER,
            _ => theme::NORMAL_STYLE,
        };

        let tx_label = app.resolve_tx(&tx.hash);
        let tx_hash_display = tx_hash_cell(tx_label, &tx.hash);
        let tx_hash_style = if tx_label.is_some() {
            theme::LABEL_STYLE
        } else {
            theme::TX_HASH_STYLE
        };

        let main_line = Line::from(vec![
            Span::styled(format!(" {:<8}", tx.nonce), theme::NORMAL_STYLE),
            Span::styled(format!("{:<15}", tx.tx_type), type_style),
            Span::styled(format!("{:<14}", tx_hash_display), tx_hash_style),
            Span::styled(format!("{:<30}", contracts_display), theme::LABEL_STYLE),
            Span::styled(format!("{:<31}", endpoint), theme::LABEL_STYLE),
            Span::styled(format!("{:<17}", fee_str), theme::TX_FEE_STYLE),
            Span::styled(format!("{:<17}", tip_str), theme::SUGGESTION_STYLE),
            Span::styled(
                format!("#{:<9}", tx.block_number),
                theme::BLOCK_NUMBER_STYLE,
            ),
            Span::styled(format!("{:<4}", &tx.status), status_style),
            Span::styled(age, theme::BLOCK_AGE_STYLE),
        ]);
        items.push(ListItem::new(main_line));
    }

    let gap_suffix = if app.address.unfilled_gaps.is_empty() {
        String::new()
    } else {
        let total_missing: u64 = app
            .address
            .unfilled_gaps
            .iter()
            .map(|g| g.missing_count)
            .sum();
        let n = app.address.unfilled_gaps.len();
        let any_pending = app.address.unfilled_gaps.iter().any(|g| !g.fill_dispatched);
        if any_pending {
            format!(
                " — {n} gap{plural} ({total_missing} txs deferred, Enter on a gap row to load) ",
                plural = if n == 1 { "" } else { "s" }
            )
        } else {
            format!(
                " — {n} gap{plural} ({total_missing} txs, press r to retry) ",
                plural = if n == 1 { "" } else { "s" }
            )
        }
    };
    let count = tx_count_fragment(app);
    let title = if app.is_loading {
        format!(" Transactions ({count}) fetching...{gap_suffix} ")
    } else {
        format!(" Transactions ({count}){gap_suffix} ")
    };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_STYLE)
                .title(Span::styled(title, theme::TITLE_STYLE)),
        )
        .highlight_style(theme::SELECTED_STYLE.add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");

    // Sync rendered selection (gap-aware) into the persistent render state.
    app.address
        .txs_render_state
        .select(app.address.tx_list_rendered_selected());
    f.render_stateful_widget(list, list_area, &mut app.address.txs_render_state);
}

/// Render the contracts-called column: up to the first two contract labels
/// (registry/Voyager-resolved or short hex), followed by ` +N` for any
/// remaining. Each label is truncated to share the 29-char content budget;
/// any slack left by a short first label spills over to the second.
fn format_called_contracts(app: &App, contracts: &[Felt]) -> String {
    const BUDGET: usize = 29;
    if contracts.is_empty() {
        return String::new();
    }
    let labels: Vec<String> = contracts.iter().map(|c| app.format_address(c)).collect();
    if labels.len() == 1 {
        return truncate_to(&labels[0], BUDGET);
    }
    let extra = labels.len().saturating_sub(2);
    let suffix = if extra > 0 {
        format!(" +{extra}")
    } else {
        String::new()
    };
    const SEP: &str = ", ";
    let usable = BUDGET.saturating_sub(suffix.chars().count() + SEP.len());
    let a_budget = usable / 2;
    let a = truncate_to(&labels[0], a_budget);
    let b_budget = usable.saturating_sub(a.chars().count());
    let b = truncate_to(&labels[1], b_budget);
    format!("{a}{SEP}{b}{suffix}")
}

/// Truncate a label to fit `max` chars, appending `…` when shortened. Returns
/// an empty string if `max` is 0 (caller is responsible for not asking for
/// space it doesn't have).
fn truncate_to(label: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if label.chars().count() <= max {
        return label.to_string();
    }
    let head: String = label.chars().take(max - 1).collect();
    format!("{head}…")
}

fn format_age(timestamp: u64) -> String {
    if timestamp == 0 {
        return String::new();
    }
    let now = chrono::Utc::now().timestamp() as u64;
    if timestamp > now {
        return "now".to_string();
    }
    let diff = now - timestamp;
    if diff < 60 {
        format!("{diff}s")
    } else if diff < 3600 {
        format!("{}m", diff / 60)
    } else if diff < 86400 {
        format!("{}h", diff / 3600)
    } else {
        format!("{}d", diff / 86400)
    }
}

fn draw_calls_tab(f: &mut Frame, app: &mut App, area: Rect) {
    // Column headers
    let header_area = Rect { height: 1, ..area };
    let list_area = Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        ..area
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled("    Sender                   ", theme::SUGGESTION_STYLE),
        Span::styled("Endpoint(s)                    ", theme::SUGGESTION_STYLE),
        Span::styled("Hash          ", theme::SUGGESTION_STYLE),
        Span::styled("Nonce     ", theme::SUGGESTION_STYLE),
        Span::styled("Fee(STRK)        ", theme::SUGGESTION_STYLE),
        Span::styled("Tip              ", theme::SUGGESTION_STYLE),
        Span::styled("Block     ", theme::SUGGESTION_STYLE),
        Span::styled("St  ", theme::SUGGESTION_STYLE),
        Span::styled("Age  ", theme::SUGGESTION_STYLE),
    ]));
    f.render_widget(header, header_area);

    if app.address.calls.items.is_empty() {
        let msg = if app.is_loading {
            " Loading contract calls..."
        } else {
            " No calls found"
        };
        f.render_widget(
            Paragraph::new(msg).style(theme::SUGGESTION_STYLE).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::BORDER_STYLE),
            ),
            list_area,
        );
        return;
    }

    // Refresh the per-sender count + color slot cache. No-op when the calls
    // list hasn't grown since the last render.
    let registry = app.search_engine.as_ref().map(|e| e.registry());
    app.address
        .update_call_color_map(|addr| registry.is_some_and(|r| r.is_known(addr)));

    let items: Vec<ListItem> = app
        .address
        .calls
        .items
        .iter()
        .map(|call| {
            let is_known = registry.is_some_and(|r| r.is_known(&call.sender));
            let sender_style = if is_known {
                theme::LABEL_STYLE
            } else {
                app.address.call_color_map.style_for(&call.sender)
            };
            let sender_label = app.format_address(&call.sender);
            let sender_display = if sender_label.chars().count() > 25 {
                let truncated: String = sender_label.chars().take(24).collect();
                format!("{truncated}…")
            } else {
                sender_label
            };
            let func = if call.function_name.chars().count() > 30 {
                let truncated: String = call.function_name.chars().take(29).collect();
                format!("{truncated}…")
            } else {
                call.function_name.clone()
            };
            let fee_str = format_strk_u128(call.total_fee_fri)
                .trim_end_matches(" STRK")
                .to_string();
            let nonce_str = match call.nonce {
                Some(n) => n.to_string(),
                None => "—".to_string(),
            };
            let tip_str = if call.tip > 0 {
                format_fri(call.tip as u128)
            } else {
                "0".to_string()
            };
            let status_style = match call.status.as_str() {
                "OK" => theme::STATUS_OK,
                "REV" => theme::STATUS_REVERTED,
                _ => theme::SUGGESTION_STYLE,
            };

            let tx_label = app.resolve_tx(&call.tx_hash);
            let tx_hash_display = tx_hash_cell(tx_label, &call.tx_hash);
            let tx_hash_style = if tx_label.is_some() {
                theme::LABEL_STYLE
            } else {
                theme::TX_HASH_STYLE
            };

            let line = Line::from(vec![
                Span::styled(format!(" {:<25} ", sender_display), sender_style),
                Span::styled(format!("{:<31}", func), theme::LABEL_STYLE),
                Span::styled(format!("{:<14}", tx_hash_display), tx_hash_style),
                Span::styled(format!("{:<10}", nonce_str), theme::NORMAL_STYLE),
                Span::styled(format!("{:<17}", fee_str), theme::TX_FEE_STYLE),
                Span::styled(format!("{:<17}", tip_str), theme::SUGGESTION_STYLE),
                Span::styled(
                    format!("#{:<9}", call.block_number),
                    theme::BLOCK_NUMBER_STYLE,
                ),
                Span::styled(format!("{:<4}", &call.status), status_style),
                Span::styled(format_age(call.timestamp), theme::BLOCK_AGE_STYLE),
            ]);
            ListItem::new(line)
        })
        .collect();

    let gap_suffix = event_window_gap_suffix(app);
    let count = call_count_fragment(app);
    let title = if app.is_loading {
        format!(" Calls ({count}) fetching...{gap_suffix} ")
    } else {
        format!(" Calls ({count}){gap_suffix} ")
    };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_STYLE)
                .title(Span::styled(title, theme::TITLE_STYLE)),
        )
        .highlight_style(theme::SELECTED_STYLE.add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");

    f.render_stateful_widget(list, list_area, &mut app.address.calls.state);
}

fn draw_meta_txs_tab(f: &mut Frame, app: &mut App, area: Rect) {
    let header_area = Rect { height: 1, ..area };
    let list_area = Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        ..area
    };
    // 3 leading spaces match the ">> " highlight_symbol on the list rows.
    let header = Paragraph::new(Line::from(vec![
        Span::styled("   Age   ", theme::SUGGESTION_STYLE),
        Span::styled("Hash          ", theme::SUGGESTION_STYLE),
        Span::styled("Block      ", theme::SUGGESTION_STYLE),
        Span::styled("Paymaster            ", theme::SUGGESTION_STYLE),
        Span::styled("Ver   ", theme::SUGGESTION_STYLE),
        Span::styled("Protocol(s)          ", theme::SUGGESTION_STYLE),
        Span::styled(
            "Endpoint(s)                        ",
            theme::SUGGESTION_STYLE,
        ),
        Span::styled("Fee(STRK)      ", theme::SUGGESTION_STYLE),
        Span::styled("St  ", theme::SUGGESTION_STYLE),
    ]));
    f.render_widget(header, header_area);

    if app.address.meta_txs.items.is_empty() {
        let msg = if app.address.fetching_meta_txs || !app.address.meta_txs_dispatched {
            " Scanning for meta-transactions..."
        } else {
            " No meta-transactions found (requires pf-query; only Argent/Braavos accounts)"
        };
        f.render_widget(
            Paragraph::new(msg).style(theme::SUGGESTION_STYLE).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::BORDER_STYLE),
            ),
            list_area,
        );
        return;
    }

    let items: Vec<ListItem> = app
        .address
        .meta_txs
        .items
        .iter()
        .map(|m| {
            let age = format_age(m.timestamp);
            let paymaster_label = app.format_address(&m.paymaster);
            let paymaster_display = if paymaster_label.chars().count() > 20 {
                let truncated: String = paymaster_label.chars().take(19).collect();
                format!("{truncated}…")
            } else {
                paymaster_label
            };

            // Protocol column: first inner target (labeled) + " +N" if more.
            let protocol = match m.inner_targets.first() {
                Some(t) => {
                    let base = app.format_address(t);
                    let trimmed: String = if base.chars().count() > 16 {
                        let t: String = base.chars().take(15).collect();
                        format!("{t}…")
                    } else {
                        base
                    };
                    let extra = m.inner_targets.len().saturating_sub(1);
                    if extra > 0 {
                        format!("{trimmed} +{extra}")
                    } else {
                        trimmed
                    }
                }
                None => "-".to_string(),
            };
            let protocol_display = if protocol.chars().count() > 20 {
                let truncated: String = protocol.chars().take(19).collect();
                format!("{truncated}…")
            } else {
                protocol
            };

            let endpoint = if m.inner_endpoints.chars().count() > 34 {
                let truncated: String = m.inner_endpoints.chars().take(33).collect();
                format!("{truncated}…")
            } else {
                m.inner_endpoints.clone()
            };

            let fee_str = format_strk_u128(m.total_fee_fri)
                .trim_end_matches(" STRK")
                .to_string();

            let status_style = match m.status.as_str() {
                "OK" => theme::STATUS_OK,
                "REV" => theme::STATUS_REVERTED,
                _ => theme::SUGGESTION_STYLE,
            };

            let tx_label = app.resolve_tx(&m.hash);
            let tx_hash_display = tx_hash_cell(tx_label, &m.hash);
            let tx_hash_style = if tx_label.is_some() {
                theme::LABEL_STYLE
            } else {
                theme::TX_HASH_STYLE
            };

            let line = Line::from(vec![
                Span::styled(format!(" {:<5}", age), theme::BLOCK_AGE_STYLE),
                Span::styled(format!("{:<14}", tx_hash_display), tx_hash_style),
                Span::styled(
                    format!("#{:<10}", m.block_number),
                    theme::BLOCK_NUMBER_STYLE,
                ),
                Span::styled(format!("{:<21}", paymaster_display), theme::LABEL_STYLE),
                Span::styled(format!("{:<6}", m.version), theme::SUGGESTION_STYLE),
                Span::styled(format!("{:<21}", protocol_display), theme::LABEL_STYLE),
                Span::styled(format!("{:<35}", endpoint), theme::LABEL_STYLE),
                Span::styled(format!("{:<15}", fee_str), theme::TX_FEE_STYLE),
                Span::styled(format!("{:<4}", &m.status), status_style),
            ]);
            ListItem::new(line)
        })
        .collect();

    let gap_suffix = event_window_gap_suffix(app);
    // The body title shares the same fragment helper as the compact tab row.
    let count = meta_tx_count_fragment(app);
    // While an adaptive walk is in flight, surface how far back we've
    // scanned — helpful signal for sparse addresses where the first few
    // windows return nothing and the list appears to hang.
    let scan_suffix = match app.address.event_window.as_ref() {
        Some(hint) if hint.min_searched > 0 && app.address.fetching_meta_txs => {
            format!(" scanned to block {}", hint.min_searched)
        }
        _ => String::new(),
    };
    let title = if app.address.fetching_meta_txs {
        format!(" MetaTxs ({count}) fetching...{scan_suffix}{gap_suffix} ")
    } else {
        format!(" MetaTxs ({count}){gap_suffix} ")
    };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_STYLE)
                .title(Span::styled(title, theme::TITLE_STYLE)),
        )
        .highlight_style(theme::SELECTED_STYLE.add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");

    f.render_stateful_widget(list, list_area, &mut app.address.meta_txs.state);
}

fn draw_balances_tab(f: &mut Frame, app: &App, area: Rect) {
    let info = match &app.address.info {
        Some(i) => i,
        None => return,
    };

    let nonzero: Vec<&TokenBalance> = info
        .token_balances
        .iter()
        .filter(|b| felt_to_u128(&b.balance_raw) > 0)
        .collect();

    // Aggregate the user's *private* holdings (viewing-key decrypted,
    // unspent incoming notes). Outgoing notes are excluded — those belong
    // to recipients now. Spent incoming notes are excluded via the
    // sync-time `nullifiers[*]` slot read.
    let private_by_token: Vec<(Felt, u128, usize)> = compute_private_holdings(app, info.address);

    // Layout: when there are private holdings, stack both panels at
    // their natural heights and absorb any leftover space below.
    // Otherwise keep the historical full-area Token Balances widget.
    if private_by_token.is_empty() {
        draw_token_balances(f, app, area, &nonzero);
        return;
    }
    // +2 = border rows (top + bottom). The "+ 1" floor keeps an empty
    // panel from collapsing below "Title" + 1 row.
    let token_rows = nonzero.len().max(1) as u16;
    let token_height = (token_rows + 2).clamp(3, 12);
    let holdings_rows = private_by_token.len() as u16;
    let holdings_height = (holdings_rows + 2).clamp(3, 12);
    let chunks = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            Constraint::Length(token_height),
            Constraint::Length(holdings_height),
            Constraint::Min(0),
        ])
        .split(area);
    draw_token_balances(f, app, chunks[0], &nonzero);
    draw_private_holdings(f, app, chunks[1], &private_by_token);
}

/// Pad-or-truncate a token label to a fixed display width so amount
/// columns line up across rows whose token names vary in length (e.g.
/// "STRK" vs. an unknown `0x6d6d…6854`). Truncation marks with `…` so
/// the row stays readable.
fn fmt_token_name(name: &str, width: usize) -> String {
    fmt_fixed_width(name, width)
}

/// Pad-with-spaces or truncate-with-`…` to exactly `width` columns.
/// Used to keep every column in the Balances tab at a fixed character
/// budget so the `·` separator aligns regardless of value length —
/// otherwise an unknown-decimals raw u128 amount (~20 digits) bumps
/// the suffix off-grid.
fn fmt_fixed_width(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len <= width {
        let pad = " ".repeat(width - len);
        format!("{s}{pad}")
    } else if width == 0 {
        String::new()
    } else {
        let truncated: String = s.chars().take(width.saturating_sub(1)).collect();
        format!("{truncated}…")
    }
}

fn draw_token_balances(f: &mut Frame, app: &App, area: Rect, nonzero: &[&TokenBalance]) {
    if nonzero.is_empty() {
        f.render_widget(
            Paragraph::new(" No token balances found")
                .style(theme::SUGGESTION_STYLE)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(theme::BORDER_STYLE)
                        .title(Span::styled(" Token Balances ", theme::TITLE_STYLE)),
                ),
            area,
        );
        return;
    }

    let items: Vec<ListItem> = nonzero
        .iter()
        .map(|bal| {
            let formatted = format_token_balance(bal);
            let usd_str = balance_usd_value(app, bal)
                .map(price::format_usd)
                .unwrap_or_default();
            let spans = vec![
                Span::styled(
                    format!(" {}  ", fmt_token_name(&bal.token_name, 10)),
                    theme::LABEL_STYLE,
                ),
                Span::styled(fmt_fixed_width(&formatted, 18), theme::NORMAL_STYLE),
                Span::styled(
                    format!("  {}", fmt_fixed_width(&usd_str, 8)),
                    theme::SUGGESTION_STYLE,
                ),
            ];
            ListItem::new(Line::from(spans))
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::BORDER_STYLE)
            .title(Span::styled(" Token Balances ", theme::TITLE_STYLE)),
    );
    f.render_widget(list, area);
}

/// Sum unspent incoming decrypted-note amounts for `address`, grouped by
/// token. Returns `(token, amount, n_unspent_notes)` rows sorted by
/// amount descending. Empty when the address has no viewing key, no
/// notes synced yet, or every note is spent.
fn compute_private_holdings(app: &App, address: Felt) -> Vec<(Felt, u128, usize)> {
    use crate::decode::privacy_sync::NoteDirection;
    let mut by_token: std::collections::HashMap<Felt, (u128, usize)> =
        std::collections::HashMap::new();
    for note in app.private_notes.values() {
        if note.user != address {
            continue;
        }
        if note.spent {
            continue;
        }
        if note.direction != NoteDirection::Incoming {
            continue;
        }
        let entry = by_token.entry(note.token).or_insert((0u128, 0usize));
        entry.0 = entry.0.saturating_add(note.amount);
        entry.1 += 1;
    }
    let mut rows: Vec<(Felt, u128, usize)> =
        by_token.into_iter().map(|(t, (a, n))| (t, a, n)).collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    rows
}

fn draw_private_holdings(f: &mut Frame, app: &App, area: Rect, rows: &[(Felt, u128, usize)]) {
    let items: Vec<ListItem> = rows
        .iter()
        .map(|(token, amount, n_unspent)| {
            // Prefer the registry/runtime-fetched ticker over the
            // truncated address. `App::token_symbol` consults the
            // static registry first, then `fetched_token_metadata`, so
            // tokens whose `symbol()` was just fetched render the
            // ticker (e.g. `USDS`) instead of `0x6d6d…68…`.
            let token_name = app.token_symbol(token).unwrap_or_else(|| short_addr(token));
            let amount_str =
                crate::ui::views::tx_detail::format_amount_for_token(app, token, *amount);
            let suffix = if *n_unspent == 1 {
                "1 unspent note".to_string()
            } else {
                format!("{} unspent notes", n_unspent)
            };
            let usd_str = private_holding_usd(app, token, *amount)
                .map(price::format_usd)
                .unwrap_or_default();
            let spans = vec![
                Span::styled(
                    format!(" {}  ", fmt_token_name(&token_name, 10)),
                    theme::LABEL_STYLE,
                ),
                Span::styled(fmt_fixed_width(&amount_str, 18), theme::NORMAL_STYLE),
                Span::styled(
                    format!("  {}", fmt_fixed_width(&usd_str, 8)),
                    theme::SUGGESTION_STYLE,
                ),
                Span::styled(format!("  · {}", suffix), theme::SUGGESTION_STYLE),
            ];
            ListItem::new(Line::from(spans))
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::BORDER_STYLE)
            .title(Span::styled(
                " Private holdings (viewing key) ",
                theme::PRIVACY_STYLE,
            )),
    );
    f.render_widget(list, area);
}

fn short_addr(felt: &Felt) -> String {
    let s = format!("{:#x}", felt);
    if s.len() <= 10 {
        s
    } else {
        format!("{}…{}", &s[..6], &s[s.len() - 4..])
    }
}

/// USD value of a (token, raw u128 amount) pair using today's price and
/// registry-known decimals. Same semantics as `balance_usd_value` but
/// for inputs that don't have a `TokenBalance` struct (e.g. summed
/// private-note amounts).
fn private_holding_usd(app: &App, token: &Felt, amount: u128) -> Option<f64> {
    let price = app.price_client.as_ref()?.get_today_price(token)?;
    let registry = app.search_engine.as_ref().map(|e| e.registry())?;
    let decimals = registry.get_decimals(token)? as i32;
    let raw = amount as f64;
    let scale = 10f64.powi(decimals);
    Some(raw / scale * price)
}

fn balance_usd_value(app: &App, bal: &TokenBalance) -> Option<f64> {
    let price = app
        .price_client
        .as_ref()?
        .get_today_price(&bal.token_address)?;
    let raw = felt_to_u128(&bal.balance_raw) as f64;
    let scale = 10f64.powi(bal.decimals as i32);
    Some(raw / scale * price)
}

fn draw_events_tab(f: &mut Frame, app: &mut App, area: Rect) {
    // Column headers
    let header_area = Rect { height: 1, ..area };
    let list_area = Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        ..area
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled("    Event               ", theme::SUGGESTION_STYLE),
        Span::styled("Contract         ", theme::SUGGESTION_STYLE),
        Span::styled("Tx             ", theme::SUGGESTION_STYLE),
    ]));
    f.render_widget(header, header_area);

    if app.address.events.items.is_empty() {
        let msg = if app.is_loading {
            " Loading events..."
        } else {
            " No events found"
        };
        f.render_widget(
            Paragraph::new(msg).style(theme::SUGGESTION_STYLE).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::BORDER_STYLE),
            ),
            list_area,
        );
        return;
    }

    let items: Vec<ListItem> = app
        .address
        .events
        .items
        .iter()
        .map(|event| {
            let contract = app.format_address(&event.contract_address);
            let name = event.event_name.as_deref().unwrap_or("?");
            let tx_hash = event.raw.transaction_hash;
            let tx_label = app.resolve_tx(&tx_hash);
            let tx_display = tx_hash_cell(tx_label, &tx_hash);
            let tx_style = if tx_label.is_some() {
                theme::LABEL_STYLE
            } else {
                theme::TX_HASH_STYLE
            };

            let line = Line::from(vec![
                Span::styled(format!(" {:<20}", name), theme::LABEL_STYLE),
                Span::styled(format!("{:<17}", contract), theme::BLOCK_HASH_STYLE),
                Span::styled(tx_display, tx_style),
            ]);
            ListItem::new(line)
        })
        .collect();

    let count = events_count_fragment(app);
    let title = if app.is_loading {
        format!(" Events ({count}) fetching... ")
    } else {
        format!(" Events ({count}) ")
    };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_STYLE)
                .title(Span::styled(title, theme::TITLE_STYLE)),
        )
        .highlight_style(theme::SELECTED_STYLE.add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");

    f.render_stateful_widget(list, list_area, &mut app.address.events.state);
}

fn format_token_balance(bal: &TokenBalance) -> String {
    let raw = felt_to_u128(&bal.balance_raw);
    if raw == 0 {
        return "0".to_string();
    }
    let divisor = 10u128.pow(bal.decimals as u32);
    let whole = raw / divisor;
    let frac = raw % divisor;
    let frac_digits = bal.decimals.min(6) as u32;
    let frac_divisor = 10u128.pow(bal.decimals as u32 - frac_digits);
    let frac_display = frac / frac_divisor;
    format!(
        "{}.{:0>width$}",
        whole,
        frac_display,
        width = frac_digits as usize
    )
}

fn draw_class_history_tab(f: &mut Frame, app: &App, area: Rect) {
    let header_area = Rect { height: 1, ..area };
    let list_area = Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        ..area
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled("   Block          ", theme::SUGGESTION_STYLE),
        Span::styled(
            "Class Hash                                                              ",
            theme::SUGGESTION_STYLE,
        ),
    ]));
    f.render_widget(header, header_area);

    if app.address.class_history.is_empty() {
        let msg = if app.is_loading {
            " Loading class history..."
        } else {
            " No class history available (requires PF service)"
        };
        f.render_widget(
            Paragraph::new(msg).style(theme::SUGGESTION_STYLE).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme::BORDER_STYLE),
            ),
            list_area,
        );
        return;
    }

    let items: Vec<ListItem> = app
        .address
        .class_history
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let hash_display = if entry.class_hash.len() > 66 {
                format!("{}…", &entry.class_hash[..65])
            } else {
                entry.class_hash.clone()
            };
            let is_selected = i == app.address.class_history_scroll;
            let hash_style = if is_selected {
                theme::VISUAL_SELECTED_STYLE
            } else {
                theme::TX_HASH_STYLE
            };
            let marker = if is_selected { "►" } else { " " };
            let line = Line::from(vec![
                Span::styled(
                    marker,
                    if is_selected {
                        theme::VISUAL_SELECTED_STYLE
                    } else {
                        theme::NORMAL_STYLE
                    },
                ),
                Span::styled(
                    format!("#{:<14}", entry.block_number),
                    theme::BLOCK_NUMBER_STYLE,
                ),
                Span::styled(format!("{:<68}", hash_display), hash_style),
            ]);
            ListItem::new(line)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::BORDER_STYLE),
    );
    f.render_widget(list, list_area);
}

#[cfg(test)]
mod count_fragment_tests {
    //! The count-fragment helper is the single source of truth for what the
    //! four address-view tabs render inside `(...)`. These tests pin its
    //! five distinct display outputs so regressions show up here before
    //! they surface on screen.
    use super::{CountBound, count_fragment};

    #[test]
    fn exact_shown_less_than_total_renders_fraction() {
        assert_eq!(count_fragment(7, CountBound::Exact(20)), "7 / 20");
    }

    #[test]
    fn exact_shown_equal_total_renders_bare_count() {
        assert_eq!(count_fragment(20, CountBound::Exact(20)), "20");
    }

    #[test]
    fn exact_shown_greater_than_total_falls_back_to_bare_count() {
        // Stale probe: Dune total lags an active stream. Never render a
        // negative-looking "20 / 7" — fall back to the authoritative shown.
        assert_eq!(count_fragment(20, CountBound::Exact(7)), "20");
    }

    #[test]
    fn lower_bound_without_hint_renders_shown_over_shown_plus() {
        // No hint ⇒ denominator defaults to `shown` itself. We know at
        // least N items exist, so "N / N+" is both truthful and keeps
        // the Shown/Known format across every tab state.
        assert_eq!(
            count_fragment(7, CountBound::LowerBound { hint: None }),
            "7 / 7+"
        );
    }

    #[test]
    fn lower_bound_zero_renders_zero_over_zero_plus() {
        // The cold-state display the user wants on every tab — never a
        // bare "0" or an empty label while probes are still warming up.
        assert_eq!(
            count_fragment(0, CountBound::LowerBound { hint: None }),
            "0 / 0+"
        );
    }

    #[test]
    fn lower_bound_with_hint_above_shown_renders_fraction_plus() {
        assert_eq!(
            count_fragment(7, CountBound::LowerBound { hint: Some(100) }),
            "7 / 100+"
        );
    }

    #[test]
    fn lower_bound_with_hint_at_or_below_shown_falls_back_to_shown_denominator() {
        // Hint is stale or conservative; we still want the Shown/Known
        // format, so fall back to "shown / shown+" rather than a
        // misleading "7 / 3+".
        assert_eq!(
            count_fragment(7, CountBound::LowerBound { hint: Some(3) }),
            "7 / 7+"
        );
        assert_eq!(
            count_fragment(7, CountBound::LowerBound { hint: Some(7) }),
            "7 / 7+"
        );
    }
}
