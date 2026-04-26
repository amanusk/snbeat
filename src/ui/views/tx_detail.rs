use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs, Wrap};
use starknet::core::types::{CallType, EntryPointType, Felt};

use crate::app::App;
use crate::app::state::TxNavItem;
use crate::app::views::tx_detail::TxTab;
use crate::data::types::{ExecutionStatus, SnTransaction};
use crate::decode::calldata::{self, DecodedValue};
use crate::decode::events::{DecodedEvent, DecodedParam};
use crate::decode::functions::RawCall;
use crate::decode::outside_execution;
use crate::decode::trace::DecodedTraceCall;
use crate::ui::theme;
use crate::ui::widgets::address_color::AddressColorMap;
use crate::ui::widgets::hex_display::{format_commas, format_fri, format_strk_u128};
use crate::ui::widgets::{param_display, price, search_bar, status_bar};
use crate::utils::felt_to_u128;

/// Cap of top-level multicall entries shown in the fixed header before
/// collapsing the rest into a "... and N more" line.
const HEADER_CALLS_PREVIEW: usize = 4;

pub fn draw(f: &mut Frame, app: &mut App) {
    let selected: Option<TxNavItem> = if app.tx_detail.visual_mode {
        app.tx_detail
            .nav_items
            .get(app.tx_detail.nav_cursor)
            .cloned()
    } else {
        None
    };

    if app.tx_detail.transaction.is_none() {
        let chunks = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(f.area());
        search_bar::draw_input(f, app, chunks[0]);
        f.render_widget(
            Paragraph::new(" Loading transaction...").style(theme::STATUS_LOADING),
            chunks[1],
        );
        status_bar::draw(f, app, chunks[2]);
        search_bar::draw_dropdown(f, app, chunks[0]);
        return;
    }

    let color_map = build_color_map(app);
    let mut line_map: Vec<Option<u16>> = vec![None; app.tx_detail.nav_items.len()];

    // Build line buffers for header + every tab body up front. Header always
    // renders; bodies are computed for all three tabs every frame so visual-mode
    // tab-switches see up-to-date line offsets for the destination tab — without
    // this, the first `j` after a tab switch would scroll to a stale offset.
    let header_lines = build_header_lines(app, &color_map, selected.as_ref(), &mut line_map);
    let events_lines = build_events_lines(app, &color_map, selected.as_ref(), &mut line_map);
    let calls_lines = build_calls_lines(app, &color_map, selected.as_ref(), &mut line_map);
    let trace_lines = build_trace_lines(app, &color_map, selected.as_ref(), &mut line_map);

    let header_height = (header_lines.len() as u16).saturating_add(2); // borders
    // Header is fixed-content; clamp to ~60% of screen so tab body always
    // gets at least a few rows on small terminals. The tab body Min(5) below
    // works in tandem with this clamp.
    let max_header = (f.area().height.saturating_sub(4) * 6 / 10).max(5);
    let header_height = header_height.min(max_header);

    let chunks = Layout::vertical([
        Constraint::Length(1),             // search bar
        Constraint::Length(header_height), // fixed header
        Constraint::Length(1),             // tabs bar
        Constraint::Min(5),                // tab body (scrollable)
        Constraint::Length(1),             // status bar
    ])
    .split(f.area());

    search_bar::draw_input(f, app, chunks[0]);
    draw_header_panel(f, header_lines, chunks[1]);
    draw_tabs_bar(f, app, chunks[2]);
    draw_active_tab_body(f, app, chunks[3], events_lines, calls_lines, trace_lines);
    status_bar::draw(f, app, chunks[4]);

    app.tx_detail.nav_item_lines = line_map.into_iter().map(|o| o.unwrap_or(0)).collect();
    search_bar::draw_dropdown(f, app, chunks[0]);
}

fn draw_header_panel(f: &mut Frame, lines: Vec<Line<'static>>, area: Rect) {
    let widget = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_FOCUSED_STYLE)
                .title(Span::styled(" Transaction ", theme::TITLE_STYLE)),
        )
        .wrap(Wrap { trim: false });
    f.render_widget(widget, area);
}

fn draw_tabs_bar(f: &mut Frame, app: &App, area: Rect) {
    let events_count = app.tx_detail.decoded_events.len();
    let calls_count = app.tx_detail.decoded_calls.len();
    let trace_count = app
        .tx_detail
        .trace
        .as_ref()
        .map(|t| {
            let mut n = 0usize;
            t.for_each_call(|_| n += 1);
            n
        })
        .unwrap_or(0);
    let trace_label = if app.tx_detail.trace.is_some() {
        format!("Trace ({trace_count})")
    } else if app.tx_detail.trace_loading {
        "Trace (loading…)".to_string()
    } else {
        "Trace".to_string()
    };
    let titles = vec![
        Span::raw(format!(" Events ({events_count}) ")),
        Span::raw(format!(" Calls ({calls_count}) ")),
        Span::raw(format!(" {trace_label} ")),
    ];
    let selected = match app.tx_detail.active_tab {
        TxTab::Events => 0,
        TxTab::Calls => 1,
        TxTab::Trace => 2,
    };
    // Active tab uses a filled background for high contrast — much more
    // visible than the default underline at-a-glance.
    let highlight = ratatui::style::Style::new()
        .fg(ratatui::style::Color::Black)
        .bg(ratatui::style::Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let tabs = Tabs::new(titles)
        .select(selected)
        .style(theme::SUGGESTION_STYLE)
        .highlight_style(highlight)
        .divider(Span::styled("·", theme::BORDER_STYLE))
        .padding("", "");
    f.render_widget(tabs, area);
}

fn draw_active_tab_body(
    f: &mut Frame,
    app: &App,
    area: Rect,
    events_lines: Vec<Line<'static>>,
    calls_lines: Vec<Line<'static>>,
    trace_lines: Vec<Line<'static>>,
) {
    let (lines, scroll, title) = match app.tx_detail.active_tab {
        TxTab::Events => (
            events_lines,
            app.tx_detail.events_scroll,
            " Events (j/k: scroll · v: visual · Tab: next) ",
        ),
        TxTab::Calls => (
            calls_lines,
            app.tx_detail.calls_scroll,
            " Calls (c: raw · d: decode · o: intent · e: expand · Tab: next) ",
        ),
        TxTab::Trace => (
            trace_lines,
            app.tx_detail.trace_scroll,
            " Trace (j/k: scroll · v: visual · e: expand · Tab: next) ",
        ),
    };
    let widget = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_FOCUSED_STYLE)
                .title(Span::styled(title, theme::TITLE_STYLE)),
        )
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    f.render_widget(widget, area);
}

/// Address formatting that honours the `e` (expand_all) toggle.
/// When expand_all is on we always show the full hex; if the address has a
/// label (registry or Voyager-sourced), we append `(label)` after the hex
/// so the tag is still visible — the user gets both pieces of info on one
/// line and can copy the full hex without losing the human-readable name.
fn format_addr_expanded(app: &App, felt: &Felt) -> String {
    let full = format!("{:#x}", felt);
    if let Some(engine) = &app.search_engine
        && let Some(name) = engine.registry().resolve(felt)
    {
        return format!("{full} ({name})");
    }
    if let Some(label) = app.voyager_labels.get(felt)
        && let Some(name) = &label.name
    {
        return format!("{full} ({name})");
    }
    full
}

/// `format_address` with expand-all override.
fn fmt_addr(app: &App, felt: &Felt) -> String {
    if app.tx_detail.expand_all {
        format_addr_expanded(app, felt)
    } else {
        app.format_address(felt)
    }
}

/// `format_address_full` with expand-all override.
fn fmt_addr_full(app: &App, felt: &Felt) -> String {
    if app.tx_detail.expand_all {
        format_addr_expanded(app, felt)
    } else {
        app.format_address_full(felt)
    }
}

/// Record `item`'s first-occurrence line position into `map` (if not already set).
fn record(item: &TxNavItem, cur_line: usize, map: &mut [Option<u16>], nav: &[TxNavItem]) {
    if let Some(idx) = nav.iter().position(|x| x == item) {
        map[idx].get_or_insert(cur_line as u16);
    }
}

/// Build the fixed header lines (tx metadata, status, top-level calls preview, fee).
fn build_header_lines(
    app: &App,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    line_map: &mut [Option<u16>],
) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let tx = match &app.tx_detail.transaction {
        Some(t) => t,
        None => return lines,
    };

    // === TX HEADER ===
    lines.push(Line::from(vec![
        Span::styled(" Hash:   ", theme::NORMAL_STYLE),
        Span::styled(format!("{:#x}", tx.hash()), theme::TX_HASH_STYLE),
    ]));

    // Block + index from receipt
    let (blk_num, blk_hash_str, finality_str) = if let Some(receipt) = &app.tx_detail.receipt {
        let hash_str = receipt
            .block_hash
            .map(|h| format!("{:#x}", h))
            .unwrap_or_default();
        (receipt.block_number, hash_str, receipt.finality.clone())
    } else {
        (tx.block_number(), String::new(), String::new())
    };
    let block_hash_short = if blk_hash_str.len() > 18 {
        format!(
            "{}..{}",
            &blk_hash_str[..8],
            &blk_hash_str[blk_hash_str.len() - 6..]
        )
    } else {
        blk_hash_str
    };
    let blk_style = if matches!(selected, Some(TxNavItem::Block(b)) if *b == blk_num) {
        theme::VISUAL_SELECTED_STYLE
    } else {
        theme::BLOCK_NUMBER_STYLE
    };
    // Block age (when the block timestamp has been fetched).
    let age_suffix = app
        .tx_detail
        .block_timestamp
        .map(|ts| {
            let now = chrono::Utc::now().timestamp() as u64;
            let diff = now.saturating_sub(ts);
            if diff < 60 {
                format!("  ({diff}s ago)")
            } else if diff < 3600 {
                format!("  ({}m ago)", diff / 60)
            } else if diff < 86400 {
                format!("  ({}h ago)", diff / 3600)
            } else {
                format!("  ({}d ago)", diff / 86400)
            }
        })
        .unwrap_or_default();
    record(
        &TxNavItem::Block(blk_num),
        lines.len(),
        line_map,
        &app.tx_detail.nav_items,
    );
    lines.push(Line::from(vec![
        block_marker(blk_num, selected),
        Span::styled("Block:  ", theme::NORMAL_STYLE),
        Span::styled(format!("#{}", blk_num), blk_style),
        Span::styled(format!("  {}", block_hash_short), theme::BLOCK_HASH_STYLE),
        Span::styled(format!("  Idx: {}", tx.index()), theme::NORMAL_STYLE),
        Span::styled(format!("  {}", finality_str), theme::STATUS_OK),
        Span::styled(age_suffix, theme::SUGGESTION_STYLE),
    ]));

    // Type
    let type_style = match tx.type_name() {
        "INVOKE" => theme::TX_TYPE_INVOKE,
        "DECLARE" => theme::TX_TYPE_DECLARE,
        "DEPLOY_ACCOUNT" | "DEPLOY" => theme::TX_TYPE_DEPLOY,
        "L1_HANDLER" => theme::TX_TYPE_L1HANDLER,
        _ => theme::NORMAL_STYLE,
    };
    lines.push(Line::from(vec![
        Span::styled(" Type:   ", theme::NORMAL_STYLE),
        Span::styled(tx.type_name(), type_style),
    ]));

    // META TX indicator for outside executions
    if !app.tx_detail.outside_executions.is_empty() {
        for (_, oe) in &app.tx_detail.outside_executions {
            let intender_style = addr_style(&oe.intender, color_map, selected);
            record(
                &TxNavItem::Address(oe.intender),
                lines.len(),
                line_map,
                &app.tx_detail.nav_items,
            );
            lines.push(Line::from(vec![
                addr_marker(&oe.intender, selected),
                Span::styled("Meta:   ", theme::NORMAL_STYLE),
                Span::styled(format!("META TX ({})", oe.version), theme::META_TX_STYLE),
                Span::styled("  Intender: ", theme::NORMAL_STYLE),
                Span::styled(fmt_addr_full(app, &oe.intender), intender_style),
                Span::styled(format!("  Nonce: {:#x}", oe.nonce), theme::SUGGESTION_STYLE),
            ]));
            lines.push(Line::from(vec![
                Span::raw("        "),
                Span::styled(format!(" {:#x}", oe.intender), intender_style),
            ]));
        }
    }

    // Sender + Nonce — colored with slot 0
    let sender = tx.sender();
    let nonce_str = tx
        .nonce()
        .map(|n| {
            let bytes = n.to_bytes_be();
            format!(
                "{}",
                u64::from_be_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]))
            )
        })
        .unwrap_or_else(|| "N/A".into());
    let sender_style = addr_style(&sender, color_map, selected);
    record(
        &TxNavItem::Address(sender),
        lines.len(),
        line_map,
        &app.tx_detail.nav_items,
    );
    lines.push(Line::from(vec![
        addr_marker(&sender, selected),
        Span::styled("Sender: ", theme::NORMAL_STYLE),
        Span::styled(fmt_addr_full(app, &sender), sender_style),
        Span::styled(format!("  Nonce: {}", nonce_str), theme::NORMAL_STYLE),
    ]));
    lines.push(Line::from(vec![
        Span::raw("        "),
        Span::styled(format!(" {:#x}", sender), sender_style),
    ]));

    // Class hash for Declare txs
    if let SnTransaction::Declare(decl) = tx {
        let ch_item = TxNavItem::ClassHash(decl.class_hash);
        let ch_style = if selected == Some(&ch_item) {
            theme::VISUAL_SELECTED_STYLE
        } else {
            theme::TX_HASH_STYLE
        };
        record(&ch_item, lines.len(), line_map, &app.tx_detail.nav_items);
        let ch_marker = if selected == Some(&ch_item) {
            Span::styled("►", theme::VISUAL_SELECTED_STYLE)
        } else {
            Span::raw(" ")
        };
        lines.push(Line::from(vec![
            ch_marker,
            Span::styled("Class:  ", theme::NORMAL_STYLE),
            Span::styled(format!("{:#x}", decl.class_hash), ch_style),
        ]));
    }

    // Class hash for DeployAccount txs
    if let SnTransaction::DeployAccount(da) = tx {
        let ch_item = TxNavItem::ClassHash(da.class_hash);
        let ch_style = if selected == Some(&ch_item) {
            theme::VISUAL_SELECTED_STYLE
        } else {
            theme::TX_HASH_STYLE
        };
        record(&ch_item, lines.len(), line_map, &app.tx_detail.nav_items);
        let ch_marker = if selected == Some(&ch_item) {
            Span::styled("►", theme::VISUAL_SELECTED_STYLE)
        } else {
            Span::raw(" ")
        };
        lines.push(Line::from(vec![
            ch_marker,
            Span::styled("Class:  ", theme::NORMAL_STYLE),
            Span::styled(format!("{:#x}", da.class_hash), ch_style),
        ]));
    }

    // Execution status
    if let Some(receipt) = &app.tx_detail.receipt {
        let (status_text, style) = match &receipt.execution_status {
            ExecutionStatus::Succeeded => ("SUCCEEDED", theme::STATUS_OK),
            ExecutionStatus::Reverted(_) => ("REVERTED", theme::STATUS_REVERTED),
            ExecutionStatus::Unknown => ("UNKNOWN", theme::STATUS_LOADING),
        };
        lines.push(Line::from(vec![
            Span::styled(" Status: ", theme::NORMAL_STYLE),
            Span::styled(status_text, style),
        ]));
        if let ExecutionStatus::Reverted(reason) = &receipt.execution_status {
            lines.push(Line::from(vec![
                Span::styled(" Revert: ", theme::STATUS_ERROR),
                Span::raw(reason.clone()),
            ]));
        }
    }

    // === CONTRACTS DEPLOYED (via UDC) ===
    let deployed_addrs =
        crate::decode::events::extract_deployed_addresses(&app.tx_detail.decoded_events);
    if !deployed_addrs.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!(" Contracts Deployed ({})", deployed_addrs.len()),
            theme::TITLE_STYLE,
        )));
        for addr in &deployed_addrs {
            let style = addr_style(addr, color_map, selected);
            record(
                &TxNavItem::Address(*addr),
                lines.len(),
                line_map,
                &app.tx_detail.nav_items,
            );
            lines.push(Line::from(vec![
                addr_marker(addr, selected),
                Span::styled("  ", theme::NORMAL_STYLE),
                Span::styled(fmt_addr_full(app, addr), style),
            ]));
            lines.push(Line::from(vec![
                Span::raw("   "),
                Span::styled(format!("{:#x}", addr), style),
            ]));
        }
    }

    // === TOP-LEVEL CALLS PREVIEW (compact) ===
    // Show up to HEADER_CALLS_PREVIEW top-level multicall entries as a quick
    // glance; the full list with c/d/o toggles lives in the Calls tab.
    if !app.tx_detail.decoded_calls.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!(" Calls ({})", app.tx_detail.decoded_calls.len()),
            theme::TITLE_STYLE,
        )));
        let preview_n = app.tx_detail.decoded_calls.len().min(HEADER_CALLS_PREVIEW);
        for (i, call) in app
            .tx_detail
            .decoded_calls
            .iter()
            .take(preview_n)
            .enumerate()
        {
            let display_name = call.function_name.clone().unwrap_or_else(|| {
                let hex = format!("{:#x}", call.selector);
                if hex.len() > 18 {
                    format!("{}…", &hex[..18])
                } else {
                    hex
                }
            });
            let target = fmt_addr(app, &call.contract_address);
            let contract_style = addr_style(&call.contract_address, color_map, selected);
            record(
                &TxNavItem::Address(call.contract_address),
                lines.len(),
                line_map,
                &app.tx_detail.nav_items,
            );
            lines.push(Line::from(vec![
                addr_marker(&call.contract_address, selected),
                Span::styled(format!("  {i}: "), theme::NORMAL_STYLE),
                Span::styled(format!("{:<20}", target), contract_style),
                Span::raw(" → "),
                Span::styled(display_name, theme::TX_HASH_STYLE),
                Span::styled(
                    format!(" ({} args)", call.data.len()),
                    theme::SUGGESTION_STYLE,
                ),
            ]));
        }
        let remaining = app.tx_detail.decoded_calls.len().saturating_sub(preview_n);
        if remaining > 0 {
            lines.push(Line::from(Span::styled(
                format!("    … and {remaining} more (Calls tab)"),
                theme::SUGGESTION_STYLE,
            )));
        }
    }

    // === FEE SECTION ===
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(" Fee Info", theme::TITLE_STYLE)));

    // Block gas prices
    if let Some(block) = &app.block_detail.block {
        lines.push(Line::from(vec![
            Span::styled("   Block Gas:  ", theme::NORMAL_STYLE),
            Span::raw(format!(
                "L1: {}  L2: {}  L1-Data: {}",
                format_fri(block.l1_gas_price_fri),
                format_fri(block.l2_gas_price_fri),
                format_fri(block.l1_data_gas_price_fri),
            )),
        ]));
    }

    // Extract tip
    let tip: u64 = match tx {
        SnTransaction::Invoke(i) => i.tip,
        SnTransaction::Declare(d) => d.tip,
        SnTransaction::DeployAccount(da) => da.tip,
        _ => 0,
    };

    // Actual fee
    if let Some(receipt) = &app.tx_detail.receipt {
        let total_fri = felt_to_u128(&receipt.actual_fee);
        // tip is per-L2-gas (FRI/gas); actual tip paid = tip * l2_gas_used
        let tip_paid_fri = (tip as u128) * (receipt.execution_resources.l2_gas as u128);
        let resource_fee_fri = total_fri.saturating_sub(tip_paid_fri);

        lines.push(Line::from(vec![
            Span::styled("   Total Fee:  ", theme::NORMAL_STYLE),
            Span::styled(format_strk_u128(total_fri), theme::TX_FEE_STYLE),
            Span::styled(
                format!(
                    "  =  tip: {} + resources: {}",
                    format_strk_u128(tip_paid_fri),
                    format_strk_u128(resource_fee_fri)
                ),
                theme::SUGGESTION_STYLE,
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("   Tip:        ", theme::NORMAL_STYLE),
            Span::raw(format_fri(tip as u128)),
            Span::styled("  (Tip/gas)", theme::SUGGESTION_STYLE),
        ]));

        let res = &receipt.execution_resources;
        lines.push(Line::from(vec![
            Span::styled("   Gas Used:   ", theme::NORMAL_STYLE),
            Span::raw(format!(
                "L1: {}  L2: {}  L1-Data: {}",
                format_commas(res.l1_gas),
                format_commas(res.l2_gas),
                format_commas(res.l1_data_gas),
            )),
        ]));
    }

    // Resource bounds
    let rb = match tx {
        SnTransaction::Invoke(i) => i.resource_bounds.as_ref(),
        SnTransaction::Declare(d) => d.resource_bounds.as_ref(),
        SnTransaction::DeployAccount(da) => da.resource_bounds.as_ref(),
        _ => None,
    };
    if let Some(rb) = rb {
        lines.push(Line::from(Span::styled(
            "   Resource Bounds (requested)",
            theme::SUGGESTION_STYLE,
        )));
        lines.push(Line::from(vec![Span::raw(format!(
            "     L1:      max_amount={:<14} max_price={}",
            format_commas(rb.l1_gas_max_amount),
            format_fri(rb.l1_gas_max_price)
        ))]));
        lines.push(Line::from(vec![Span::raw(format!(
            "     L2:      max_amount={:<14} max_price={}",
            format_commas(rb.l2_gas_max_amount),
            format_fri(rb.l2_gas_max_price)
        ))]));
        lines.push(Line::from(vec![Span::raw(format!(
            "     L1-Data: max_amount={:<14} max_price={}",
            format_commas(rb.l1_data_gas_max_amount),
            format_fri(rb.l1_data_gas_max_price)
        ))]));
    }

    lines
}

/// Build the Events tab body: group decoded events by contract and render
/// each event with its decoded params + USD pricing where applicable.
fn build_events_lines(
    app: &App,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    line_map: &mut [Option<u16>],
) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let registry = app.search_engine.as_ref().map(|e| e.registry());

    let groups = crate::decode::events::group_events_by_contract(&app.tx_detail.decoded_events);
    if groups.is_empty() {
        lines.push(Line::from(Span::styled(
            "   (no events)",
            theme::SUGGESTION_STYLE,
        )));
        return lines;
    }
    for (gi, group) in groups.iter().enumerate() {
        let is_last_group = gi == groups.len() - 1;
        let branch = if is_last_group { "└─" } else { "├─" };
        let continuation = if is_last_group { "   " } else { "│  " };

        let contract_label = fmt_addr_full(app, &group.contract_address);
        let contract_style = addr_style(&group.contract_address, color_map, selected);
        record(
            &TxNavItem::Address(group.contract_address),
            lines.len(),
            line_map,
            &app.tx_detail.nav_items,
        );
        lines.push(Line::from(vec![
            addr_marker(&group.contract_address, selected),
            Span::styled(format!("{branch} "), theme::BORDER_STYLE),
            Span::styled(contract_label, contract_style),
            Span::styled(
                format!("  ({} events)", group.events.len()),
                theme::SUGGESTION_STYLE,
            ),
        ]));

        let event_prices =
            price::token_prices(app, &group.contract_address, app.tx_detail.block_timestamp);

        for (ei, event) in group.events.iter().enumerate() {
            let is_last = ei == group.events.len() - 1;
            let eb = if is_last { "└─" } else { "├─" };
            push_event_line(
                event,
                &format!(" {continuation}{eb} "),
                event_prices,
                app,
                color_map,
                registry,
                selected,
                line_map,
                &app.tx_detail.nav_items,
                &mut lines,
            );
        }
    }
    lines
}

/// Build the Calls tab body: full multicall list with c/d/o toggles, plus
/// the Outside Execution Intent expansion when toggled on.
fn build_calls_lines(
    app: &App,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    line_map: &mut [Option<u16>],
) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();

    if app.tx_detail.decoded_calls.is_empty() {
        lines.push(Line::from(Span::styled(
            "   (no calls)",
            theme::SUGGESTION_STYLE,
        )));
        return lines;
    }

    let has_oe = !app.tx_detail.outside_executions.is_empty();
    // `e` is a master switch: when on, it forces decoded calldata and
    // outside-exec intent on regardless of `d`/`o`.
    let effective_decoded = app.tx_detail.show_decoded_calldata || app.tx_detail.expand_all;
    let effective_outside = app.tx_detail.show_outside_execution || app.tx_detail.expand_all;
    let oe_hint = if has_oe {
        if effective_outside {
            " [o: hide intent]"
        } else {
            " [o: intent]"
        }
    } else {
        ""
    };
    let expand_hint = if app.tx_detail.expand_all {
        " [e: collapse]"
    } else {
        " [e: expand]"
    };
    let calldata_hint = if effective_decoded {
        format!(" [d: hide decoded] [c: raw]{oe_hint}{expand_hint}")
    } else if app.tx_detail.show_calldata {
        format!(" [c: hide calldata] [d: decode]{oe_hint}{expand_hint}")
    } else {
        format!(" [c: raw calldata] [d: decode]{oe_hint}{expand_hint}")
    };
    lines.push(Line::from(vec![
        Span::styled(
            format!(" Calls ({})", app.tx_detail.decoded_calls.len()),
            theme::TITLE_STYLE,
        ),
        Span::styled(calldata_hint, theme::SUGGESTION_STYLE),
    ]));

    for (i, call) in app.tx_detail.decoded_calls.iter().enumerate() {
        let display_name = call.function_name.clone().unwrap_or_else(|| {
            let hex = format!("{:#x}", call.selector);
            if !app.tx_detail.expand_all && hex.len() > 18 {
                format!("{}…", &hex[..18])
            } else {
                hex
            }
        });
        let target = fmt_addr(app, &call.contract_address);
        let contract_style = addr_style(&call.contract_address, color_map, selected);
        record(
            &TxNavItem::Address(call.contract_address),
            lines.len(),
            line_map,
            &app.tx_detail.nav_items,
        );
        lines.push(Line::from(vec![
            addr_marker(&call.contract_address, selected),
            Span::styled(format!("  {i}: "), theme::NORMAL_STYLE),
            Span::styled(format!("{:<20}", target), contract_style),
            Span::raw(" → "),
            Span::styled(display_name, theme::TX_HASH_STYLE),
            Span::styled(
                format!(" ({} args)", call.data.len()),
                theme::SUGGESTION_STYLE,
            ),
        ]));
        // Inline annotation for outside execution calls
        if let Some((_, oe)) = app
            .tx_detail
            .outside_executions
            .iter()
            .find(|(idx, _)| *idx == i)
        {
            let caller_str = outside_execution::format_caller(&oe.caller);
            lines.push(Line::from(vec![
                Span::raw("        "),
                Span::styled(
                    format!("Outside Execution ({})", oe.version),
                    theme::META_TX_STYLE,
                ),
                Span::styled(
                    format!(
                        "  nonce: {:#x}  caller: {}  inner calls: {}",
                        oe.nonce,
                        caller_str,
                        oe.inner_calls.len()
                    ),
                    theme::SUGGESTION_STYLE,
                ),
            ]));
        }
        if effective_decoded {
            render_decoded_calldata(call, app, color_map, selected, &mut lines);
        } else if app.tx_detail.show_calldata {
            for (di, felt) in call.data.iter().enumerate() {
                lines.push(Line::from(vec![
                    Span::raw("        "),
                    Span::styled(format!("[{di}] {:#x}", felt), theme::SUGGESTION_STYLE),
                ]));
            }
        }
    }

    // === OUTSIDE EXECUTION INTENT (toggled with `o`, or forced by `e`) ===
    if effective_outside && !app.tx_detail.outside_executions.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled(" Outside Execution Intent", theme::TITLE_STYLE),
            Span::styled(" [o: hide]", theme::SUGGESTION_STYLE),
        ]));
        for (_, oe) in &app.tx_detail.outside_executions {
            let intender_style = addr_style(&oe.intender, color_map, selected);
            let caller_str = outside_execution::format_caller(&oe.caller);
            lines.push(Line::from(vec![
                Span::styled("   Intender: ", theme::NORMAL_STYLE),
                Span::styled(fmt_addr_full(app, &oe.intender), intender_style),
            ]));
            lines.push(Line::from(vec![
                Span::raw("             "),
                Span::styled(format!("{:#x}", oe.intender), intender_style),
            ]));
            lines.push(Line::from(vec![
                Span::styled("   Caller:   ", theme::NORMAL_STYLE),
                Span::raw(caller_str),
            ]));
            lines.push(Line::from(vec![
                Span::styled("   Nonce:    ", theme::NORMAL_STYLE),
                Span::styled(format!("{:#x}", oe.nonce), theme::TX_HASH_STYLE),
            ]));
            lines.push(Line::from(vec![
                Span::styled("   Window:   ", theme::NORMAL_STYLE),
                Span::raw(format!(
                    "after: {}  before: {}",
                    oe.execute_after, oe.execute_before
                )),
            ]));
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("   Inner Calls ({})", oe.inner_calls.len()),
                theme::TITLE_STYLE,
            )));
            for (ci, inner_call) in oe.inner_calls.iter().enumerate() {
                let inner_name = inner_call.function_name.clone().unwrap_or_else(|| {
                    let hex = format!("{:#x}", inner_call.selector);
                    if !app.tx_detail.expand_all && hex.len() > 18 {
                        format!("{}…", &hex[..18])
                    } else {
                        hex
                    }
                });
                let inner_target = fmt_addr(app, &inner_call.contract_address);
                let inner_style = addr_style(&inner_call.contract_address, color_map, selected);
                record(
                    &TxNavItem::Address(inner_call.contract_address),
                    lines.len(),
                    line_map,
                    &app.tx_detail.nav_items,
                );
                lines.push(Line::from(vec![
                    addr_marker(&inner_call.contract_address, selected),
                    Span::styled(format!("    {ci}: "), theme::NORMAL_STYLE),
                    Span::styled(format!("{:<20}", inner_target), inner_style),
                    Span::raw(" → "),
                    Span::styled(inner_name, theme::TX_HASH_STYLE),
                    Span::styled(
                        format!(" ({} args)", inner_call.data.len()),
                        theme::SUGGESTION_STYLE,
                    ),
                ]));
                if effective_decoded {
                    render_decoded_calldata(inner_call, app, color_map, selected, &mut lines);
                } else if app.tx_detail.show_calldata {
                    for (di, felt) in inner_call.data.iter().enumerate() {
                        lines.push(Line::from(vec![
                            Span::raw("          "),
                            Span::styled(format!("[{di}] {:#x}", felt), theme::SUGGESTION_STYLE),
                        ]));
                    }
                }
            }
        }
    }

    lines
}

/// Build the Trace tab body: recursive call tree with ABI-decoded function
/// names, decoded params (incl. token amounts + USD), per-node events, and
/// raw result felts.
fn build_trace_lines(
    app: &App,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    line_map: &mut [Option<u16>],
) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let registry = app.search_engine.as_ref().map(|e| e.registry());

    let trace = match &app.tx_detail.trace {
        Some(t) => t,
        None => {
            let msg = if app.tx_detail.trace_loading {
                "   (trace loading…)"
            } else {
                "   (trace unavailable)"
            };
            lines.push(Line::from(Span::styled(msg, theme::SUGGESTION_STYLE)));
            return lines;
        }
    };
    if let Some(reason) = &trace.revert_reason {
        lines.push(Line::from(vec![
            Span::styled(" Revert: ", theme::STATUS_ERROR),
            Span::raw(reason.clone()),
        ]));
        lines.push(Line::from(""));
    }

    let roots = trace.roots();
    if roots.is_empty() {
        lines.push(Line::from(Span::styled(
            "   (no invocations)",
            theme::SUGGESTION_STYLE,
        )));
        return lines;
    }
    for (label, root) in roots {
        lines.push(Line::from(Span::styled(
            format!(" {label}"),
            theme::TITLE_STYLE,
        )));
        render_trace_call(
            root,
            "",
            true,
            app,
            color_map,
            registry,
            selected,
            line_map,
            &app.tx_detail.nav_items,
            &mut lines,
        );
        lines.push(Line::from(""));
    }
    lines
}

/// Render a single trace node and its descendants.
#[allow(clippy::too_many_arguments)]
fn render_trace_call(
    call: &DecodedTraceCall,
    prefix: &str,
    is_last: bool,
    app: &App,
    color_map: &AddressColorMap,
    registry: Option<&crate::registry::AddressRegistry>,
    selected: Option<&TxNavItem>,
    line_map: &mut [Option<u16>],
    nav_items: &[TxNavItem],
    lines: &mut Vec<Line<'static>>,
) {
    let branch = if is_last { "└─" } else { "├─" };
    let next_prefix = format!("{prefix}{}", if is_last { "   " } else { "│  " });
    // Body lines (fn / → / events) get a leading space so they line up under
    // the header's content column — the header reserves column 1 for the
    // visual-mode marker (`►` or space), and body lines match that offset.
    let body_prefix = format!(" {next_prefix}");

    // Header line: branch + contract label + optional kind tag (only when
    // non-default; the default CALL/EXTERNAL combo is just visual noise).
    let label = fmt_addr_full(app, &call.contract_address);
    let style = addr_style(&call.contract_address, color_map, selected);
    record(
        &TxNavItem::Address(call.contract_address),
        lines.len(),
        line_map,
        nav_items,
    );
    let mut spans: Vec<Span<'static>> = vec![
        addr_marker(&call.contract_address, selected),
        Span::styled(format!("{prefix}{branch} "), theme::BORDER_STYLE),
        Span::styled(label, style),
    ];
    if let Some(kind) = call_kind_tag(call.call_type, call.entry_point_type) {
        spans.push(Span::raw(" "));
        spans.push(Span::styled(kind, theme::SUGGESTION_STYLE));
    }
    if call.is_reverted {
        spans.push(Span::styled("  REVERTED", theme::STATUS_REVERTED));
    }
    lines.push(Line::from(spans));

    // Function call line: fn_name(decoded args)
    let fn_label = call.function_name.clone().unwrap_or_else(|| {
        let hex = format!("{:#x}", call.entry_point_selector);
        if !app.tx_detail.expand_all && hex.len() > 18 {
            format!("{}…", &hex[..18])
        } else {
            hex
        }
    });
    let mut fn_spans: Vec<Span<'static>> = vec![
        Span::styled(format!("{body_prefix}fn "), theme::BORDER_STYLE),
        Span::styled(fn_label, theme::TX_HASH_STYLE),
    ];
    if let (Some(func_def), Some(abi)) = (&call.function_def, &call.contract_abi) {
        let decoded = calldata::decode_calldata(&call.calldata, &func_def.inputs, abi);
        let prices =
            price::token_prices(app, &call.contract_address, app.tx_detail.block_timestamp);
        fn_spans.push(Span::raw("("));
        for (pi, p) in decoded.iter().enumerate() {
            if pi > 0 {
                fn_spans.push(Span::raw(", "));
            }
            if let Some(name) = &p.name {
                fn_spans.push(Span::styled(format!("{name}: "), theme::SUGGESTION_STYLE));
            }
            render_value_spans(&p.value, app, color_map, selected, &mut fn_spans);
            // USD pair if param matches a tracked token's u256 amount.
            if let Some((amount, _)) =
                decoded_value_token_amount(&p.value, &call.contract_address, registry)
                && (prices.0.is_some() || prices.1.is_some())
            {
                fn_spans.push(Span::styled(
                    format_usd_pair(amount, prices.0, prices.1),
                    theme::SUGGESTION_STYLE,
                ));
            }
        }
        fn_spans.push(Span::raw(")"));
    } else {
        fn_spans.push(Span::styled(
            format!("({} felts)", call.calldata.len()),
            theme::SUGGESTION_STYLE,
        ));
    }
    lines.push(Line::from(fn_spans));

    // Result line(s): raw felts (return-value decoding is a follow-up).
    if !call.result.is_empty() {
        let limit = if app.tx_detail.expand_all {
            call.result.len()
        } else {
            4
        };
        let preview: Vec<String> = call
            .result
            .iter()
            .take(limit)
            .map(|f| format!("{:#x}", f))
            .collect();
        let extra = call.result.len().saturating_sub(preview.len());
        let suffix = if extra > 0 {
            format!(", … +{extra}")
        } else {
            String::new()
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{body_prefix}→ "), theme::BORDER_STYLE),
            Span::styled(
                format!("[{}]{suffix}", preview.join(", ")),
                theme::SUGGESTION_STYLE,
            ),
        ]));
    }

    // Events + inner calls share the same child-list under this node, so
    // their tree branches share a column. Treat them as one combined list
    // when picking ├─ vs └─ so only the very last entry gets └─.
    let prices = price::token_prices(app, &call.contract_address, app.tx_detail.block_timestamp);
    let total_children = call.events.len() + call.inner.len();
    let mut child_idx = 0usize;
    for event in call.events.iter() {
        let is_last_child = child_idx == total_children - 1;
        let eb = if is_last_child { "└─" } else { "├─" };
        push_event_line(
            event,
            &format!("{body_prefix}{eb} "),
            prices,
            app,
            color_map,
            registry,
            selected,
            line_map,
            nav_items,
            lines,
        );
        child_idx += 1;
    }

    for child in call.inner.iter() {
        let is_last_child = child_idx == total_children - 1;
        render_trace_call(
            child,
            &next_prefix,
            is_last_child,
            app,
            color_map,
            registry,
            selected,
            line_map,
            nav_items,
            lines,
        );
        child_idx += 1;
    }
}

#[allow(clippy::too_many_arguments)]
fn push_event_line(
    event: &DecodedEvent,
    prefix: &str,
    prices: (Option<f64>, Option<f64>),
    app: &App,
    color_map: &AddressColorMap,
    registry: Option<&crate::registry::AddressRegistry>,
    selected: Option<&TxNavItem>,
    line_map: &mut [Option<u16>],
    nav_items: &[TxNavItem],
    lines: &mut Vec<Line<'static>>,
) {
    let name = event.event_name.as_deref().unwrap_or("Unknown");
    let all_params: Vec<&DecodedParam> = event
        .decoded_keys
        .iter()
        .chain(event.decoded_data.iter())
        .collect();

    let mut spans: Vec<Span<'static>> = vec![Span::styled(prefix.to_string(), theme::BORDER_STYLE)];

    if all_params.is_empty() {
        spans.push(Span::raw(name.to_string()));
    } else {
        spans.push(Span::raw(format!("{name}(")));
        for (pi, p) in all_params.iter().enumerate() {
            if p.type_name
                .as_deref()
                .unwrap_or("")
                .contains("ContractAddress")
            {
                record(
                    &TxNavItem::Address(p.value),
                    lines.len(),
                    line_map,
                    nav_items,
                );
            }
            let mut param_spans = param_display::format_param_styled(
                p,
                &event.contract_address,
                registry,
                color_map,
                selected,
                &|a| fmt_addr(app, a),
                app.tx_detail.expand_all,
            );
            spans.append(&mut param_spans);
            let (today, historic) = prices;
            if (today.is_some() || historic.is_some())
                && let Some((amount, _)) =
                    price::token_amount_from_param(p, &event.contract_address, registry)
            {
                spans.push(Span::styled(
                    format_usd_pair(amount, today, historic),
                    theme::SUGGESTION_STYLE,
                ));
            }
            if pi < all_params.len() - 1 {
                spans.push(Span::raw(", "));
            }
        }
        spans.push(Span::raw(")"));
    }

    lines.push(Line::from(spans));
}

/// Compact tag like "(LIBRARY_CALL)", "(CONSTRUCTOR)", or "(L1_HANDLER)".
/// Returns None for the common case `(CALL, EXTERNAL)` so the trace stays
/// uncluttered — that's the default for ~every node and adds no signal.
fn call_kind_tag(c: CallType, t: EntryPointType) -> Option<String> {
    match (c, t) {
        (CallType::Call, EntryPointType::External) => None,
        (CallType::Call, EntryPointType::L1Handler) => Some("(L1_HANDLER)".into()),
        (CallType::Call, EntryPointType::Constructor) => Some("(CONSTRUCTOR)".into()),
        (CallType::LibraryCall, EntryPointType::External) => Some("(LIBRARY_CALL)".into()),
        (CallType::LibraryCall, t) => Some(format!("(LIBRARY_CALL {})", entry_type_str(t))),
        (CallType::Delegate, t) => Some(format!("(DELEGATE {})", entry_type_str(t))),
    }
}

fn entry_type_str(t: EntryPointType) -> &'static str {
    match t {
        EntryPointType::External => "EXTERNAL",
        EntryPointType::L1Handler => "L1_HANDLER",
        EntryPointType::Constructor => "CONSTRUCTOR",
    }
}

/// If `value` is a u256 amount on a tracked token, return `(amount_f64, decimals)`.
/// Mirrors `price::token_amount_from_param` but accepts a `DecodedValue` so the
/// trace tab can use the same USD-pair formatting as events without needing to
/// re-shape the trace's calldata as `DecodedParam`.
fn decoded_value_token_amount(
    value: &DecodedValue,
    contract_address: &Felt,
    registry: Option<&crate::registry::AddressRegistry>,
) -> Option<(f64, u8)> {
    // Re-pack a u256 DecodedValue into the (low, Some(high)) shape that the
    // existing helper consumes (which expects Felt-encoded halves).
    let (low, high) = match value {
        DecodedValue::U256 { low, high } => (*low, *high),
        _ => return None,
    };
    let synth = DecodedParam {
        name: None,
        type_name: Some("u256".into()),
        value: Felt::from(low),
        value_high: Some(Felt::from(high)),
    };
    price::token_amount_from_param(&synth, contract_address, registry)
}

/// Returns the style to use for an address span, applying visual-mode highlight when selected.
fn addr_style(
    addr: &Felt,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
) -> ratatui::style::Style {
    if matches!(selected, Some(TxNavItem::Address(a)) if a == addr) {
        theme::VISUAL_SELECTED_STYLE
    } else {
        color_map.style_for(addr)
    }
}

/// Returns a `►` marker span when this address is the selected item, otherwise a space.
fn addr_marker(addr: &Felt, selected: Option<&TxNavItem>) -> Span<'static> {
    if matches!(selected, Some(TxNavItem::Address(a)) if a == addr) {
        Span::styled("►", theme::VISUAL_SELECTED_STYLE)
    } else {
        Span::raw(" ")
    }
}

/// Returns a `►` marker span when this block number is the selected item, otherwise a space.
fn block_marker(n: u64, selected: Option<&TxNavItem>) -> Span<'static> {
    if matches!(selected, Some(TxNavItem::Block(b)) if *b == n) {
        Span::styled("►", theme::VISUAL_SELECTED_STYLE)
    } else {
        Span::raw(" ")
    }
}

/// Build the address color map for the current tx view.
/// Sender is registered first (slot 0), then call contracts, then event contracts,
/// then ContractAddress-typed params — so the same address always gets the same color.
fn build_color_map(app: &App) -> AddressColorMap {
    let mut cm = AddressColorMap::new();

    if let Some(tx) = &app.tx_detail.transaction {
        cm.register(tx.sender());
    }

    // Deployed addresses (via UDC) get their own color slots
    for addr in crate::decode::events::extract_deployed_addresses(&app.tx_detail.decoded_events) {
        cm.register(addr);
    }

    for call in &app.tx_detail.decoded_calls {
        cm.register(call.contract_address);
    }

    // Outside execution intender and inner call addresses
    for (_, oe) in &app.tx_detail.outside_executions {
        cm.register(oe.intender);
        for inner in &oe.inner_calls {
            cm.register(inner.contract_address);
        }
    }

    for event in &app.tx_detail.decoded_events {
        cm.register(event.contract_address);
    }

    for event in &app.tx_detail.decoded_events {
        for p in event.decoded_keys.iter().chain(event.decoded_data.iter()) {
            if p.type_name
                .as_deref()
                .unwrap_or("")
                .contains("ContractAddress")
            {
                cm.register(p.value);
            }
        }
    }

    cm
}

/// Render ABI-decoded calldata for a single call.
fn render_decoded_calldata(
    call: &RawCall,
    app: &App,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    lines: &mut Vec<Line<'static>>,
) {
    let (func_def, abi) = match (&call.function_def, &call.contract_abi) {
        (Some(fd), Some(abi)) => (fd, abi),
        _ => {
            // No ABI available — fall back to raw felts with hint
            lines.push(Line::from(vec![
                Span::raw("        "),
                Span::styled("(no ABI — showing raw felts)", theme::SUGGESTION_STYLE),
            ]));
            for (di, felt) in call.data.iter().enumerate() {
                lines.push(Line::from(vec![
                    Span::raw("        "),
                    Span::styled(format!("[{di}] {:#x}", felt), theme::SUGGESTION_STYLE),
                ]));
            }
            return;
        }
    };

    let decoded = calldata::decode_calldata(&call.data, &func_def.inputs, abi);

    for param in &decoded {
        let name_str = param.name.as_deref().unwrap_or("?");
        let type_str = param
            .type_name
            .as_deref()
            .map(short_type_name)
            .unwrap_or_default();

        let mut spans: Vec<Span<'static>> = vec![Span::raw("        ")];

        // Parameter name
        spans.push(Span::styled(format!("{name_str}: "), theme::NORMAL_STYLE));

        // Value — use styled rendering for addresses
        render_value_spans(&param.value, app, color_map, selected, &mut spans);

        // Type annotation
        if !type_str.is_empty() {
            spans.push(Span::styled(
                format!("  ({type_str})"),
                theme::SUGGESTION_STYLE,
            ));
        }

        lines.push(Line::from(spans));

        // For structs, arrays, and enums with children — render nested content
        render_nested_value(&param.value, app, color_map, selected, lines, 3);
    }
}

/// Render a DecodedValue into styled spans (inline, single-line).
fn render_value_spans(
    value: &DecodedValue,
    app: &App,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    spans: &mut Vec<Span<'static>>,
) {
    let expand = app.tx_detail.expand_all;
    match value {
        DecodedValue::Address(felt) => {
            let label = fmt_addr(app, felt);
            let style = if matches!(selected, Some(TxNavItem::Address(a)) if *a == *felt) {
                theme::VISUAL_SELECTED_STYLE
            } else {
                color_map.style_for(felt)
            };
            spans.push(Span::styled(label, style));
        }
        DecodedValue::String(s) => {
            let display = if !expand && s.len() > 60 {
                format!("\"{}...\"", &s[..57])
            } else {
                format!("\"{s}\"")
            };
            spans.push(Span::styled(display, theme::TX_HASH_STYLE));
        }
        DecodedValue::Bool(b) => {
            spans.push(Span::styled(b.to_string(), theme::TX_HASH_STYLE));
        }
        DecodedValue::Struct { name, fields } => {
            let short = name.rsplit("::").next().unwrap_or(name);
            if expand {
                spans.push(Span::styled(format!("{short} {{ "), theme::TX_HASH_STYLE));
                for (i, (fname, fval)) in fields.iter().enumerate() {
                    if i > 0 {
                        spans.push(Span::raw(", "));
                    }
                    spans.push(Span::styled(format!("{fname}: "), theme::SUGGESTION_STYLE));
                    render_value_spans(fval, app, color_map, selected, spans);
                }
                spans.push(Span::styled(" }", theme::TX_HASH_STYLE));
            } else {
                spans.push(Span::styled(
                    format!("{short} {{ {} fields }}", fields.len()),
                    theme::TX_HASH_STYLE,
                ));
            }
        }
        DecodedValue::Enum {
            name,
            variant,
            value: inner,
        } => {
            let short = name.rsplit("::").next().unwrap_or(name);
            if expand {
                spans.push(Span::styled(
                    format!("{short}::{variant}"),
                    theme::TX_HASH_STYLE,
                ));
                if let Some(inner) = inner {
                    spans.push(Span::raw("("));
                    render_value_spans(inner, app, color_map, selected, spans);
                    spans.push(Span::raw(")"));
                }
            } else {
                let suffix = if inner.is_some() { "(...)" } else { "" };
                spans.push(Span::styled(
                    format!("{short}::{variant}{suffix}"),
                    theme::TX_HASH_STYLE,
                ));
            }
        }
        DecodedValue::Array(items) => {
            if expand {
                spans.push(Span::styled("[", theme::TX_HASH_STYLE));
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        spans.push(Span::raw(", "));
                    }
                    render_value_spans(item, app, color_map, selected, spans);
                }
                spans.push(Span::styled("]", theme::TX_HASH_STYLE));
            } else {
                spans.push(Span::styled(
                    format!("[{} items]", items.len()),
                    theme::TX_HASH_STYLE,
                ));
            }
        }
        DecodedValue::Tuple(items) => {
            if expand {
                spans.push(Span::styled("(", theme::TX_HASH_STYLE));
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        spans.push(Span::raw(", "));
                    }
                    render_value_spans(item, app, color_map, selected, spans);
                }
                spans.push(Span::styled(")", theme::TX_HASH_STYLE));
            } else {
                spans.push(Span::styled(
                    format!("({} items)", items.len()),
                    theme::TX_HASH_STYLE,
                ));
            }
        }
        // Simple values: use Display
        other => {
            spans.push(Span::styled(other.to_string(), theme::TX_HASH_STYLE));
        }
    }
}

/// Render nested children (struct fields, array elements, enum payload) on separate lines.
fn render_nested_value(
    value: &DecodedValue,
    app: &App,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    lines: &mut Vec<Line<'static>>,
    depth: usize,
) {
    if depth > 6 {
        return; // Prevent excessive nesting
    }
    let indent: String = " ".repeat(4 + depth * 2);

    match value {
        DecodedValue::Struct { fields, .. } => {
            for (fname, fval) in fields {
                let mut spans = vec![Span::raw(indent.clone())];
                spans.push(Span::styled(format!("{fname}: "), theme::NORMAL_STYLE));
                render_value_spans(fval, app, color_map, selected, &mut spans);
                lines.push(Line::from(spans));
                render_nested_value(fval, app, color_map, selected, lines, depth + 1);
            }
        }
        DecodedValue::Array(items) => {
            let display_count = items.len().min(20);
            for (i, item) in items.iter().enumerate().take(display_count) {
                let mut spans = vec![Span::raw(indent.clone())];
                spans.push(Span::styled(format!("[{i}] "), theme::SUGGESTION_STYLE));
                render_value_spans(item, app, color_map, selected, &mut spans);
                lines.push(Line::from(spans));
                render_nested_value(item, app, color_map, selected, lines, depth + 1);
            }
            if items.len() > display_count {
                lines.push(Line::from(vec![
                    Span::raw(indent),
                    Span::styled(
                        format!("... +{} more", items.len() - display_count),
                        theme::SUGGESTION_STYLE,
                    ),
                ]));
            }
        }
        DecodedValue::Enum {
            value: Some(inner), ..
        } => {
            render_nested_value(inner, app, color_map, selected, lines, depth);
        }
        DecodedValue::Tuple(items) => {
            for (i, item) in items.iter().enumerate() {
                let mut spans = vec![Span::raw(indent.clone())];
                spans.push(Span::styled(format!(".{i}: "), theme::SUGGESTION_STYLE));
                render_value_spans(item, app, color_map, selected, &mut spans);
                lines.push(Line::from(spans));
                render_nested_value(item, app, color_map, selected, lines, depth + 1);
            }
        }
        _ => {} // Leaf values — nothing to nest
    }
}

fn format_usd_pair(amount: f64, today: Option<f64>, historic: Option<f64>) -> String {
    let today_str = today.map(|p| price::format_usd(amount * p));
    let historic_str = historic.map(|p| price::format_usd(amount * p));
    match (today_str, historic_str) {
        (Some(t), Some(h)) => format!(" [{t} ({h})]"),
        (Some(t), None) => format!(" [{t}]"),
        (None, Some(h)) => format!(" [({h})]"),
        (None, None) => String::new(),
    }
}

/// Extract a short type name from a fully-qualified Cairo type.
fn short_type_name(full: &str) -> String {
    // Handle generics: "core::array::Array::<core::felt252>" → "Array<felt252>"
    if let Some(lt) = full.find('<') {
        let base = full[..lt].rsplit("::").next().unwrap_or(&full[..lt]);
        let inner = &full[lt + 1..full.len().saturating_sub(1)];
        let inner_short = short_type_name(inner);
        format!("{base}<{inner_short}>")
    } else {
        full.rsplit("::").next().unwrap_or(full).to_string()
    }
}
