use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use starknet::core::types::Felt;

use crate::app::App;
use crate::app::state::TxNavItem;
use crate::data::types::{ExecutionStatus, SnTransaction};
use crate::decode::calldata::{self, DecodedValue};
use crate::decode::events::DecodedParam;
use crate::decode::functions::RawCall;
use crate::decode::outside_execution;
use crate::ui::theme;
use crate::ui::widgets::address_color::AddressColorMap;
use crate::ui::widgets::hex_display::{format_commas, format_fri, format_strk_u128};
use crate::ui::widgets::{param_display, search_bar, status_bar};
use crate::utils::felt_to_u128;

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1), // search bar
        Constraint::Min(3),    // scrollable tx detail + events
        Constraint::Length(1), // status bar
    ])
    .split(f.area());

    let selected: Option<TxNavItem> = if app.tx_detail.visual_mode {
        app.tx_detail
            .nav_items
            .get(app.tx_detail.nav_cursor)
            .cloned()
    } else {
        None
    };
    search_bar::draw_input(f, app, chunks[0]);
    let nav_line_map = draw_scrollable_detail(f, app, chunks[1], selected.as_ref());
    app.tx_detail.nav_item_lines = nav_line_map;
    status_bar::draw(f, app, chunks[2]);

    search_bar::draw_dropdown(f, app, chunks[0]);
}

/// Returns the first-line index for each nav item in `app.tx_detail.nav_items` (same order).
fn draw_scrollable_detail(
    f: &mut Frame,
    app: &App,
    area: ratatui::layout::Rect,
    selected: Option<&TxNavItem>,
) -> Vec<u16> {
    let tx = match &app.tx_detail.transaction {
        Some(t) => t,
        None => {
            f.render_widget(
                Paragraph::new(" Loading transaction...").style(theme::STATUS_LOADING),
                area,
            );
            return vec![];
        }
    };

    // Build the address color map for this tx view.
    // Registration order determines slot (= color). Sender is always slot 0.
    let color_map = build_color_map(app);
    let registry = app.search_engine.as_ref().map(|e| e.registry());

    let mut lines: Vec<Line> = Vec::new();
    // Tracks the first line index where each TxNavItem appears (same order as app.tx_detail.nav_items).
    let mut line_map: Vec<Option<u16>> = vec![None; app.tx_detail.nav_items.len()];

    // Record the current line count as the first occurrence of `item` (if not already recorded).
    let record =
        |item: &TxNavItem, lines: &Vec<Line>, map: &mut Vec<Option<u16>>, nav: &[TxNavItem]| {
            if let Some(idx) = nav.iter().position(|x| x == item) {
                map[idx].get_or_insert(lines.len() as u16);
            }
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
    record(
        &TxNavItem::Block(blk_num),
        &lines,
        &mut line_map,
        &app.tx_detail.nav_items,
    );
    lines.push(Line::from(vec![
        block_marker(blk_num, selected),
        Span::styled("Block:  ", theme::NORMAL_STYLE),
        Span::styled(format!("#{}", blk_num), blk_style),
        Span::styled(format!("  {}", block_hash_short), theme::BLOCK_HASH_STYLE),
        Span::styled(format!("  Idx: {}", tx.index()), theme::NORMAL_STYLE),
        Span::styled(format!("  {}", finality_str), theme::STATUS_OK),
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
            let intender_style = addr_style(&oe.intender, &color_map, selected);
            record(
                &TxNavItem::Address(oe.intender),
                &lines,
                &mut line_map,
                &app.tx_detail.nav_items,
            );
            lines.push(Line::from(vec![
                addr_marker(&oe.intender, selected),
                Span::styled("Meta:   ", theme::NORMAL_STYLE),
                Span::styled(format!("META TX ({})", oe.version), theme::META_TX_STYLE),
                Span::styled("  Intender: ", theme::NORMAL_STYLE),
                Span::styled(app.format_address_full(&oe.intender), intender_style),
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
    let sender_style = addr_style(&sender, &color_map, selected);
    record(
        &TxNavItem::Address(sender),
        &lines,
        &mut line_map,
        &app.tx_detail.nav_items,
    );
    lines.push(Line::from(vec![
        addr_marker(&sender, selected),
        Span::styled("Sender: ", theme::NORMAL_STYLE),
        Span::styled(app.format_address_full(&sender), sender_style),
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
        record(&ch_item, &lines, &mut line_map, &app.tx_detail.nav_items);
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
        record(&ch_item, &lines, &mut line_map, &app.tx_detail.nav_items);
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
            let style = addr_style(addr, &color_map, selected);
            record(
                &TxNavItem::Address(*addr),
                &lines,
                &mut line_map,
                &app.tx_detail.nav_items,
            );
            lines.push(Line::from(vec![
                addr_marker(addr, selected),
                Span::styled("  ", theme::NORMAL_STYLE),
                Span::styled(app.format_address_full(addr), style),
            ]));
            lines.push(Line::from(vec![
                Span::raw("   "),
                Span::styled(format!("{:#x}", addr), style),
            ]));
        }
    }

    // === DECODED CALLS ===
    if !app.tx_detail.decoded_calls.is_empty() {
        lines.push(Line::from(""));
        let has_oe = !app.tx_detail.outside_executions.is_empty();
        let oe_hint = if has_oe {
            if app.tx_detail.show_outside_execution {
                " [o: hide intent]"
            } else {
                " [o: intent]"
            }
        } else {
            ""
        };
        let calldata_hint = if app.tx_detail.show_decoded_calldata {
            format!(" [d: hide decoded] [c: raw]{oe_hint}")
        } else if app.tx_detail.show_calldata {
            format!(" [c: hide calldata] [d: decode]{oe_hint}")
        } else {
            format!(" [c: raw calldata] [d: decode]{oe_hint}")
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
                if hex.len() > 18 {
                    format!("{}…", &hex[..18])
                } else {
                    hex
                }
            });
            let target = app.format_address(&call.contract_address);
            let contract_style = addr_style(&call.contract_address, &color_map, selected);
            record(
                &TxNavItem::Address(call.contract_address),
                &lines,
                &mut line_map,
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
            if app.tx_detail.show_decoded_calldata {
                render_decoded_calldata(call, app, &color_map, selected, &mut lines);
            } else if app.tx_detail.show_calldata {
                for (di, felt) in call.data.iter().enumerate() {
                    lines.push(Line::from(vec![
                        Span::raw("        "),
                        Span::styled(format!("[{di}] {:#x}", felt), theme::SUGGESTION_STYLE),
                    ]));
                }
            }
        }
    }

    // === OUTSIDE EXECUTION INTENT (toggled with `o`) ===
    if app.tx_detail.show_outside_execution && !app.tx_detail.outside_executions.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled(" Outside Execution Intent", theme::TITLE_STYLE),
            Span::styled(" [o: hide]", theme::SUGGESTION_STYLE),
        ]));
        for (_, oe) in &app.tx_detail.outside_executions {
            let intender_style = addr_style(&oe.intender, &color_map, selected);
            let caller_str = outside_execution::format_caller(&oe.caller);
            lines.push(Line::from(vec![
                Span::styled("   Intender: ", theme::NORMAL_STYLE),
                Span::styled(app.format_address_full(&oe.intender), intender_style),
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
                    if hex.len() > 18 {
                        format!("{}…", &hex[..18])
                    } else {
                        hex
                    }
                });
                let inner_target = app.format_address(&inner_call.contract_address);
                let inner_style = addr_style(&inner_call.contract_address, &color_map, selected);
                record(
                    &TxNavItem::Address(inner_call.contract_address),
                    &lines,
                    &mut line_map,
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
                if app.tx_detail.show_decoded_calldata {
                    render_decoded_calldata(inner_call, app, &color_map, selected, &mut lines);
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

    // === EVENTS ===
    lines.push(Line::from(""));
    let event_count = app.tx_detail.decoded_events.len();
    lines.push(Line::from(Span::styled(
        format!(" Events ({event_count})"),
        theme::TITLE_STYLE,
    )));

    // Group events by contract
    let groups = crate::decode::events::group_events_by_contract(&app.tx_detail.decoded_events);
    for (gi, group) in groups.iter().enumerate() {
        let is_last_group = gi == groups.len() - 1;
        let branch = if is_last_group { "└─" } else { "├─" };
        let continuation = if is_last_group { "   " } else { "│  " };

        let contract_label = app.format_address_full(&group.contract_address);
        let contract_style = addr_style(&group.contract_address, &color_map, selected);
        record(
            &TxNavItem::Address(group.contract_address),
            &lines,
            &mut line_map,
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

        for (ei, event) in group.events.iter().enumerate() {
            let is_last = ei == group.events.len() - 1;
            let eb = if is_last { "└─" } else { "├─" };
            let name = event.event_name.as_deref().unwrap_or("Unknown");

            let all_params: Vec<&DecodedParam> = event
                .decoded_keys
                .iter()
                .chain(event.decoded_data.iter())
                .collect();

            let mut event_spans: Vec<Span<'static>> = vec![Span::styled(
                format!(" {continuation}{eb} "),
                theme::BORDER_STYLE,
            )];

            if all_params.is_empty() {
                event_spans.push(Span::raw(name.to_string()));
            } else {
                event_spans.push(Span::raw(format!("{name}(")));
                for (pi, p) in all_params.iter().enumerate() {
                    // Record address-typed params before the event line is pushed.
                    if p.type_name
                        .as_deref()
                        .unwrap_or("")
                        .contains("ContractAddress")
                    {
                        record(
                            &TxNavItem::Address(p.value),
                            &lines,
                            &mut line_map,
                            &app.tx_detail.nav_items,
                        );
                    }
                    let mut param_spans = param_display::format_param_styled(
                        p,
                        &event.contract_address,
                        registry,
                        &color_map,
                        selected,
                        &|a| app.format_address(a),
                    );
                    event_spans.append(&mut param_spans);
                    if pi < all_params.len() - 1 {
                        event_spans.push(Span::raw(", "));
                    }
                }
                event_spans.push(Span::raw(")"));
            }

            lines.push(Line::from(event_spans));
        }
    }

    if app.tx_detail.decoded_events.is_empty() {
        lines.push(Line::from(Span::styled(
            "   (no events)",
            theme::SUGGESTION_STYLE,
        )));
    }

    // Render as scrollable paragraph
    let title = if app.tx_detail.visual_mode {
        " Transaction Detail [VISUAL] (j/k: cycle · Enter: open · Esc: exit) "
    } else {
        " Transaction Detail (j/k: scroll · v: visual · c: calldata) "
    };
    let widget = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_FOCUSED_STYLE)
                .title(Span::styled(title, theme::TITLE_STYLE)),
        )
        .wrap(Wrap { trim: false })
        .scroll((app.tx_detail.scroll, 0));

    f.render_widget(widget, area);

    line_map.into_iter().map(|o| o.unwrap_or(0)).collect()
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
            .map(|t| short_type_name(t))
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
    match value {
        DecodedValue::Address(felt) => {
            let label = app.format_address(felt);
            let style = if matches!(selected, Some(TxNavItem::Address(a)) if *a == *felt) {
                theme::VISUAL_SELECTED_STYLE
            } else {
                color_map.style_for(felt)
            };
            spans.push(Span::styled(label, style));
        }
        DecodedValue::String(s) => {
            let display = if s.len() > 60 {
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
            spans.push(Span::styled(
                format!("{short} {{ {} fields }}", fields.len()),
                theme::TX_HASH_STYLE,
            ));
        }
        DecodedValue::Enum {
            name,
            variant,
            value: inner,
        } => {
            let short = name.rsplit("::").next().unwrap_or(name);
            let suffix = if inner.is_some() { "(...)" } else { "" };
            spans.push(Span::styled(
                format!("{short}::{variant}{suffix}"),
                theme::TX_HASH_STYLE,
            ));
        }
        DecodedValue::Array(items) => {
            spans.push(Span::styled(
                format!("[{} items]", items.len()),
                theme::TX_HASH_STYLE,
            ));
        }
        DecodedValue::Tuple(items) => {
            spans.push(Span::styled(
                format!("({} items)", items.len()),
                theme::TX_HASH_STYLE,
            ));
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
