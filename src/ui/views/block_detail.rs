use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};

use crate::app::App;
use crate::ui::theme;
use crate::ui::widgets::address_color::{AddressColorMap, known_or_palette_style};
use crate::ui::widgets::hex_display::{format_fee, format_fri, short_hash, tx_hash_cell};
use crate::ui::widgets::{search_bar, status_bar};
use crate::utils::felt_to_u64;

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1), // search bar
        Constraint::Length(8), // block header
        Constraint::Min(3),    // tx list
        Constraint::Length(1), // status bar
    ])
    .split(f.area());

    search_bar::draw_input(f, app, chunks[0]);
    draw_header(f, app, chunks[1]);
    draw_tx_list(f, app, chunks[2]);
    status_bar::draw(f, app, chunks[3]);

    // Search dropdown overlay (last)
    search_bar::draw_dropdown(f, app, chunks[0]);
}

fn draw_header(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let block = match &app.block_detail.block {
        Some(b) => b,
        None => {
            let loading = Paragraph::new(" Loading block...").style(theme::STATUS_LOADING);
            f.render_widget(loading, area);
            return;
        }
    };

    let age = {
        let now = chrono::Utc::now().timestamp() as u64;
        let diff = now.saturating_sub(block.timestamp);
        if diff < 60 {
            format!("{diff}s ago")
        } else if diff < 3600 {
            format!("{}m ago", diff / 60)
        } else {
            format!("{}h ago", diff / 3600)
        }
    };

    let lines = vec![
        Line::from(vec![
            Span::styled(" Hash: ", theme::NORMAL_STYLE),
            Span::styled(format!("{:#x}", block.hash), theme::TX_HASH_STYLE),
        ]),
        Line::from(vec![
            Span::styled(" Parent: ", theme::NORMAL_STYLE),
            Span::styled(short_hash(&block.parent_hash), theme::BLOCK_HASH_STYLE),
        ]),
        Line::from(vec![
            Span::styled(" Timestamp: ", theme::NORMAL_STYLE),
            Span::raw(format!("{} ({})", block.timestamp, age)),
        ]),
        Line::from(vec![
            Span::styled(" Sequencer: ", theme::NORMAL_STYLE),
            Span::styled(
                app.format_address(&block.sequencer_address),
                theme::LABEL_STYLE,
            ),
        ]),
        Line::from(vec![
            Span::styled(" L1 Gas: ", theme::NORMAL_STYLE),
            Span::raw(format_fri(block.l1_gas_price_fri)),
            Span::raw("  "),
            Span::styled("L2 Gas: ", theme::NORMAL_STYLE),
            Span::raw(format_fri(block.l2_gas_price_fri)),
            Span::raw("  "),
            Span::styled("L1 Data: ", theme::NORMAL_STYLE),
            Span::raw(format_fri(block.l1_data_gas_price_fri)),
        ]),
        Line::from(vec![
            Span::styled(" Txs: ", theme::NORMAL_STYLE),
            Span::styled(
                block.transaction_count.to_string(),
                theme::BLOCK_TX_COUNT_STYLE,
            ),
            Span::raw(format!("  Version: {}", block.starknet_version)),
        ]),
    ];

    let widget = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::BORDER_STYLE)
            .title(Span::styled(
                format!(" Block #{} ", block.number),
                theme::TITLE_STYLE,
            )),
    );
    f.render_widget(widget, area);
}

fn draw_tx_list(f: &mut Frame, app: &mut App, area: ratatui::layout::Rect) {
    // Header row
    let header_area = Rect { height: 1, ..area };
    let list_area = Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        ..area
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled("      Idx  ", theme::SUGGESTION_STYLE),
        Span::styled("St  ", theme::SUGGESTION_STYLE),
        Span::styled("Prv ", theme::SUGGESTION_STYLE),
        Span::styled("Type            ", theme::SUGGESTION_STYLE),
        Span::styled("Meta      ", theme::SUGGESTION_STYLE),
        Span::styled("Hash          ", theme::SUGGESTION_STYLE),
        Span::styled("Sender               ", theme::SUGGESTION_STYLE),
        Span::styled("Intender             ", theme::SUGGESTION_STYLE),
        Span::styled(
            "Endpoint(s)                            ",
            theme::SUGGESTION_STYLE,
        ),
        Span::styled("Nonce      ", theme::SUGGESTION_STYLE),
        Span::styled("Fee(STRK)        ", theme::SUGGESTION_STYLE),
        Span::styled("Tip             ", theme::SUGGESTION_STYLE),
    ]));
    f.render_widget(header, header_area);

    // Count how many times each sender appears.
    let mut counts: std::collections::HashMap<starknet::core::types::Felt, usize> =
        std::collections::HashMap::new();
    for tx in &app.block_detail.txs.items {
        *counts.entry(tx.sender()).or_insert(0) += 1;
    }

    // Assign palette colors only to senders that appear more than once AND have no
    // registry label. Known addresses are already identifiable by their tag; one-off
    // addresses don't need a color since there's nothing to spot.
    let mut color_map = AddressColorMap::new();
    if let Some(engine) = &app.search_engine {
        color_map.set_privacy_overrides(engine.registry().privacy_addresses());
    }
    for tx in &app.block_detail.txs.items {
        let addr = tx.sender();
        let is_repeat = counts.get(&addr).copied().unwrap_or(0) > 1;
        let is_known = app
            .search_engine
            .as_ref()
            .map(|e| e.registry().is_known(&addr))
            .unwrap_or(false);
        if is_repeat && !is_known {
            color_map.register(addr);
        }
    }

    // In visual mode, the focused sender is the one at block_nav_cursor.
    let focused_sender = if app.block_detail.visual_mode {
        app.block_detail
            .txs
            .items
            .get(app.block_detail.nav_cursor)
            .map(|tx| tx.sender())
    } else {
        None
    };

    // Items are already in descending order (reversed when loaded)
    let items: Vec<ListItem> = app
        .block_detail
        .txs
        .items
        .iter()
        .enumerate()
        .map(|(i, tx)| {
            let type_style = match tx.type_name() {
                "INVOKE" => theme::TX_TYPE_INVOKE,
                "DECLARE" => theme::TX_TYPE_DECLARE,
                "DEPLOY_ACCOUNT" | "DEPLOY" => theme::TX_TYPE_DEPLOY,
                "L1_HANDLER" => theme::TX_TYPE_L1HANDLER,
                _ => theme::NORMAL_STYLE,
            };

            let fee_str = tx
                .actual_fee()
                .map(|f| format_fee(&f).trim_end_matches(" STRK").to_string())
                .unwrap_or_else(|| "0".to_string());

            let nonce_str = tx
                .nonce()
                .map(|n| felt_to_u64(&n).to_string())
                .unwrap_or_default();

            let tip_str = format_fri(tx.tip() as u128);

            // Decoded endpoint names from the ABI (may be multicall)
            let endpoint = app
                .block_detail
                .endpoint_names
                .get(i)
                .and_then(|n| n.as_deref())
                .unwrap_or("");
            let endpoint_display = if endpoint.chars().count() > 38 {
                let truncated: String = endpoint.chars().take(37).collect();
                format!("{truncated}…")
            } else {
                endpoint.to_string()
            };

            // Execution status from receipt
            let status = app
                .block_detail
                .tx_statuses
                .get(i)
                .map(|s| s.as_str())
                .unwrap_or("?");
            let status_style = match status {
                "OK" => theme::STATUS_OK,
                "REV" => theme::STATUS_REVERTED,
                _ => theme::SUGGESTION_STYLE,
            };

            let sender = tx.sender();
            let sender_label = app.format_address(&sender);
            let sender_display = if sender_label.chars().count() > 20 {
                let truncated: String = sender_label.chars().take(19).collect();
                format!("{truncated}…")
            } else {
                sender_label
            };

            // Visual mode: selected address (and all matching) get highlight style.
            // Outside visual mode:
            //   - known/tagged address  → LABEL_STYLE (bold yellow, already identifiable)
            //   - repeat unknown        → palette color (spot the pattern)
            //   - one-off unknown       → NORMAL_STYLE (no color wasted)
            let registry = app.search_engine.as_ref().map(|e| e.registry());
            let sender_style = match focused_sender {
                Some(focused) if sender == focused => theme::VISUAL_SELECTED_STYLE,
                _ => known_or_palette_style(&sender, registry, &color_map),
            };

            // Marker column: ► on the visually selected row, space otherwise.
            let marker = if app.block_detail.visual_mode && i == app.block_detail.nav_cursor {
                "► "
            } else {
                "  "
            };

            // Meta TX indicator and intender
            let meta_info = app
                .block_detail
                .meta_tx_info
                .get(i)
                .and_then(|m| m.as_ref());
            let meta_str = match meta_info {
                Some(m) => format!("Meta({})", m.version),
                None => String::new(),
            };
            let meta_intender = meta_info.map(|m| &m.intender);
            let intender_display = if let Some(intender) = meta_intender {
                let label = app.format_address(intender);
                if label.chars().count() > 20 {
                    let truncated: String = label.chars().take(19).collect();
                    format!("{truncated}…")
                } else {
                    label
                }
            } else {
                String::new()
            };
            let intender_style = if let Some(intender) = meta_intender {
                match focused_sender {
                    Some(focused) if *intender == focused => theme::VISUAL_SELECTED_STYLE,
                    _ => known_or_palette_style(intender, registry, &color_map),
                }
            } else {
                theme::NORMAL_STYLE
            };

            let tx_hash = tx.hash();
            let tx_label = app.resolve_tx(&tx_hash);
            let tx_hash_display = tx_hash_cell(tx_label, &tx_hash);
            let tx_hash_style = if tx_label.is_some() {
                theme::LABEL_STYLE
            } else {
                theme::TX_HASH_STYLE
            };

            let is_priv = app
                .block_detail
                .is_privacy_tx
                .get(i)
                .copied()
                .unwrap_or(false);
            // Solid orange vertical bar — single-cell so the column aligns
            // reliably across terminals (a 🛡 emoji rendered 1 cell wide in
            // some setups while ratatui's unicode-width measured 2, so the
            // row shifted by one column).
            let prv_marker_text = if is_priv { "▌   " } else { "    " };

            let line = Line::from(vec![
                Span::styled(format!(" {marker}"), theme::NORMAL_STYLE),
                Span::styled(format!("{:<4} ", tx.index()), theme::BLOCK_NUMBER_STYLE),
                Span::styled(format!("{:<4}", status), status_style),
                Span::styled(prv_marker_text, theme::PRIVACY_STYLE),
                Span::styled(format!("{:<15}", tx.type_name()), type_style),
                Span::styled(format!("{:<10}", meta_str), theme::META_TX_STYLE),
                Span::styled(format!("{:<14}", tx_hash_display), tx_hash_style),
                Span::styled(format!("{:<21}", sender_display), sender_style),
                Span::styled(format!("{:<21}", intender_display), intender_style),
                Span::styled(format!("{:<38} ", endpoint_display), theme::LABEL_STYLE),
                Span::styled(format!("{:<11}", nonce_str), theme::NORMAL_STYLE),
                Span::styled(format!("{:<17}", fee_str), theme::TX_FEE_STYLE),
                Span::styled(format!("{:<16}", tip_str), theme::TX_FEE_STYLE),
            ]);
            ListItem::new(line)
        })
        .collect();

    let title = if app.block_detail.visual_mode {
        " Transactions  [VISUAL: j/k navigate · Enter: open address · Esc: exit] "
    } else {
        " Transactions "
    };

    let block_widget = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::BORDER_FOCUSED_STYLE)
        .title(Span::styled(title, theme::TITLE_STYLE));

    let list = List::new(items)
        .block(block_widget)
        .highlight_style(theme::SELECTED_STYLE.add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");

    f.render_stateful_widget(list, list_area, &mut app.block_detail.txs.state);
}
