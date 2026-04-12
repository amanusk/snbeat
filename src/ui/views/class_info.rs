use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap,
};

use crate::app::App;
use crate::app::state::TxNavItem;
use crate::ui::theme;
use crate::ui::widgets::{search_bar, status_bar};

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1), // search bar
        Constraint::Min(3),    // scrollable class detail
        Constraint::Length(1), // status bar
    ])
    .split(f.area());

    let selected: Option<TxNavItem> = if app.class.visual_mode {
        app.class.nav_items.get(app.class.nav_cursor).cloned()
    } else {
        None
    };
    search_bar::draw_input(f, app, chunks[0]);
    let nav_line_map = draw_scrollable_detail(f, app, chunks[1], selected.as_ref());
    app.class.nav_item_lines = nav_line_map;
    status_bar::draw(f, app, chunks[2]);

    search_bar::draw_dropdown(f, app, chunks[0]);
}

fn draw_scrollable_detail(
    f: &mut Frame,
    app: &App,
    area: Rect,
    selected: Option<&TxNavItem>,
) -> Vec<u16> {
    let class_hash = match app.class.hash {
        Some(h) => h,
        None => {
            f.render_widget(
                Paragraph::new(" Loading class info...").style(theme::STATUS_LOADING),
                area,
            );
            return vec![];
        }
    };

    let mut lines: Vec<Line> = Vec::new();
    let mut line_map: Vec<Option<u16>> = vec![None; app.class.nav_items.len()];

    let record =
        |item: &TxNavItem, lines: &Vec<Line>, map: &mut Vec<Option<u16>>, nav: &[TxNavItem]| {
            if let Some(idx) = nav.iter().position(|x| x == item) {
                map[idx].get_or_insert(lines.len() as u16);
            }
        };

    // === CLASS HASH HEADER ===
    lines.push(Line::from(vec![
        Span::styled(" Class:  ", theme::NORMAL_STYLE),
        Span::styled(format!("{:#x}", class_hash), theme::TX_HASH_STYLE),
    ]));
    lines.push(Line::from(""));

    // === DECLARATION INFO ===
    lines.push(Line::from(Span::styled(
        " Declaration",
        theme::SUGGESTION_STYLE,
    )));

    if let Some(decl_block) = app.class.decl_block {
        let blk_item = TxNavItem::Block(decl_block);
        let blk_style = if matches!(selected, Some(TxNavItem::Block(b)) if *b == decl_block) {
            theme::VISUAL_SELECTED_STYLE
        } else {
            theme::BLOCK_NUMBER_STYLE
        };
        record(&blk_item, &lines, &mut line_map, &app.class.nav_items);
        lines.push(Line::from(vec![
            marker(&blk_item, selected),
            Span::styled("Block:       ", theme::NORMAL_STYLE),
            Span::styled(format!("#{}", decl_block), blk_style),
        ]));
    }

    if let Some(decl) = &app.class.declare {
        // Declare tx hash
        let tx_item = TxNavItem::Transaction(decl.tx_hash);
        let tx_style = if selected == Some(&tx_item) {
            theme::VISUAL_SELECTED_STYLE
        } else {
            theme::TX_HASH_STYLE
        };
        record(&tx_item, &lines, &mut line_map, &app.class.nav_items);
        lines.push(Line::from(vec![
            marker(&tx_item, selected),
            Span::styled("Tx:          ", theme::NORMAL_STYLE),
            Span::styled(format!("{:#x}", decl.tx_hash), tx_style),
        ]));

        // Declared by
        let addr_item = TxNavItem::Address(decl.sender);
        let addr_style = if selected == Some(&addr_item) {
            theme::VISUAL_SELECTED_STYLE
        } else {
            theme::NORMAL_STYLE
        };
        record(&addr_item, &lines, &mut line_map, &app.class.nav_items);
        let mut decl_by_spans = vec![
            marker(&addr_item, selected),
            Span::styled("Declared by: ", theme::NORMAL_STYLE),
            Span::styled(format!("{:#x}", decl.sender), addr_style),
        ];
        let label = app.format_address_full(&decl.sender);
        if !label.starts_with("0x") {
            decl_by_spans.push(Span::styled(format!("  {}", label), theme::LABEL_STYLE));
        }
        lines.push(Line::from(decl_by_spans));
    } else if app.is_loading {
        lines.push(Line::from(Span::styled(
            "   Loading declaration info...",
            theme::STATUS_LOADING,
        )));
    } else {
        lines.push(Line::from(Span::styled(
            "   Declaration info not available",
            theme::SUGGESTION_STYLE,
        )));
    }

    lines.push(Line::from(""));

    // === ABI FUNCTIONS ===
    if let Some(abi) = &app.class.abi {
        let mut read_fns: Vec<&crate::decode::abi::FunctionDef> = Vec::new();
        let mut write_fns: Vec<&crate::decode::abi::FunctionDef> = Vec::new();

        for func in abi.functions.values() {
            if func.state_mutability.as_deref() == Some("view") {
                read_fns.push(func);
            } else {
                write_fns.push(func);
            }
        }
        read_fns.sort_by(|a, b| a.name.cmp(&b.name));
        write_fns.sort_by(|a, b| a.name.cmp(&b.name));

        let abi_hint = if app.class.show_abi {
            " [a: hide]"
        } else {
            " [a: show]"
        };
        lines.push(Line::from(vec![
            Span::styled(" ABI", theme::SUGGESTION_STYLE),
            Span::styled(
                format!("  (Read: {} | Write: {})", read_fns.len(), write_fns.len()),
                theme::SUGGESTION_STYLE,
            ),
            Span::styled(abi_hint, theme::SUGGESTION_STYLE),
        ]));

        if app.class.show_abi {
            if !read_fns.is_empty() {
                lines.push(Line::from(Span::styled(
                    "   Read Functions:",
                    theme::NORMAL_STYLE,
                )));
                for func in &read_fns {
                    lines.push(format_function_line(func));
                }
            }

            if !write_fns.is_empty() {
                lines.push(Line::from(Span::styled(
                    "   Write Functions:",
                    theme::NORMAL_STYLE,
                )));
                for func in &write_fns {
                    lines.push(format_function_line(func));
                }
            }
        }
    } else if app.is_loading {
        lines.push(Line::from(Span::styled(
            " ABI  Loading...",
            theme::STATUS_LOADING,
        )));
    } else {
        lines.push(Line::from(Span::styled(
            " ABI  Not available",
            theme::SUGGESTION_STYLE,
        )));
    }

    lines.push(Line::from(""));

    // === DEPLOYED CONTRACTS ===
    let contract_count = app.class.contracts.len();
    lines.push(Line::from(Span::styled(
        format!(" Deployed Contracts ({})", contract_count),
        theme::SUGGESTION_STYLE,
    )));

    if contract_count > 0 {
        // Header
        lines.push(Line::from(vec![
            Span::styled(
                "   Address                           ",
                theme::SUGGESTION_STYLE,
            ),
            Span::styled("Block          ", theme::SUGGESTION_STYLE),
            Span::styled("Age", theme::SUGGESTION_STYLE),
        ]));

        for entry in &app.class.contracts {
            let addr_item = TxNavItem::Address(entry.address);
            let addr_style = if selected == Some(&addr_item) {
                theme::VISUAL_SELECTED_STYLE
            } else {
                theme::TX_HASH_STYLE
            };
            record(&addr_item, &lines, &mut line_map, &app.class.nav_items);

            let addr_label = app.format_address(&entry.address);
            // Estimate age from block number relative to latest
            let age_str = if app.latest_block_number > 0 && entry.block_number > 0 {
                let blocks_ago = app.latest_block_number.saturating_sub(entry.block_number);
                // ~12s per block on Starknet
                let secs = blocks_ago * 12;
                if secs < 3600 {
                    format!("{}m", secs / 60)
                } else if secs < 86400 {
                    format!("{}h", secs / 3600)
                } else {
                    format!("{}d", secs / 86400)
                }
            } else {
                String::new()
            };

            lines.push(Line::from(vec![
                marker(&addr_item, selected),
                Span::styled(format!("  {:<33}", addr_label), addr_style),
                Span::styled(
                    format!("#{:<13}", entry.block_number),
                    theme::BLOCK_NUMBER_STYLE,
                ),
                Span::styled(age_str, theme::SUGGESTION_STYLE),
            ]));
        }
    } else if app.is_loading {
        lines.push(Line::from(Span::styled(
            "   Loading...",
            theme::STATUS_LOADING,
        )));
    } else {
        lines.push(Line::from(Span::styled(
            "   No contracts found (requires PF service)",
            theme::SUGGESTION_STYLE,
        )));
    }

    // Render with scrolling
    let total_lines = lines.len() as u16;
    let scroll = app.class.scroll.min(total_lines.saturating_sub(1));
    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_STYLE),
        )
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    f.render_widget(paragraph, area);

    // Scrollbar
    if total_lines > area.height {
        let mut scrollbar_state =
            ScrollbarState::new(total_lines as usize).position(scroll as usize);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            area,
            &mut scrollbar_state,
        );
    }

    // Flatten line_map (replace None with 0)
    line_map.into_iter().map(|o| o.unwrap_or(0)).collect()
}

/// Format a function signature line for ABI display.
fn format_function_line(func: &crate::decode::abi::FunctionDef) -> Line<'static> {
    let params: Vec<String> = func
        .inputs
        .iter()
        .map(|(name, ty)| {
            let short_ty = ty.rsplit("::").next().unwrap_or(ty);
            format!("{}: {}", name, short_ty)
        })
        .collect();
    let ret = if func.outputs.is_empty() {
        String::new()
    } else {
        let short_rets: Vec<&str> = func
            .outputs
            .iter()
            .map(|t| t.rsplit("::").next().unwrap_or(t))
            .collect();
        format!(" -> {}", short_rets.join(", "))
    };
    Line::from(Span::styled(
        format!("     {}({}){}", func.name, params.join(", "), ret),
        theme::NORMAL_STYLE,
    ))
}

/// Returns a `►` marker span when this item is selected, otherwise a space.
fn marker(item: &TxNavItem, selected: Option<&TxNavItem>) -> Span<'static> {
    if selected == Some(item) {
        Span::styled("►", theme::VISUAL_SELECTED_STYLE)
    } else {
        Span::raw(" ")
    }
}
