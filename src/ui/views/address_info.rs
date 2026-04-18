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
use crate::ui::widgets::hex_display::{format_fri, format_strk_u128, short_hash};
use crate::ui::widgets::price;
use crate::ui::widgets::{search_bar, status_bar};
use crate::utils::{felt_to_u64, felt_to_u128};

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
        if let Some(vl) = app.voyager_labels.get(&info.address) {
            if let Some(ca) = &vl.class_alias {
                if !ca.is_empty() {
                    class_line.push(Span::styled(format!(" [{}]", ca), theme::LABEL_STYLE));
                }
            }
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
        if let Some(sender) = deploy.sender {
            if sender != info.address && sender != Felt::ZERO {
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
    let tx_count = app.address.txs.items.len();
    let call_count = app.address.calls.items.len();
    let bal_count = app
        .address
        .info
        .as_ref()
        .map(|i| i.token_balances.len())
        .unwrap_or(0);
    let ev_count = app.address.events.len();

    let class_count = app.address.class_history.len();
    let titles = vec![
        Span::raw(format!(" Txs ({tx_count}) ")),
        Span::raw(format!(" Calls ({call_count}) ")),
        Span::raw(format!(" Balances ({bal_count}) ")),
        Span::raw(format!(" Events ({ev_count}) ")),
        Span::raw(format!(" Class ({class_count}) ")),
    ];
    let selected = match app.address.tab {
        AddressTab::Transactions => 0,
        AddressTab::Calls => 1,
        AddressTab::Balances => 2,
        AddressTab::Events => 3,
        AddressTab::ClassHistory => 4,
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
        Span::styled("Endpoint             ", theme::SUGGESTION_STYLE),
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

    // If a large nonce gap is deferred, remember its (hi_nonce → lo_nonce)
    // boundary so we can render a divider above the row where the jump occurs.
    let gap_boundary: Option<(u64, u64, u64, bool)> = app
        .address
        .unfilled_gap
        .as_ref()
        .map(|g| (g.hi_nonce, g.lo_nonce, g.missing_count, g.fill_dispatched));

    let mut prev_nonce: Option<u64> = None;
    let items: Vec<ListItem> = app
        .address
        .txs
        .items
        .iter()
        .map(|tx| {
            let fee_str = format_strk_u128(tx.total_fee_fri)
                .trim_end_matches(" STRK")
                .to_string();
            let tip_str = if tx.tip > 0 {
                format_fri(tx.tip as u128)
            } else {
                "0".to_string()
            };
            let age = format_age(tx.timestamp);
            let endpoint = if tx.endpoint_names.chars().count() > 20 {
                let truncated: String = tx.endpoint_names.chars().take(19).collect();
                format!("{truncated}…")
            } else {
                tx.endpoint_names.clone()
            };

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

            let main_line = Line::from(vec![
                Span::styled(format!(" {:<8}", tx.nonce), theme::NORMAL_STYLE),
                Span::styled(format!("{:<15}", tx.tx_type), type_style),
                Span::styled(
                    format!("{:<14}", short_hash(&tx.hash)),
                    theme::TX_HASH_STYLE,
                ),
                Span::styled(format!("{:<21}", endpoint), theme::LABEL_STYLE),
                Span::styled(format!("{:<17}", fee_str), theme::TX_FEE_STYLE),
                Span::styled(format!("{:<17}", tip_str), theme::SUGGESTION_STYLE),
                Span::styled(
                    format!("#{:<9}", tx.block_number),
                    theme::BLOCK_NUMBER_STYLE,
                ),
                Span::styled(format!("{:<4}", &tx.status), status_style),
                Span::styled(age, theme::BLOCK_AGE_STYLE),
            ]);

            // Insert a dimmed divider above the row that sits on the far side
            // of the unfilled gap (i.e. when we're about to step from hi_nonce
            // down to lo_nonce in the descending list).
            let separator: Option<Line> = match (prev_nonce, gap_boundary) {
                (Some(prev), Some((hi, lo, missing, dispatched)))
                    if prev == hi && tx.nonce == lo =>
                {
                    let msg = if dispatched {
                        format!(" ── gap of {missing} txs — loading / retry with 'r' ──")
                    } else {
                        format!(" ── {missing} txs hidden — scroll down to load ──")
                    };
                    Some(Line::from(Span::styled(msg, theme::SUGGESTION_STYLE)))
                }
                _ => None,
            };
            prev_nonce = Some(tx.nonce);

            let lines = match separator {
                Some(sep) => vec![sep, main_line],
                None => vec![main_line],
            };
            ListItem::new(lines)
        })
        .collect();

    let gap_suffix = match &app.address.unfilled_gap {
        Some(g) if !g.fill_dispatched => format!(
            " — {} older txs deferred (scroll down to load) ",
            g.missing_count
        ),
        Some(g) => format!(" — gap of {} txs (press r to retry) ", g.missing_count),
        None => String::new(),
    };
    let title = if app.is_loading {
        format!(
            " Transactions ({}) fetching...{} ",
            app.address.txs.items.len(),
            gap_suffix
        )
    } else {
        format!(
            " Transactions ({}){} ",
            app.address.txs.items.len(),
            gap_suffix
        )
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

    f.render_stateful_widget(list, list_area, &mut app.address.txs.state);
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
        Span::styled("    Sender              ", theme::SUGGESTION_STYLE),
        Span::styled("Function             ", theme::SUGGESTION_STYLE),
        Span::styled("Hash          ", theme::SUGGESTION_STYLE),
        Span::styled("Fee(STRK)        ", theme::SUGGESTION_STYLE),
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

    let items: Vec<ListItem> = app
        .address
        .calls
        .items
        .iter()
        .map(|call| {
            let sender_label = app.format_address(&call.sender);
            let sender_display = if sender_label.chars().count() > 20 {
                let truncated: String = sender_label.chars().take(19).collect();
                format!("{truncated}…")
            } else {
                sender_label
            };
            let func = if call.function_name.chars().count() > 20 {
                let truncated: String = call.function_name.chars().take(19).collect();
                format!("{truncated}…")
            } else {
                call.function_name.clone()
            };
            let fee_str = format_strk_u128(call.total_fee_fri)
                .trim_end_matches(" STRK")
                .to_string();
            let status_style = match call.status.as_str() {
                "OK" => theme::STATUS_OK,
                "REV" => theme::STATUS_REVERTED,
                _ => theme::SUGGESTION_STYLE,
            };

            let line = Line::from(vec![
                Span::styled(format!(" {:<20}", sender_display), theme::LABEL_STYLE),
                Span::styled(format!("{:<21}", func), theme::LABEL_STYLE),
                Span::styled(
                    format!("{:<14}", short_hash(&call.tx_hash)),
                    theme::TX_HASH_STYLE,
                ),
                Span::styled(format!("{:<17}", fee_str), theme::TX_FEE_STYLE),
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

    let title = if app.is_loading {
        format!(" Calls ({}) fetching... ", app.address.calls.items.len())
    } else {
        format!(" Calls ({}) ", app.address.calls.items.len())
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

    if nonzero.is_empty() {
        f.render_widget(
            Paragraph::new(" No token balances found")
                .style(theme::SUGGESTION_STYLE)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(theme::BORDER_STYLE),
                ),
            area,
        );
        return;
    }

    let items: Vec<ListItem> = nonzero
        .iter()
        .map(|bal| {
            let formatted = format_token_balance(bal);
            let mut spans = vec![
                Span::styled(format!(" {:<8}", bal.token_name), theme::LABEL_STYLE),
                Span::styled(format!("{:<24}", formatted), theme::NORMAL_STYLE),
            ];
            if let Some(usd) = balance_usd_value(app, bal) {
                spans.push(Span::styled(
                    format!("  {}", price::format_usd(usd)),
                    theme::SUGGESTION_STYLE,
                ));
            }
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

fn balance_usd_value(app: &App, bal: &TokenBalance) -> Option<f64> {
    let price = app
        .price_client
        .as_ref()?
        .get_today_price(&bal.token_address)?;
    let raw = felt_to_u128(&bal.balance_raw) as f64;
    let scale = 10f64.powi(bal.decimals as i32);
    Some(raw / scale * price)
}

fn draw_events_tab(f: &mut Frame, app: &App, area: Rect) {
    if app.address.events.is_empty() {
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
            area,
        );
        return;
    }

    let visible_height = area.height.saturating_sub(2) as usize; // -2 for borders
    let start = app.address.event_scroll;
    let end = (start + visible_height).min(app.address.events.len());

    let items: Vec<ListItem> = app.address.events[start..end]
        .iter()
        .enumerate()
        .map(|(i, event)| {
            let idx = start + i;
            let contract = app.format_address(&event.contract_address);
            let name = event.event_name.as_deref().unwrap_or("?");
            let tx_short = short_hash(&event.raw.transaction_hash);

            let marker = if idx == app.address.event_scroll {
                "> "
            } else {
                "  "
            };

            let line = Line::from(vec![
                Span::raw(marker),
                Span::styled(format!("{:<20}", name), theme::LABEL_STYLE),
                Span::styled(format!("{:<16} ", contract), theme::BLOCK_HASH_STYLE),
                Span::styled(format!("tx:{}", tx_short), theme::TX_HASH_STYLE),
            ]);
            ListItem::new(line)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::BORDER_STYLE)
            .title(Span::styled(
                format!(
                    " Events ({}/{}) ",
                    app.address.event_scroll + 1,
                    app.address.events.len()
                ),
                theme::TITLE_STYLE,
            )),
    );
    f.render_widget(list, area);
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
