use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::App;
use crate::app::actions::Action;
use crate::app::state::{Focus, InputMode, NavTarget, View};
use crate::utils::felt_to_u64;

/// Handle a key event and return an optional Action to dispatch to the network task.
pub fn handle_key(app: &mut App, key: KeyEvent) -> Option<Action> {
    // Ctrl+C always quits
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.should_quit = true;
        return None;
    }

    match app.input_mode {
        InputMode::Normal => handle_normal_mode(app, key),
        InputMode::Search => handle_search_mode(app, key),
    }
}

fn handle_normal_mode(app: &mut App, key: KeyEvent) -> Option<Action> {
    // Intercept all keys when TxDetail visual mode is active
    if app.tx_detail.visual_mode && app.current_view() == View::TxDetail {
        return handle_tx_visual_mode(app, key);
    }

    // Intercept all keys when BlockDetail visual mode is active
    if app.block_detail.visual_mode && app.current_view() == View::BlockDetail {
        return handle_block_visual_mode(app, key);
    }

    // Intercept all keys when ClassInfo visual mode is active
    if app.class.visual_mode && app.current_view() == View::ClassInfo {
        return handle_class_visual_mode(app, key);
    }

    // Intercept all keys when AddressInfo visual mode is active
    if app.address.visual_mode && app.current_view() == View::AddressInfo {
        return handle_address_visual_mode(app, key);
    }

    match key.code {
        // q: jump to Blocks root (clearing forward history), or quit if already there
        KeyCode::Char('q') => {
            app.go_to_root_or_quit();
            None
        }

        // Search mode
        KeyCode::Char('/') => {
            app.input_mode = InputMode::Search;
            app.focus = Focus::SearchBar;
            app.search_input.clear();
            app.search_cursor = 0;
            app.search_suggestions.clear();
            app.search_selected = 0;
            None
        }

        // Navigation: down
        KeyCode::Char('j') | KeyCode::Down => {
            app.select_next();
            None
        }

        // Navigation: up
        KeyCode::Char('k') | KeyCode::Up => {
            app.select_previous();
            None
        }

        // Navigation: back one view (saves to forward history for Ctrl+i)
        KeyCode::Char('h') | KeyCode::Left => {
            app.pop_view_saving_forward();
            None
        }

        // Ctrl+o: back one view (vim-style jump back)
        KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.pop_view_saving_forward();
            None
        }

        // Navigation: forward / drill in
        KeyCode::Char('l') | KeyCode::Right | KeyCode::Enter => handle_enter(app),

        // Jump to top
        KeyCode::Char('g') => {
            app.select_first();
            None
        }

        // Jump to bottom
        KeyCode::Char('G') => {
            app.select_last();
            None
        }

        // Ctrl+U / PgUp: cycle to next block or tx
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => handle_cycle(app, 1),
        KeyCode::PageUp => handle_cycle(app, 1),

        // Ctrl+D / PgDn: cycle to prev block or tx
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            handle_cycle(app, -1)
        }
        KeyCode::PageDown => handle_cycle(app, -1),

        // Tab: cycle address tabs (AddressInfo), no-op elsewhere
        KeyCode::Tab => {
            if app.current_view() == View::AddressInfo {
                app.address.tab = match app.address.tab {
                    crate::app::AddressTab::Transactions => crate::app::AddressTab::Calls,
                    crate::app::AddressTab::Calls => crate::app::AddressTab::Balances,
                    crate::app::AddressTab::Balances => crate::app::AddressTab::Events,
                    crate::app::AddressTab::Events => crate::app::AddressTab::ClassHistory,
                    crate::app::AddressTab::ClassHistory => crate::app::AddressTab::Transactions,
                };
            }
            None
        }

        // ]: jump forward in history (replaces Ctrl+i which terminals map to Tab)
        KeyCode::Char(']') => app.navigate_forward(),

        // n/N: nonce-based tx cycling (in TxDetail, navigate to next/prev tx by same sender)
        KeyCode::Char('n') => handle_nonce_cycle(app, 1),
        KeyCode::Char('N') => handle_nonce_cycle(app, -1),

        // Refresh
        KeyCode::Char('r') => match app.current_view() {
            View::AddressInfo => {
                if let Some(addr) = app.address.context {
                    app.is_loading = true;
                    Some(Action::FetchAddressInfo { address: addr })
                } else {
                    None
                }
            }
            View::ClassInfo => {
                if let Some(ch) = app.class.hash {
                    app.clear_class_info();
                    Some(Action::FetchClassInfo { class_hash: ch })
                } else {
                    None
                }
            }
            _ => Some(Action::FetchRecentBlocks { count: 50 }),
        },

        // Help toggle
        KeyCode::Char('?') => {
            app.show_help = !app.show_help;
            None
        }

        // Escape: back one view (saves forward history)
        KeyCode::Esc => {
            app.pop_view_saving_forward();
            None
        }

        // v: enter visual mode in TxDetail (cycle through navigable items)
        KeyCode::Char('v') if app.current_view() == View::TxDetail => {
            if !app.tx_detail.nav_items.is_empty() {
                app.tx_detail.visual_mode = true;
                app.tx_detail.nav_cursor = 0;
            }
            None
        }

        // v: enter visual mode in AddressInfo header (cycle through class, deploy tx, block, deployer)
        KeyCode::Char('v') if app.current_view() == View::AddressInfo => {
            if !app.address.nav_items.is_empty() {
                app.address.visual_mode = true;
                app.address.nav_cursor = 0;
            }
            None
        }

        // v: enter visual mode in ClassInfo (cycle through navigable items)
        KeyCode::Char('v') if app.current_view() == View::ClassInfo => {
            if !app.class.nav_items.is_empty() {
                app.class.visual_mode = true;
                app.class.nav_cursor = 0;
            }
            None
        }

        // v: enter visual mode in BlockDetail (cycle through tx senders)
        KeyCode::Char('v') if app.current_view() == View::BlockDetail => {
            if !app.block_detail.txs.items.is_empty() {
                app.block_detail.visual_mode = true;
                app.block_detail.nav_cursor = app.block_detail.txs.state.selected().unwrap_or(0);
            }
            None
        }

        // a: toggle ABI display in ClassInfo
        KeyCode::Char('a') if app.current_view() == View::ClassInfo => {
            app.class.show_abi = !app.class.show_abi;
            None
        }

        // c: toggle calldata display in TxDetail
        KeyCode::Char('c') if app.current_view() == View::TxDetail => {
            app.tx_detail.show_calldata = !app.tx_detail.show_calldata;
            None
        }

        // d: toggle decoded calldata display in TxDetail
        KeyCode::Char('d') if app.current_view() == View::TxDetail => {
            app.tx_detail.show_decoded_calldata = !app.tx_detail.show_decoded_calldata;
            None
        }

        // o: toggle outside execution intent view in TxDetail
        KeyCode::Char('o') if app.current_view() == View::TxDetail => {
            if !app.tx_detail.outside_executions.is_empty() {
                app.tx_detail.show_outside_execution = !app.tx_detail.show_outside_execution;
            }
            None
        }

        _ => None,
    }
}

/// Handle key events when TxDetail visual mode is active.
fn handle_tx_visual_mode(app: &mut App, key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            app.tx_nav_step(1);
            None
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.tx_nav_step(-1);
            None
        }
        KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => {
            let item = app
                .tx_detail
                .nav_items
                .get(app.tx_detail.nav_cursor)
                .cloned();
            item.and_then(|i| app.navigate_to(NavTarget::from_nav_item(&i)))
        }
        KeyCode::Char('c') => {
            app.tx_detail.show_calldata = !app.tx_detail.show_calldata;
            None
        }
        KeyCode::Char('d') => {
            app.tx_detail.show_decoded_calldata = !app.tx_detail.show_decoded_calldata;
            None
        }
        KeyCode::Char('o') => {
            if !app.tx_detail.outside_executions.is_empty() {
                app.tx_detail.show_outside_execution = !app.tx_detail.show_outside_execution;
            }
            None
        }
        KeyCode::Esc => {
            app.tx_detail.visual_mode = false;
            None
        }
        _ => None,
    }
}

/// Handle key events when BlockDetail visual mode is active.
fn handle_block_visual_mode(app: &mut App, key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            let max = app.block_detail.txs.items.len().saturating_sub(1);
            app.block_detail.nav_cursor = (app.block_detail.nav_cursor + 1).min(max);
            app.block_detail
                .txs
                .state
                .select(Some(app.block_detail.nav_cursor));
            None
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.block_detail.nav_cursor = app.block_detail.nav_cursor.saturating_sub(1);
            app.block_detail
                .txs
                .state
                .select(Some(app.block_detail.nav_cursor));
            None
        }
        KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => {
            let addr = app
                .block_detail
                .txs
                .items
                .get(app.block_detail.nav_cursor)
                .map(|tx| tx.sender());
            addr.and_then(|a| app.navigate_to(NavTarget::Address(a)))
        }
        KeyCode::Esc => {
            app.block_detail.visual_mode = false;
            None
        }
        _ => None,
    }
}

/// Handle key events when ClassInfo visual mode is active.
fn handle_class_visual_mode(app: &mut App, key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            app.class_nav_step(1);
            None
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.class_nav_step(-1);
            None
        }
        KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => {
            let item = app.class.nav_items.get(app.class.nav_cursor).cloned();
            item.and_then(|i| app.navigate_to(NavTarget::from_nav_item(&i)))
        }
        KeyCode::Esc => {
            app.class.visual_mode = false;
            None
        }
        _ => None,
    }
}

/// Handle key events when AddressInfo visual mode is active (header navigation).
fn handle_address_visual_mode(app: &mut App, key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            app.address_nav_step(1);
            None
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.address_nav_step(-1);
            None
        }
        KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => {
            let item = app.address.nav_items.get(app.address.nav_cursor).cloned();
            item.and_then(|i| app.navigate_to(NavTarget::from_nav_item(&i)))
        }
        KeyCode::Esc => {
            app.address.visual_mode = false;
            None
        }
        _ => None,
    }
}

fn handle_search_mode(app: &mut App, key: KeyEvent) -> Option<Action> {
    // Arrow keys navigate dropdown when suggestions are visible
    if !app.search_suggestions.is_empty() {
        match key.code {
            KeyCode::Down => {
                app.search_selected =
                    (app.search_selected + 1).min(app.search_suggestions.len() - 1);
                return None;
            }
            KeyCode::Up => {
                app.search_selected = app.search_selected.saturating_sub(1);
                return None;
            }
            _ => {}
        }
    }

    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.focus = Focus::BlockList;
            app.search_input.clear();
            app.search_cursor = 0;
            app.search_suggestions.clear();
            app.search_selected = 0;
            None
        }

        KeyCode::Enter => {
            // If a suggestion is selected and dropdown is visible, use it
            let query = if !app.search_suggestions.is_empty()
                && app.search_selected < app.search_suggestions.len()
            {
                format!("{:#x}", app.search_suggestions[app.search_selected].address)
            } else {
                app.search_input.clone()
            };
            app.input_mode = InputMode::Normal;
            app.focus = Focus::BlockList;
            app.search_suggestions.clear();
            app.search_selected = 0;
            if query.is_empty() {
                None
            } else {
                Some(Action::ResolveSearch { query })
            }
        }

        // Tab accepts the selected suggestion's NAME into the input field
        KeyCode::Tab => {
            if let Some(result) = app.search_suggestions.get(app.search_selected) {
                // Extract just the name part (before the " (0x..." suffix)
                let name = extract_name_from_display(&result.display);
                app.search_input = name;
                app.search_cursor = app.search_input.len();
                app.update_suggestions();
            }
            None
        }

        KeyCode::Char(c) => {
            app.search_input.insert(app.search_cursor, c);
            app.search_cursor += 1;
            app.search_selected = 0;
            app.update_suggestions();
            None
        }

        KeyCode::Backspace => {
            if app.search_cursor > 0 {
                app.search_cursor -= 1;
                app.search_input.remove(app.search_cursor);
            }
            app.search_selected = 0;
            app.update_suggestions();
            None
        }

        KeyCode::Left => {
            if app.search_cursor > 0 {
                app.search_cursor -= 1;
            }
            None
        }

        KeyCode::Right => {
            if app.search_cursor < app.search_input.len() {
                app.search_cursor += 1;
            }
            None
        }

        // Up/Down — ignore when no suggestions (handled above when suggestions exist)
        KeyCode::Up | KeyCode::Down => None,

        _ => None,
    }
}

/// Extract the name portion from a search result display string.
/// Display format: " ETH (0x49d..dc7)" or "*ETH (0x49d..dc7)"
/// Returns: "ETH"
fn extract_name_from_display(display: &str) -> String {
    let trimmed = display.trim().trim_start_matches('*').trim();
    if let Some(paren_pos) = trimmed.find(" (") {
        trimmed[..paren_pos].to_string()
    } else {
        trimmed.to_string()
    }
}

/// Cycle to the next/previous block or transaction depending on active view.
fn handle_cycle(app: &mut App, direction: i64) -> Option<Action> {
    match app.current_view() {
        View::BlockDetail => {
            let current = app.block_detail.block.as_ref()?.number;
            let target = (current as i64 + direction).max(0) as u64;
            if target == current {
                return None;
            }
            // Stay in same view -- clear and refetch without pushing
            app.is_loading = true;
            app.clear_block_detail();
            Some(Action::FetchBlockDetail { number: target })
        }
        View::TxDetail => {
            let current_hash = app.tx_detail.transaction.as_ref()?.hash();
            let idx = app
                .block_detail
                .txs
                .items
                .iter()
                .position(|tx| tx.hash() == current_hash)?;
            let new_idx = (idx as i64 + direction).max(0) as usize;
            let new_idx = new_idx.min(app.block_detail.txs.items.len().saturating_sub(1));
            if new_idx == idx {
                return None;
            }
            let hash = app.block_detail.txs.items[new_idx].hash();
            app.block_detail.txs.state.select(Some(new_idx));
            // Stay in same view -- clear and refetch without pushing
            app.is_loading = true;
            app.clear_tx_detail();
            Some(Action::FetchTransaction { hash })
        }
        View::AddressInfo => {
            // Half-page-ish scroll inside the active address tab.
            // direction: +1 = Ctrl+U / PageUp (up), -1 = Ctrl+D / PageDown (down).
            // Address lists are newest-first, so "up" means lower index.
            const CHUNK: i64 = 20;
            let delta = -direction * CHUNK;
            match app.address.tab {
                crate::app::AddressTab::Transactions => app.address.txs.scroll_by(delta),
                crate::app::AddressTab::Calls => app.address.calls.scroll_by(delta),
                _ => {}
            }
            None
        }
        _ => None,
    }
}

/// Navigate to the next/prev tx by the same sender (nonce-based).
/// Uses the cached address tx list if available, falls back to RPC.
fn handle_nonce_cycle(app: &mut App, direction: i64) -> Option<Action> {
    if app.current_view() != View::TxDetail {
        return None;
    }
    let tx = app.tx_detail.transaction.as_ref()?;
    let current_nonce = match tx {
        crate::data::types::SnTransaction::Invoke(i) => {
            i.nonce.map(|n| felt_to_u64(&n)).unwrap_or(0)
        }
        _ => return None,
    };

    let target_nonce = (current_nonce as i64 + direction).max(0) as u64;

    // Try to find the tx in the cached address tx list first (instant, no RPC)
    if let Some(summary) = app
        .address
        .txs
        .items
        .iter()
        .find(|s| s.nonce == target_nonce)
    {
        let hash = summary.hash;
        // Stay in same view -- clear and refetch without pushing
        app.is_loading = true;
        app.clear_tx_detail();
        return Some(Action::FetchTransaction { hash });
    }

    // Fall back to RPC search
    let sender = tx.sender();
    app.is_loading = true;
    app.clear_tx_detail();
    Some(Action::FetchTxByNonce {
        sender,
        current_nonce: current_nonce,
        direction,
    })
}

fn handle_enter(app: &mut App) -> Option<Action> {
    match app.current_view() {
        View::Blocks => {
            let number = app.selected_block()?.number;
            app.navigate_to(NavTarget::Block(number))
        }
        View::BlockDetail => {
            let hash = app.selected_transaction()?.hash();
            app.navigate_to(NavTarget::Transaction(hash))
        }
        View::AddressInfo => {
            match app.address.tab {
                crate::app::AddressTab::Transactions => {
                    let hash = app.address.txs.selected_item()?.hash;
                    return app.navigate_to(NavTarget::Transaction(hash));
                }
                crate::app::AddressTab::Calls => {
                    let hash = app.address.calls.selected_item()?.tx_hash;
                    return app.navigate_to(NavTarget::Transaction(hash));
                }
                crate::app::AddressTab::ClassHistory => {
                    let felt = app
                        .address
                        .class_history
                        .get(app.address.class_history_scroll)
                        .and_then(|e| starknet::core::types::Felt::from_hex(&e.class_hash).ok())?;
                    return app.navigate_to(NavTarget::ClassHash(felt));
                }
                _ => {}
            }
            None
        }
        View::TxDetail => None,
        View::ClassInfo => None,
    }
}
