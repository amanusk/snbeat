use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};

use crate::app::App;
use crate::ui::theme;
use crate::ui::widgets::hex_display::{format_fri, short_hash};
use crate::ui::widgets::{search_bar, status_bar};

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1), // search bar
        Constraint::Min(3),    // block list
        Constraint::Length(1), // status bar
    ])
    .split(f.area());

    // Search bar input line
    search_bar::draw_input(f, app, chunks[0]);

    // Block list
    draw_block_list(f, app, chunks[1]);

    // Status bar
    status_bar::draw(f, app, chunks[2]);

    // Search dropdown overlay (rendered LAST to float on top of everything)
    search_bar::draw_dropdown(f, app, chunks[0]);
}

fn draw_block_list(f: &mut Frame, app: &mut App, area: Rect) {
    // Column header
    let header_area = Rect { height: 1, ..area };
    let list_area = Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        ..area
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(" Block     ", theme::SUGGESTION_STYLE),
        Span::styled("Hash          ", theme::SUGGESTION_STYLE),
        Span::styled("Txs  ", theme::SUGGESTION_STYLE),
        Span::styled("Age       ", theme::SUGGESTION_STYLE),
        Span::styled("L2 Gas Price      ", theme::SUGGESTION_STYLE),
        Span::styled("L1 Gas Price      ", theme::SUGGESTION_STYLE),
        Span::styled("L1 Data Price     ", theme::SUGGESTION_STYLE),
        Span::styled("Sequencer", theme::SUGGESTION_STYLE),
    ]));
    f.render_widget(header, header_area);

    let items: Vec<ListItem> = app
        .blocks
        .items
        .iter()
        .map(|block| {
            let age = format_age(block.timestamp);
            let sequencer = app.format_address(&block.sequencer_address);
            let l2_gas = format_fri(block.l2_gas_price_fri);
            let l1_gas = format_fri(block.l1_gas_price_fri);
            let l1_data = format_fri(block.l1_data_gas_price_fri);
            let line = Line::from(vec![
                Span::styled(format!(" #{:<8}", block.number), theme::BLOCK_NUMBER_STYLE),
                Span::styled(
                    format!(" {} ", short_hash(&block.hash)),
                    theme::BLOCK_HASH_STYLE,
                ),
                Span::styled(
                    format!("{:>3} txs ", block.transaction_count),
                    theme::BLOCK_TX_COUNT_STYLE,
                ),
                Span::styled(format!("{:>8}  ", age), theme::BLOCK_AGE_STYLE),
                Span::styled(format!("{:<18}", l2_gas), theme::NORMAL_STYLE),
                Span::styled(format!("{:<18}", l1_gas), theme::NORMAL_STYLE),
                Span::styled(format!("{:<18}", l1_data), theme::NORMAL_STYLE),
                Span::styled(sequencer, theme::LABEL_STYLE),
            ]);
            ListItem::new(line)
        })
        .collect();

    let block_widget = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::BORDER_FOCUSED_STYLE)
        .title(Span::styled(" Blocks ", theme::TITLE_STYLE));

    let list = List::new(items)
        .block(block_widget)
        .highlight_style(theme::SELECTED_STYLE.add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");

    f.render_stateful_widget(list, list_area, &mut app.blocks.state);
}

fn format_age(timestamp: u64) -> String {
    let now = chrono::Utc::now().timestamp() as u64;
    if timestamp > now {
        return "just now".to_string();
    }
    let diff = now - timestamp;
    if diff < 60 {
        format!("{diff}s ago")
    } else if diff < 3600 {
        format!("{}m ago", diff / 60)
    } else if diff < 86400 {
        format!("{}h ago", diff / 3600)
    } else {
        format!("{}d ago", diff / 86400)
    }
}
