use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};

use crate::app::App;
use crate::app::state::InputMode;
use crate::ui::theme;

/// Draw just the search input line (call this first in the layout).
pub fn draw_input(f: &mut Frame, app: &App, area: Rect) {
    match app.input_mode {
        InputMode::Search => {
            let line = Line::from(vec![
                Span::styled(" /", theme::SEARCH_PROMPT_STYLE),
                Span::styled(&app.search_input, theme::SEARCH_INPUT_STYLE),
            ]);
            let paragraph = Paragraph::new(line);
            f.render_widget(paragraph, area);

            // Place cursor
            let cursor_x = area.x + 2 + app.search_cursor as u16;
            f.set_cursor_position((cursor_x, area.y));
        }
        InputMode::Normal => {
            let hint = Paragraph::new(Line::from(vec![
                Span::styled(" /", theme::SEARCH_PROMPT_STYLE),
                Span::styled(" Search blocks, txs, addresses...", theme::SUGGESTION_STYLE),
            ]));
            f.render_widget(hint, area);
        }
    }
}

/// Draw the suggestion dropdown as a floating overlay (call this LAST, after all other widgets).
pub fn draw_dropdown(f: &mut Frame, app: &App, search_bar_area: Rect) {
    if app.input_mode != InputMode::Search || app.search_suggestions.is_empty() {
        return;
    }

    let max_visible = 10usize;
    let total = app.search_suggestions.len();
    let visible = total.min(max_visible);
    let dropdown_height = visible as u16;
    let dropdown_area = Rect {
        x: search_bar_area.x + 1,
        y: search_bar_area.y + 1,
        width: search_bar_area.width.min(60).saturating_sub(1),
        height: dropdown_height + 2,
    };

    // Only draw if it fits on screen
    if dropdown_area.y + dropdown_area.height > f.area().height {
        return;
    }

    // Clear the area under the dropdown first
    f.render_widget(Clear, dropdown_area);

    // Compute the visible window that keeps search_selected in view
    let scroll_offset = if app.search_selected >= max_visible {
        app.search_selected - max_visible + 1
    } else {
        0
    };

    let items: Vec<ListItem> = app
        .search_suggestions
        .iter()
        .enumerate()
        .skip(scroll_offset)
        .take(max_visible)
        .map(|(i, result)| {
            let style = if i == app.search_selected {
                theme::SELECTED_STYLE
            } else {
                theme::NORMAL_STYLE
            };
            let prefix = if result.is_user { "* " } else { "  " };
            ListItem::new(Line::from(Span::styled(
                format!("{}{}", prefix, result.display),
                style,
            )))
        })
        .collect();

    let title = if total > max_visible {
        format!(
            " {}-{} of {} results ",
            scroll_offset + 1,
            (scroll_offset + visible).min(total),
            total
        )
    } else {
        format!(" {} results ", total)
    };

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::BORDER_FOCUSED_STYLE)
            .title(Span::styled(title, theme::SUGGESTION_STYLE)),
    );
    f.render_widget(list, dropdown_area);
}

/// Legacy single-call draw (for views that don't need overlay separation).
pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    draw_input(f, app, area);
    draw_dropdown(f, app, area);
}
