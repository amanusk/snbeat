use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::app::App;
use crate::decode::events::{DecodedEvent, group_events_by_contract};
use crate::ui::theme;
use crate::ui::widgets::param_display;

/// Draw the event tree: events grouped by emitting contract, with decoded names.
pub fn draw(f: &mut Frame, app: &App, events: &[DecodedEvent], area: Rect) {
    let groups = group_events_by_contract(events);
    let mut lines = Vec::new();

    lines.push(Line::from(Span::styled(
        format!(" Events ({})", events.len()),
        theme::TITLE_STYLE,
    )));

    for (gi, group) in groups.iter().enumerate() {
        let is_last_group = gi == groups.len() - 1;
        let branch = if is_last_group { "└─" } else { "├─" };
        let continuation = if is_last_group { "   " } else { "│  " };

        // Contract header — show both user and global labels in detail
        let contract_label = app.format_address_full(&group.contract_address);
        lines.push(Line::from(vec![
            Span::styled(format!(" {branch} "), theme::BORDER_STYLE),
            Span::styled(contract_label, theme::LABEL_STYLE),
            Span::styled(
                format!("  ({} events)", group.events.len()),
                theme::SUGGESTION_STYLE,
            ),
        ]));

        // Events under this contract
        for (ei, event) in group.events.iter().enumerate() {
            let is_last_event = ei == group.events.len() - 1;
            let event_branch = if is_last_event { "└─" } else { "├─" };

            let event_display = format_event(event, app);
            lines.push(Line::from(vec![
                Span::styled(
                    format!(" {continuation}{event_branch} "),
                    theme::BORDER_STYLE,
                ),
                Span::raw(event_display),
            ]));
        }
    }

    if events.is_empty() {
        lines.push(Line::from(Span::styled(
            "   (no events)",
            theme::SUGGESTION_STYLE,
        )));
    }

    let widget = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_STYLE)
                .title(Span::styled(" Event Tree ", theme::TITLE_STYLE)),
        )
        .wrap(Wrap { trim: false });

    f.render_widget(widget, area);
}

/// Format a single decoded event into a display string.
fn format_event(event: &DecodedEvent, app: &App) -> String {
    let name = event.event_name.as_deref().unwrap_or("Unknown");

    if event.decoded_keys.is_empty() && event.decoded_data.is_empty() {
        return name.to_string();
    }

    let registry = app.search_engine.as_ref().map(|e| e.registry());
    let mut params = Vec::new();

    let format_addr = |a: &starknet::core::types::Felt| app.format_address(a);
    for p in &event.decoded_keys {
        params.push(param_display::format_param(
            p,
            &event.contract_address,
            registry,
            &format_addr,
        ));
    }
    for p in &event.decoded_data {
        params.push(param_display::format_param(
            p,
            &event.contract_address,
            registry,
            &format_addr,
        ));
    }

    format!("{}({})", name, params.join(", "))
}
