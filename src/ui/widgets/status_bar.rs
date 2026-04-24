use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::app::state::SourceStatus;
use crate::ui::theme;
use crate::ui::widgets::help;

const SOURCE_LIVE: Style = Style::new().fg(Color::Green);
const SOURCE_FETCH_ERROR: Style = Style::new().fg(Color::Yellow);
const SOURCE_CONNECT_ERROR: Style = Style::new().fg(Color::Red);
const SOURCE_INACTIVE: Style = Style::new().fg(Color::DarkGray);

pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    // Error takes priority
    if let Some(err) = &app.error_message {
        let status = Paragraph::new(Line::from(Span::styled(
            format!(" {err}"),
            theme::STATUS_ERROR,
        )))
        .style(Style::default().bg(ratatui::style::Color::Black));
        f.render_widget(status, area);
        return;
    }

    let mut spans = Vec::new();

    spans.push(Span::styled(
        format!("#{} ", app.latest_block_number),
        theme::BLOCK_NUMBER_STYLE,
    ));

    // All data sources follow the same status model
    let ds = &app.data_sources;
    let sources: &[(&str, &SourceStatus)] = &[
        ("RPC", &ds.rpc),
        ("WS", &ds.ws),
        ("PF", &ds.pathfinder),
        ("Dune", &ds.dune),
        ("Voyager", &ds.voyager),
    ];

    // Track the first error to show after the indicators
    let mut first_error: Option<(&str, &str)> = None;

    for &(name, status) in sources {
        let style = match status {
            SourceStatus::Live => SOURCE_LIVE,
            SourceStatus::ConnectError(_) => SOURCE_CONNECT_ERROR,
            SourceStatus::FetchError(_) => SOURCE_FETCH_ERROR,
            SourceStatus::Off | SourceStatus::Configured => SOURCE_INACTIVE,
        };
        spans.push(Span::styled(name, style));
        spans.push(Span::raw(" "));

        // Capture first error for display
        if first_error.is_none()
            && let Some(msg) = status.error_msg()
        {
            first_error = Some((name, msg));
        }
    }
    spans.push(Span::raw("| "));

    // Show first source error (if any) before the hint
    if let Some((name, msg)) = first_error {
        spans.push(Span::styled(
            format!("{name}: {msg} "),
            SOURCE_CONNECT_ERROR,
        ));
        spans.push(Span::raw("| "));
    }

    // Hint for current view
    spans.push(Span::styled(
        help::hint_for_view(app),
        theme::SUGGESTION_STYLE,
    ));

    // Loading detail at the end so static elements don't shift
    if app.is_loading {
        let detail = app.loading_detail.as_deref().unwrap_or("Loading...");
        spans.push(Span::raw(" | "));
        spans.push(Span::styled(detail, theme::STATUS_LOADING));
    }

    // Per-query registry: render up to 2 active query labels joined with
    // " · " so parallel tab scans are both visible at once without
    // overflowing the status bar.
    if !app.active_queries.is_empty() {
        let joined = app
            .active_queries
            .labels()
            .take(2)
            .collect::<Vec<_>>()
            .join(" · ");
        spans.push(Span::raw(" | "));
        spans.push(Span::styled(joined, theme::STATUS_LOADING));
    }

    let status =
        Paragraph::new(Line::from(spans)).style(Style::default().bg(ratatui::style::Color::Black));
    f.render_widget(status, area);
}
