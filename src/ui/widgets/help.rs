use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::app::App;
use crate::ui::theme;

/// Draw the help overlay (toggled with ?).
pub fn draw_help_overlay(f: &mut Frame, app: &App) {
    if !app.show_help {
        return;
    }

    let area = centered_rect(60, 22, f.area());
    f.render_widget(Clear, area);

    let lines = vec![
        Line::from(Span::styled(" Navigation", theme::TITLE_STYLE)),
        Line::from("   j/k, Up/Down     Move up/down in lists"),
        Line::from("   h/l, Left/Right  Back / Drill in"),
        Line::from("   Enter            Select / Open"),
        Line::from("   g/G              Jump to top/bottom"),
        Line::from("   Esc              Go back"),
        Line::from("   q                Quit (or go back)"),
        Line::from(""),
        Line::from(Span::styled(" Search", theme::TITLE_STYLE)),
        Line::from("   /                Open search bar"),
        Line::from("   j/k              Navigate suggestions"),
        Line::from("   Tab              Accept suggestion"),
        Line::from("   Enter            Submit search"),
        Line::from(""),
        Line::from(Span::styled(" Block/Tx Navigation", theme::TITLE_STYLE)),
        Line::from("   PgUp/Ctrl+U      Page-scroll up (list or tab body)"),
        Line::from("   PgDn/Ctrl+D      Page-scroll down (list or tab body)"),
        Line::from("   Ctrl+P / Ctrl+N  Up/down axis (block / tx, wraps)"),
        Line::from("   n / N            Down/up tx by nonce (same sender)"),
        Line::from("   Tab / Shift+Tab  Cycle tabs (TxDetail / AddressInfo)"),
        Line::from("   c/d              Raw calldata / decoded calldata"),
        Line::from("   o                Outside execution intent (meta tx)"),
        Line::from("   Ctrl+o / h / Esc  Go back one view"),
        Line::from("   ]                Forward in jump history"),
        Line::from(""),
        Line::from(Span::styled(" Address View", theme::TITLE_STYLE)),
        Line::from("   Tab              Switch tabs (Txs/Calls/Balances/Events)"),
        Line::from("   r                Refresh data"),
        Line::from(""),
        Line::from(Span::styled(
            "   ?  Close this help",
            theme::SUGGESTION_STYLE,
        )),
    ];

    let help = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::BORDER_FOCUSED_STYLE)
                .title(Span::styled(" Help ", theme::TITLE_STYLE)),
        )
        .wrap(Wrap { trim: false });

    f.render_widget(help, area);
}

/// Get a hint string for the current view.
pub fn hint_for_view(app: &App) -> String {
    match app.current_view() {
        crate::app::state::View::Blocks => {
            " /search  j/k scroll  Enter open  r refresh  ? help".into()
        }
        crate::app::state::View::BlockDetail => {
            " /search  j/k scroll  PgUp/PgDn page txs  Ctrl+P/N block up/down  Enter open  h back  ? help".into()
        }
        crate::app::state::View::TxDetail => {
            " /search  h back  j/k scroll  PgUp/PgDn page  Tab switch  Ctrl+P/N tx up/down  n/N nonce down/up  v visual  c/d calldata  o intent  ? help"
                .into()
        }
        crate::app::state::View::AddressInfo => {
            " /search  j/k scroll  Tab switch  Enter open  r refresh  h back  ? help".into()
        }
        crate::app::state::View::ClassInfo => {
            " /search  j/k scroll  v visual  Enter navigate  r refresh  h back  ? help".into()
        }
    }
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect {
        x,
        y,
        width: width.min(area.width),
        height: height.min(area.height),
    }
}
