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

    let area = centered_rect(66, 30, f.area());
    f.render_widget(Clear, area);

    let lines = vec![
        Line::from(Span::styled(" Navigation", theme::TITLE_STYLE)),
        Line::from("   j/k, Up/Down     Move up/down"),
        Line::from("   h/l, Left/Right  Back / Drill in"),
        Line::from("   Enter            Select / Open"),
        Line::from("   g/G              Jump to top/bottom"),
        Line::from("   Esc, Ctrl+O      Go back   (] = forward in history)"),
        Line::from("   q                Home (or quit if already at home)"),
        Line::from(""),
        Line::from(Span::styled(" Search", theme::TITLE_STYLE)),
        Line::from("   /                Open search; Tab fills, Enter submits"),
        Line::from(""),
        Line::from(Span::styled(" Scrolling & axes", theme::TITLE_STYLE)),
        Line::from("   Ctrl+U / PgUp    Page up (list, or active tab body)"),
        Line::from("   Ctrl+D / PgDn    Page down"),
        Line::from("   Ctrl+P / Ctrl+N  Up / down axis (block or tx, wraps)"),
        Line::from("   n / N            Next / prev tx by same sender (nonce)"),
        Line::from("   Tab / Shift+Tab  Cycle tabs (TxDetail / AddressInfo)"),
        Line::from("   r                Refresh (Blocks / Address / Class)"),
        Line::from(""),
        Line::from(Span::styled(" Transaction Detail", theme::TITLE_STYLE)),
        Line::from("   Tabs: Events / Calls / Transfers / Trace / Privacy*"),
        Line::from("   c / d            Toggle raw / ABI-decoded calldata"),
        Line::from("   o                Toggle outside-execution intent"),
        Line::from("   e                Expand all (hashes, structs, OE)"),
        Line::from("   * Privacy tab shows only for Privacy-Pool txs"),
        Line::from(""),
        Line::from(Span::styled(" Address Info / Class Info", theme::TITLE_STYLE)),
        Line::from("   Tabs (Address): Txs / MetaTxs / Calls / Balances /"),
        Line::from("                   Events / ClassHistory"),
        Line::from("   a                ClassInfo: toggle full ABI pane"),
        Line::from(""),
        Line::from(Span::styled(" Visual Mode (v)", theme::TITLE_STYLE)),
        Line::from("   v                Enter; works in Tx/Block/Address/Class"),
        Line::from("   j/k, Enter, Esc  Step, open, exit"),
        Line::from("   Tab / c / d / o  Still work inside TxDetail visual mode"),
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
            " /search  j/k scroll  PgUp/PgDn page  Ctrl+P/N block up/down  v visual  Enter open  h back  ? help".into()
        }
        crate::app::state::View::TxDetail => {
            " /search  j/k scroll  PgUp/PgDn page  Tab switch  Ctrl+P/N tx up/down  n/N nonce  v visual  c/d/o calldata/intent  e expand  h back  ? help"
                .into()
        }
        crate::app::state::View::AddressInfo => {
            " /search  j/k scroll  Tab switch  Enter open  v visual  r refresh  h back  ? help".into()
        }
        crate::app::state::View::ClassInfo => {
            " /search  j/k scroll  v visual  a abi  Enter navigate  r refresh  h back  ? help".into()
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
