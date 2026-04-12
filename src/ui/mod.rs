pub mod theme;
pub mod views;
pub mod widgets;

use ratatui::Frame;

use crate::app::App;
use crate::app::state::View;

/// Top-level draw function — dispatches to the active view.
pub fn draw(f: &mut Frame, app: &mut App) {
    match app.current_view() {
        View::Blocks => views::blocks::draw(f, app),
        View::BlockDetail => views::block_detail::draw(f, app),
        View::TxDetail => views::tx_detail::draw(f, app),
        View::AddressInfo => views::address_info::draw(f, app),
        View::ClassInfo => views::class_info::draw(f, app),
    }

    // Help overlay (rendered last, on top of everything)
    widgets::help::draw_help_overlay(f, app);
}
