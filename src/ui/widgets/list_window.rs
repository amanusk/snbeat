use std::ops::Range;

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{List, ListState};

/// The rows ratatui's `List` would paint for a single-line-row list, so the
/// caller builds `ListItem`s for those alone instead of the whole list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListWindow {
    pub start: usize,
    pub end: usize,
    pub selected: Option<usize>,
}

impl ListWindow {
    /// Mirrors ratatui's offset rule (no scroll padding): keep the stored
    /// offset unless the selection falls outside the page.
    pub fn new(total: usize, selected: Option<usize>, offset: usize, height: usize) -> Self {
        if total == 0 || height == 0 {
            return Self {
                start: 0,
                end: 0,
                selected: None,
            };
        }
        let selected = selected.map(|s| s.min(total - 1));
        let mut start = offset.min(total - 1);
        if let Some(s) = selected {
            if s < start {
                start = s;
            } else if s >= start + height {
                start = s + 1 - height;
            }
        }
        Self {
            start,
            end: (start + height).min(total),
            selected,
        }
    }

    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }

    /// Render `list` (built from `range()` rows only) with the selection
    /// rebased to the window, then store absolute offset/selection in `state`.
    pub fn render(&self, f: &mut Frame, list: List, area: Rect, state: &mut ListState) {
        let mut local = ListState::default().with_selected(self.selected.map(|s| s - self.start));
        f.render_stateful_widget(list, area, &mut local);
        *state.offset_mut() = self.start;
        state.select(self.selected);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_list_or_zero_height_is_empty_window() {
        assert_eq!(ListWindow::new(0, Some(3), 5, 10).range(), 0..0);
        assert_eq!(ListWindow::new(50, Some(3), 5, 0).range(), 0..0);
    }

    #[test]
    fn keeps_offset_when_nothing_selected() {
        let w = ListWindow::new(100, None, 10, 20);
        assert_eq!((w.start, w.end, w.selected), (10, 30, None));
    }

    #[test]
    fn clamps_offset_to_last_row() {
        let w = ListWindow::new(5, None, 50, 20);
        assert_eq!((w.start, w.end), (4, 5));
    }

    #[test]
    fn selection_inside_page_keeps_offset() {
        let w = ListWindow::new(100, Some(15), 10, 20);
        assert_eq!((w.start, w.end, w.selected), (10, 30, Some(15)));
    }

    #[test]
    fn selection_above_page_scrolls_up() {
        let w = ListWindow::new(100, Some(3), 10, 20);
        assert_eq!((w.start, w.end, w.selected), (3, 23, Some(3)));
    }

    #[test]
    fn selection_below_page_scrolls_down_to_bottom_row() {
        let w = ListWindow::new(100, Some(40), 10, 20);
        assert_eq!((w.start, w.end, w.selected), (21, 41, Some(40)));
    }

    #[test]
    fn selection_past_end_clamps_to_last_row() {
        let w = ListWindow::new(10, Some(50), 0, 4);
        assert_eq!((w.start, w.end, w.selected), (6, 10, Some(9)));
    }

    #[test]
    fn short_tail_page_is_truncated() {
        let w = ListWindow::new(25, Some(24), 20, 10);
        assert_eq!((w.start, w.end), (20, 25));
    }
}
