use ratatui::widgets::ListState;

/// A generic scrollable list with selection state.
pub struct StatefulList<T> {
    pub state: ListState,
    pub items: Vec<T>,
}

impl<T> StatefulList<T> {
    pub fn new() -> Self {
        Self {
            state: ListState::default(),
            items: Vec::new(),
        }
    }

    pub fn with_items(items: Vec<T>) -> Self {
        Self {
            state: ListState::default(),
            items,
        }
    }

    pub fn next(&mut self) {
        if self.items.is_empty() {
            return;
        }
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    i // stay at bottom
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn previous(&mut self) {
        if self.items.is_empty() {
            return;
        }
        let i = match self.state.selected() {
            Some(i) => i.saturating_sub(1),
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn select_first(&mut self) {
        if !self.items.is_empty() {
            self.state.select(Some(0));
        }
    }

    pub fn select_last(&mut self) {
        if !self.items.is_empty() {
            self.state.select(Some(self.items.len() - 1));
        }
    }

    /// Move the selection by `delta`, clamped to the list bounds.
    /// Use positive delta to scroll down, negative to scroll up.
    pub fn scroll_by(&mut self, delta: i64) {
        if self.items.is_empty() {
            return;
        }
        let cur = self.state.selected().unwrap_or(0) as i64;
        let max = self.items.len() as i64 - 1;
        let next = (cur + delta).clamp(0, max) as usize;
        self.state.select(Some(next));
    }

    pub fn selected_item(&self) -> Option<&T> {
        self.state.selected().and_then(|i| self.items.get(i))
    }

    /// Returns true if the selection is at (or near) the last item.
    pub fn is_near_bottom(&self, threshold: usize) -> bool {
        if self.items.is_empty() {
            return false;
        }
        match self.state.selected() {
            Some(i) => i + threshold >= self.items.len(),
            None => false,
        }
    }
}

impl<T> Default for StatefulList<T> {
    fn default() -> Self {
        Self::new()
    }
}
