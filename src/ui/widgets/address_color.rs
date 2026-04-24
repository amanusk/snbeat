use std::collections::HashMap;

use ratatui::style::Style;
use starknet::core::types::Felt;

use crate::ui::theme;

/// Assigns a stable palette color to each unique address seen in a tx view.
///
/// Registration order determines the slot (and thus the color). The caller
/// should register addresses in a consistent order so the sender always gets
/// slot 0 (LightCyan).
pub struct AddressColorMap {
    slots: HashMap<Felt, usize>,
    next_slot: usize,
}

impl Default for AddressColorMap {
    fn default() -> Self {
        Self::new()
    }
}

impl AddressColorMap {
    pub fn new() -> Self {
        Self {
            slots: HashMap::new(),
            next_slot: 0,
        }
    }

    /// Register an address and return its assigned Style.
    /// Idempotent: the same address always gets the same slot.
    pub fn register(&mut self, addr: Felt) -> Style {
        if !self.slots.contains_key(&addr) {
            self.slots.insert(addr, self.next_slot);
            self.next_slot += 1;
        }
        let slot = self.slots[&addr];
        theme::ADDRESS_PALETTE[slot % theme::ADDRESS_PALETTE.len()]
    }

    /// Look up an already-registered address.
    /// Returns `NORMAL_STYLE` for addresses not in the map.
    pub fn style_for(&self, addr: &Felt) -> Style {
        self.slots
            .get(addr)
            .map(|&s| theme::ADDRESS_PALETTE[s % theme::ADDRESS_PALETTE.len()])
            .unwrap_or(theme::NORMAL_STYLE)
    }
}
