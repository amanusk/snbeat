use std::collections::{HashMap, HashSet};

use ratatui::style::Style;
use starknet::core::types::Felt;

use crate::registry::AddressRegistry;
use crate::ui::theme;

/// Style for an address span at sites that already special-case known
/// addresses (block_detail's Txs list, address_info's Calls list). Order:
/// 1. `PRIVACY_STYLE` for any known address tagged `type = "Privacy"`
///    (overrides the generic label tint so the pool stands out wherever
///    it appears).
/// 2. `LABEL_STYLE` for any other registry-known address — including
///    runtime-fetched Voyager labels that don't have metadata yet.
/// 3. Whatever the color map returns (palette slot, or `NORMAL_STYLE`).
pub fn known_or_palette_style(
    addr: &Felt,
    registry: Option<&AddressRegistry>,
    color_map: &AddressColorMap,
) -> Style {
    if let Some(reg) = registry {
        // Privacy classification comes from the curated bundle and beats
        // any user-label override — a user labelling the pool as `[my-pool]`
        // shouldn't downgrade it to the generic LABEL_STYLE tint.
        if reg.is_privacy_address(addr) {
            return theme::PRIVACY_STYLE;
        }
        if reg.is_known(addr) {
            return theme::LABEL_STYLE;
        }
    }
    color_map.style_for(addr)
}

/// Assigns a stable palette color to each unique address seen in a tx view.
///
/// Registration order determines the slot (and thus the color). Palette
/// slots are all plain colors so the spot-the-repeat scan stays even —
/// no one slot draws extra attention.
///
/// Two override sets escape the palette entirely:
///
/// - `privacy_overrides` — addresses pinned to `theme::PRIVACY_STYLE` so
///   curated privacy contracts pop out wherever they appear.
/// - `sender_override` — at most one address per map, pinned to
///   `theme::TX_SENDER_STYLE` (bold white). The tx detail view sets this
///   on the tx initiator so "this is the signer" is visually distinct
///   from every other address on the page.
pub struct AddressColorMap {
    slots: HashMap<Felt, usize>,
    next_slot: usize,
    privacy_overrides: HashSet<Felt>,
    sender_override: Option<Felt>,
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
            privacy_overrides: HashSet::new(),
            sender_override: None,
        }
    }

    /// Mark these addresses as privacy-pool entities; their style is
    /// pinned to `PRIVACY_STYLE` regardless of palette slot. Idempotent.
    pub fn set_privacy_overrides<I: IntoIterator<Item = Felt>>(&mut self, addrs: I) {
        self.privacy_overrides.extend(addrs);
    }

    /// Mark this address as the "tx sender" anchor. It will always render
    /// in `TX_SENDER_STYLE` (bold white), shortcutting both palette and
    /// privacy assignments. Calling this twice replaces the previous
    /// sender. Intended for tx detail; views that show many senders side
    /// by side (block detail, calls tab) should leave this unset.
    pub fn set_sender(&mut self, addr: Felt) {
        self.sender_override = Some(addr);
    }

    /// Register an address and return its assigned Style.
    /// Idempotent: the same address always gets the same slot.
    pub fn register(&mut self, addr: Felt) -> Style {
        if self.sender_override == Some(addr) {
            return theme::TX_SENDER_STYLE;
        }
        if self.privacy_overrides.contains(&addr) {
            return theme::PRIVACY_STYLE;
        }
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
        if self.sender_override.as_ref() == Some(addr) {
            return theme::TX_SENDER_STYLE;
        }
        if self.privacy_overrides.contains(addr) {
            return theme::PRIVACY_STYLE;
        }
        self.slots
            .get(addr)
            .map(|&s| theme::ADDRESS_PALETTE[s % theme::ADDRESS_PALETTE.len()])
            .unwrap_or(theme::NORMAL_STYLE)
    }

    /// Number of distinct addresses that have been assigned a slot.
    pub fn slots_count(&self) -> usize {
        self.slots.len()
    }
}
