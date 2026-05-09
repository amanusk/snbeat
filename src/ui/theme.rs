use ratatui::style::{Color, Modifier, Style};

// Block list
pub const BLOCK_NUMBER_STYLE: Style = Style::new().fg(Color::Cyan);
pub const BLOCK_HASH_STYLE: Style = Style::new().fg(Color::DarkGray);
pub const BLOCK_TX_COUNT_STYLE: Style = Style::new().fg(Color::Yellow);
pub const BLOCK_AGE_STYLE: Style = Style::new().fg(Color::Green);

// Transaction
pub const TX_HASH_STYLE: Style = Style::new().fg(Color::Cyan);
pub const TX_TYPE_INVOKE: Style = Style::new().fg(Color::Green);
pub const TX_TYPE_DECLARE: Style = Style::new().fg(Color::Blue);
pub const TX_TYPE_DEPLOY: Style = Style::new().fg(Color::Magenta);
pub const TX_TYPE_L1HANDLER: Style = Style::new().fg(Color::Yellow);
pub const TX_FEE_STYLE: Style = Style::new().fg(Color::DarkGray);
pub const META_TX_STYLE: Style = Style::new()
    .fg(Color::LightMagenta)
    .add_modifier(Modifier::BOLD);
// Privacy tab + tag. Mirrors META_TX_STYLE structure (bold accent
// foreground) but in orange so users can distinguish at a glance:
// purple = meta-tx, orange = privacy. Uses Indexed(208) for broad
// 256-color terminal compatibility.
pub const PRIVACY_STYLE: Style = Style::new()
    .fg(Color::Indexed(208))
    .add_modifier(Modifier::BOLD);

// Status
pub const STATUS_OK: Style = Style::new().fg(Color::Green);
pub const STATUS_REVERTED: Style = Style::new().fg(Color::Red);
pub const STATUS_LOADING: Style = Style::new().fg(Color::Yellow);
pub const STATUS_ERROR: Style = Style::new().fg(Color::Red);

// Selection
pub const SELECTED_STYLE: Style = Style::new()
    .bg(Color::DarkGray)
    .add_modifier(Modifier::BOLD);
pub const NORMAL_STYLE: Style = Style::new();
/// Highlight style for the item under the visual-mode cursor in TxDetail.
pub const VISUAL_SELECTED_STYLE: Style = Style::new()
    .fg(Color::Black)
    .bg(Color::LightCyan)
    .add_modifier(Modifier::BOLD);

// Labels
pub const LABEL_STYLE: Style = Style::new()
    .fg(Color::LightYellow)
    .add_modifier(Modifier::BOLD);

// Search bar
pub const SEARCH_PROMPT_STYLE: Style = Style::new().fg(Color::Cyan);
pub const SEARCH_INPUT_STYLE: Style = Style::new().fg(Color::White);
pub const SUGGESTION_STYLE: Style = Style::new().fg(Color::DarkGray);
pub const SUGGESTION_SELECTED_STYLE: Style = Style::new().fg(Color::White).bg(Color::DarkGray);

// Header / title
pub const TITLE_STYLE: Style = Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD);

// Borders
pub const BORDER_STYLE: Style = Style::new().fg(Color::DarkGray);
pub const BORDER_FOCUSED_STYLE: Style = Style::new().fg(Color::Cyan);

// Per-address color palette for tx detail view.
// Slot 0 is reserved for the tx sender.
pub const ADDRESS_PALETTE: [Style; 8] = [
    Style::new().fg(Color::White).add_modifier(Modifier::BOLD),
    Style::new().fg(Color::LightMagenta),
    Style::new().fg(Color::LightGreen),
    Style::new().fg(Color::LightYellow),
    Style::new().fg(Color::LightBlue),
    Style::new().fg(Color::LightRed),
    Style::new().fg(Color::Magenta),
    Style::new().fg(Color::Blue),
];
