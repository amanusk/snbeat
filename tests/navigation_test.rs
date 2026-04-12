use starknet::core::types::Felt;
use tokio::sync::mpsc;

use snbeat::app::App;
use snbeat::app::actions::Action;
use snbeat::app::state::{NavTarget, View};

fn make_app() -> (App, mpsc::UnboundedReceiver<Action>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (App::new(tx), rx)
}

#[test]
fn navigate_to_block_pushes_view_and_returns_fetch() {
    let (mut app, _rx) = make_app();
    assert_eq!(app.current_view(), View::Blocks);

    let action = app.navigate_to(NavTarget::Block(42));
    assert!(matches!(
        action,
        Some(Action::FetchBlockDetail { number: 42 })
    ));
    assert_eq!(app.current_view(), View::BlockDetail);
    assert!(app.is_loading);
    assert!(app.block_detail.block.is_none());
}

#[test]
fn navigate_to_transaction_pushes_view_and_returns_fetch() {
    let (mut app, _rx) = make_app();
    let hash = Felt::from(0xdeadbeefu64);

    let action = app.navigate_to(NavTarget::Transaction(hash));
    assert!(matches!(action, Some(Action::FetchTransaction { hash: h }) if h == hash));
    assert_eq!(app.current_view(), View::TxDetail);
    assert!(app.is_loading);
    assert!(app.tx_detail.transaction.is_none());
}

#[test]
fn navigate_to_address_returns_fetch_action() {
    let (mut app, _rx) = make_app();
    let addr = Felt::from(0x1234u64);

    let action = app.navigate_to(NavTarget::Address(addr));
    assert!(matches!(action, Some(Action::FetchAddressInfo { address }) if address == addr));
    assert!(app.is_loading);
}

#[test]
fn navigate_to_class_returns_fetch_action() {
    let (mut app, _rx) = make_app();
    let ch = Felt::from(0xababu64);

    let action = app.navigate_to(NavTarget::ClassHash(ch));
    assert!(matches!(action, Some(Action::FetchClassInfo { class_hash }) if class_hash == ch));
    assert!(app.is_loading);
}

#[test]
fn navigate_clears_visual_modes() {
    let (mut app, _rx) = make_app();
    app.tx_detail.visual_mode = true;
    app.block_detail.visual_mode = true;
    app.class.visual_mode = true;
    app.address.visual_mode = true;

    app.navigate_to(NavTarget::Block(1));

    assert!(!app.tx_detail.visual_mode);
    assert!(!app.block_detail.visual_mode);
    assert!(!app.class.visual_mode);
    assert!(!app.address.visual_mode);
}

#[test]
fn navigate_forward_replays_history() {
    let (mut app, _rx) = make_app();
    app.forward_history.push(snbeat::app::NavEntry::Block(5));

    let action = app.navigate_forward();
    assert!(matches!(
        action,
        Some(Action::FetchBlockDetail { number: 5 })
    ));
    assert!(app.forward_history.is_empty());
}

#[test]
fn navigate_forward_empty_returns_none() {
    let (mut app, _rx) = make_app();
    assert!(app.navigate_forward().is_none());
}

#[test]
fn go_to_root_or_quit_from_nested_view() {
    let (mut app, _rx) = make_app();
    app.push_view(View::BlockDetail);
    app.push_view(View::TxDetail);
    assert_eq!(app.view_stack.len(), 3);

    app.go_to_root_or_quit();
    assert_eq!(app.current_view(), View::Blocks);
    assert!(!app.should_quit);
}

#[test]
fn go_to_root_or_quit_at_root_quits() {
    let (mut app, _rx) = make_app();
    assert_eq!(app.current_view(), View::Blocks);

    app.go_to_root_or_quit();
    assert!(app.should_quit);
}

#[test]
fn nav_target_from_nav_item_converts_correctly() {
    use snbeat::app::state::TxNavItem;

    let addr = Felt::from(0x1u64);
    assert_eq!(
        NavTarget::from_nav_item(&TxNavItem::Address(addr)),
        NavTarget::Address(addr)
    );
    assert_eq!(
        NavTarget::from_nav_item(&TxNavItem::Block(99)),
        NavTarget::Block(99)
    );

    let hash = Felt::from(0xau64);
    assert_eq!(
        NavTarget::from_nav_item(&TxNavItem::Transaction(hash)),
        NavTarget::Transaction(hash)
    );

    let ch = Felt::from(0xbu64);
    assert_eq!(
        NavTarget::from_nav_item(&TxNavItem::ClassHash(ch)),
        NavTarget::ClassHash(ch)
    );
}
