//! View-specific state structs, extracted from the monolithic `App` struct.
//! Each view has its own state struct with `clear()`, navigation helpers, etc.

pub mod address_info;
pub mod block_detail;
pub mod class_info;
pub mod tx_detail;

pub use address_info::AddressInfoState;
pub use block_detail::BlockDetailState;
pub use class_info::ClassInfoState;
pub use tx_detail::TxDetailState;
