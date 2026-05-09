//! Stark-curve scalar mul: derive a public viewing key from a private one.
//!
//! Mirrors what the Privacy Pool's `register()` flow does on-chain when it
//! emits `ViewingKeySet { user_addr, public_key }` — `public_key = K * G`
//! where `K` is the user's private viewing key and `G` is the Stark-curve
//! generator. Used to validate that a user-supplied private viewing key
//! matches the on-chain registration.

use starknet_types_core::curve::AffinePoint;
use starknet_types_core::felt::Felt;

use super::types::SecretFelt;

/// Derive the public viewing key from a private viewing key.
///
/// Returns the x-coordinate of `private_key * G` on the Stark curve.
/// The on-chain `ViewingKeySet` event emits exactly this value as
/// `public_key`.
pub fn public_from_private(private_key: &SecretFelt) -> Felt {
    let generator = AffinePoint::generator();
    let public_point = &generator * **private_key;
    public_point.x()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity: zero scalar produces a defined point. We don't assert the
    /// exact value (it's curve-implementation defined), but the call must
    /// not panic.
    #[test]
    fn zero_scalar_does_not_panic() {
        let _ = public_from_private(&SecretFelt::new(Felt::ZERO));
    }

    /// Same private key → same public key.
    #[test]
    fn deterministic() {
        // Synthetic test scalar — never use a real viewing key in
        // committed tests.
        let k = SecretFelt::new(Felt::from(0xc0ffeeu64));
        let p1 = public_from_private(&k);
        let p2 = public_from_private(&k);
        assert_eq!(p1, p2);
    }

    /// Different private keys produce different public keys (sanity check
    /// that we're not accidentally reading a fixed point).
    #[test]
    fn different_scalars_yield_different_points() {
        let p_a = public_from_private(&SecretFelt::new(Felt::from(1u64)));
        let p_b = public_from_private(&SecretFelt::new(Felt::from(2u64)));
        assert_ne!(p_a, p_b);
    }
}
