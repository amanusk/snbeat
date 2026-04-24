//! Bloom filter implementation matching pathfinder's event_filters table format.
//!
//! Each row in `event_filters` covers 8192 blocks. The bitmap is a 2D matrix:
//! 16384 rows (one per bloom bit) x 1024 bytes (8192 blocks / 8 bits).
//! Compressed with zstd (no dictionary).

use std::hash::{Hash, Hasher};
use std::sync::LazyLock;

use siphasher::sip::SipHasher13;

/// Number of blocks covered by each aggregate bloom filter entry.
pub const BLOCK_RANGE_LEN: u64 = 8192;

/// Number of bytes per row (one bit per block in the range).
const BLOCK_RANGE_BYTES: usize = (BLOCK_RANGE_LEN as usize) / 8; // 1024

/// Number of bits in the per-block bloom filter.
const BITVEC_BITS: usize = 16_384;

/// Number of hash functions.
const K_NUM: usize = 12;

/// Fixed seed used by pathfinder for SipHash initialization.
const SEED: [u8; 32] = [
    0xef, 0x51, 0x88, 0x74, 0xef, 0x08, 0x3d, 0xf6, 0x7d, 0x7a, 0x93, 0xb7, 0xb3, 0x13, 0x1f, 0x87,
    0xd3, 0x26, 0xbd, 0x49, 0xc7, 0x18, 0xcc, 0xe5, 0xd7, 0xe8, 0xa0, 0xdb, 0xea, 0x80, 0x67, 0x52,
];

static SIPS: LazyLock<[SipHasher13; 2]> = LazyLock::new(|| {
    let k1 = u64::from_le_bytes(SEED[0..8].try_into().unwrap());
    let k2 = u64::from_le_bytes(SEED[8..16].try_into().unwrap());
    let k3 = u64::from_le_bytes(SEED[16..24].try_into().unwrap());
    let k4 = u64::from_le_bytes(SEED[24..32].try_into().unwrap());
    [
        SipHasher13::new_with_keys(k1, k2),
        SipHasher13::new_with_keys(k3, k4),
    ]
});

/// Compute the bloom bit index for a key at hash function index k_i.
fn bloom_hash(hashes: &mut [u64; 2], item: &[u8; 32], k_i: usize) -> u64 {
    if k_i < 2 {
        let mut sip = SIPS[k_i];
        // pathfinder's Felt derives Hash, which for [u8; 32] does hash_slice(&self.0)
        item.hash(&mut sip);
        let hash = sip.finish();
        hashes[k_i] = hash;
        hash
    } else {
        hashes[0].wrapping_add((k_i as u64).wrapping_mul(hashes[1])) % 0xFFFF_FFFF_FFFF_FFC5u64
    }
}

/// Compute the K_NUM bit indices for a given 32-byte key.
fn indices_for_key(key: &[u8; 32]) -> [usize; K_NUM] {
    let mut indices = [0usize; K_NUM];
    let mut hashes = [0u64; 2];
    for (k_i, slot) in indices.iter_mut().enumerate() {
        *slot = bloom_hash(&mut hashes, key, k_i) as usize % BITVEC_BITS;
    }
    indices
}

/// An aggregate bloom filter covering a range of blocks.
pub struct AggregateBloom {
    /// Decompressed bitmap: BITVEC_BITS rows x BLOCK_RANGE_BYTES columns.
    bitmap: Vec<u8>,
    pub from_block: u64,
    pub to_block: u64,
}

impl AggregateBloom {
    /// Load from a compressed bitmap blob (as stored in the event_filters table).
    pub fn from_compressed(from_block: u64, to_block: u64, compressed: &[u8]) -> Self {
        let expected = BITVEC_BITS * BLOCK_RANGE_BYTES;
        let bitmap =
            zstd::bulk::decompress(compressed, expected * 2).expect("Decompressing bloom bitmap");
        assert_eq!(bitmap.len(), expected, "bloom bitmap size mismatch");
        Self {
            bitmap,
            from_block,
            to_block,
        }
    }

    /// Check which blocks in this range might contain events from the given
    /// contract address. Returns block numbers (absolute, not offsets).
    pub fn blocks_for_address(&self, address: &[u8; 32]) -> Vec<u64> {
        let indices = indices_for_key(address);

        // Start with all bits set (all blocks match), then AND each row
        let mut result = vec![0xFFu8; BLOCK_RANGE_BYTES];

        for row_idx in indices {
            let row_start = row_idx * BLOCK_RANGE_BYTES;
            for (i, byte) in result.iter_mut().enumerate() {
                *byte &= self.bitmap[row_start + i];
            }
        }

        // Convert set bits to block numbers
        let mut blocks = Vec::new();
        for (byte_idx, &byte) in result.iter().enumerate() {
            if byte == 0 {
                continue;
            }
            for bit_idx in 0..8 {
                if byte & (1 << (7 - bit_idx)) != 0 {
                    let offset = byte_idx * 8 + bit_idx;
                    let block = self.from_block + offset as u64;
                    if block <= self.to_block {
                        blocks.push(block);
                    }
                }
            }
        }
        blocks
    }
}
