// geister_rank_triplet.cxx
//   Rank/unrank for LR-canonical 3-set Geister observation domains.
//
// Board:
//   - 6x6 board embedded in 8x8 bitboard (same layout as geister_rank.cxx).
//   - Valid squares are (rank=1..6, file=1..6) in 0..63 indexing.
//
// Ranking model:
//   - We rank 3 labeled, disjoint sets (A, B, C) on the 6x6 board.
//   - We quotient by LR-mirror symmetry (canonical representative under left-right mirror).

module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <utility>

#include <immintrin.h>

export module geister_rank_triplet;

// This repository only needs counts up to 8 for the 3-set observation ranker.
export inline constexpr int kTripletRankMaxCount = 8;

// Number of LR-canonical states for given (popA, popB, popC).
export [[nodiscard]] std::uint64_t states_for_counts(
	std::uint8_t pop_a,
	std::uint8_t pop_b,
	std::uint8_t pop_c) noexcept;

// Rank a LR-canonical representative for three disjoint labeled sets on the 6x6 board.
export [[nodiscard]] std::uint64_t rank_triplet_canon(
	std::uint64_t A,
	std::uint64_t B,
	std::uint64_t C) noexcept;

// Unrank a LR-canonical representative.
export void unrank_triplet_canon(
	std::uint64_t number,
	std::uint8_t pop_a,
	std::uint8_t pop_b,
	std::uint8_t pop_c,
	std::uint64_t& A,
	std::uint64_t& B,
	std::uint64_t& C) noexcept;

module:private;

namespace {

constexpr int kBoardN = 36;
constexpr int kHalfN = 18;
constexpr int kMaxCount = kTripletRankMaxCount;
constexpr int kRadix = kMaxCount + 1;         // 0..8
constexpr int kSplitDomain = kRadix * kRadix * kRadix;

// -----------------------------------------------------------------------------
//  Combinations table
// -----------------------------------------------------------------------------
consteval auto make_comb_table()
	-> std::array<std::array<std::uint64_t, kBoardN + 1>, kBoardN + 1>
{
	std::array<std::array<std::uint64_t, kBoardN + 1>, kBoardN + 1> C{};
	for (int n = 0; n <= kBoardN; ++n) {
		C[n][0] = 1;
		for (int k = 1; k <= n; ++k) {
			if (k == n) {
				C[n][k] = 1;
			}
			else {
				C[n][k] = C[n - 1][k - 1] + C[n - 1][k];
			}
		}
	}
	return C;
}

inline constexpr auto COMB = make_comb_table();

// -----------------------------------------------------------------------------
//  Board masks (6x6 embedded in 8x8)
// -----------------------------------------------------------------------------
consteval std::uint64_t make_mask_cols(int col_first, int col_last) {
	std::uint64_t bb = 0;
	for (int r = 1; r <= 6; ++r) {
		for (int c = col_first; c <= col_last; ++c) {
			const int sq = 8 * r + c;
			bb |= (1ULL << sq);
		}
	}
	return bb;
}

inline constexpr std::uint64_t bb_board = make_mask_cols(1, 6);
inline constexpr std::uint64_t bb_left  = make_mask_cols(1, 3);

static_assert(std::popcount(bb_board) == kBoardN);
static_assert(std::popcount(bb_left) == kHalfN);
static_assert((bb_left & bb_board) == bb_left);

// -----------------------------------------------------------------------------
//  LR mirror on embedded 8x8: reverse bits within each byte
// -----------------------------------------------------------------------------
consteval auto make_rev8_table() -> std::array<std::uint8_t, 256> {
	std::array<std::uint8_t, 256> t{};
	for (int i = 0; i < 256; ++i) {
		std::uint8_t x = static_cast<std::uint8_t>(i);
		std::uint8_t r = 0;
		for (int b = 0; b < 8; ++b) {
			r = static_cast<std::uint8_t>((r << 1) | (x & 1));
			x >>= 1;
		}
		t[static_cast<std::size_t>(i)] = r;
	}
	return t;
}

inline constexpr auto REV8 = make_rev8_table();

[[nodiscard]] inline std::uint64_t mirror_lr_u64(std::uint64_t x) noexcept {
	// reverse bits within each byte
	const std::uint64_t b0 = static_cast<std::uint64_t>(REV8[(x >>  0) & 0xFF]) <<  0;
	const std::uint64_t b1 = static_cast<std::uint64_t>(REV8[(x >>  8) & 0xFF]) <<  8;
	const std::uint64_t b2 = static_cast<std::uint64_t>(REV8[(x >> 16) & 0xFF]) << 16;
	const std::uint64_t b3 = static_cast<std::uint64_t>(REV8[(x >> 24) & 0xFF]) << 24;
	const std::uint64_t b4 = static_cast<std::uint64_t>(REV8[(x >> 32) & 0xFF]) << 32;
	const std::uint64_t b5 = static_cast<std::uint64_t>(REV8[(x >> 40) & 0xFF]) << 40;
	const std::uint64_t b6 = static_cast<std::uint64_t>(REV8[(x >> 48) & 0xFF]) << 48;
	const std::uint64_t b7 = static_cast<std::uint64_t>(REV8[(x >> 56) & 0xFF]) << 56;
	return b0 | b1 | b2 | b3 | b4 | b5 | b6 | b7;
}

// -----------------------------------------------------------------------------
//  Helpers
// -----------------------------------------------------------------------------
[[nodiscard]] constexpr int split_index9(int a, int b, int c) noexcept {
	return (a * kRadix + b) * kRadix + c;
}

[[nodiscard]] inline int lex_cmp3(
	int a1, int b1, int c1,
	int a2, int b2, int c2) noexcept
{
	if (a1 != a2) return (a1 < a2) ? -1 : 1;
	if (b1 != b2) return (b1 < b2) ? -1 : 1;
	if (c1 != c2) return (c1 < c2) ? -1 : 1;
	return 0;
}

// Count ways to place (A,B,C) on HALF board (18 squares) disjointly.
[[nodiscard]] inline std::uint64_t ways_half_counts3(int a, int b, int c) noexcept {
	assert(a >= 0 && b >= 0 && c >= 0);
	if (a + b + c > kHalfN) return 0;
	return COMB[kHalfN][a]
		* COMB[kHalfN - a][b]
		* COMB[kHalfN - a - b][c];
}

// Full states count for (A,B,C) on 36 squares, disjoint.
[[nodiscard]] inline std::uint64_t full_total_states_for_counts3(int a, int b, int c) noexcept {
	assert(a >= 0 && b >= 0 && c >= 0);
	if (a + b + c > kBoardN) return 0;
	return COMB[kBoardN][a]
		* COMB[kBoardN - a][b]
		* COMB[kBoardN - a - b][c];
}

// Number of states fixed by LR mirror.
[[nodiscard]] inline std::uint64_t fixed_by_mirror_states3(int a, int b, int c) noexcept {
	if ((a & 1) || (b & 1) || (c & 1)) return 0;
	return ways_half_counts3(a / 2, b / 2, c / 2);
}

// Burnside for group {id, mirror}.
[[nodiscard]] inline std::uint64_t canonical_total_states_for_counts3(int a, int b, int c) noexcept {
	const std::uint64_t full = full_total_states_for_counts3(a, b, c);
	const std::uint64_t fix = fixed_by_mirror_states3(a, b, c);
	assert(((full + fix) & 1) == 0);
	return (full + fix) / 2;
}

// -----------------------------------------------------------------------------
//  Colex ranking for bit patterns of size P with S set bits
// -----------------------------------------------------------------------------
[[nodiscard]] inline std::uint64_t rank_patterns_colex(std::uint64_t x, [[maybe_unused]] int P, int S) noexcept {
	assert(P >= 0 && P <= kHalfN);
	assert(S >= 0 && S <= P);
	if (S == 0) return 0;

	std::uint64_t r = 0;
	int i = 1;
	while (x != 0) {
		const int p = std::countr_zero(x);
		r += COMB[p][i];
		++i;
		x &= (x - 1);
	}
	assert(i == S + 1);
	return r;
}

[[nodiscard]] inline std::uint64_t unrank_patterns_colex(std::uint64_t r, int P, int S) noexcept {
	assert(P >= 0 && P <= kHalfN);
	assert(S >= 0 && S <= P);
	assert(r < COMB[P][S]);
	if (S == 0) return 0;

	std::uint64_t x = 0;
	int p = P;
	for (int i = S; i >= 1; --i) {
		// find largest p in [i-1, p-1] with C[p][i] <= r
		int lo = i - 1;
		int hi = p;
		while (lo + 1 < hi) {
			const int mid = (lo + hi) >> 1;
			if (COMB[mid][i] <= r) lo = mid; else hi = mid;
		}
		p = lo;
		x |= (1ULL << p);
		r -= COMB[p][i];
	}
	return x;
}

// -----------------------------------------------------------------------------
//  Half-board ranking/unranking for three disjoint sets
// -----------------------------------------------------------------------------
[[nodiscard]] inline std::uint64_t rank_half3(
	std::uint64_t A,
	std::uint64_t B,
	std::uint64_t C,
	std::uint64_t mask18) noexcept
{
	assert((mask18 & ~bb_board) == 0);
	assert(std::popcount(mask18) == kHalfN);
	assert((A & ~mask18) == 0);
	assert((B & ~mask18) == 0);
	assert((C & ~mask18) == 0);
	assert((A & B) == 0);
	assert((A & C) == 0);
	assert((B & C) == 0);

	const int a = std::popcount(A);
	const int b = std::popcount(B);
	const int c = std::popcount(C);

	std::uint64_t m = mask18;
	int pop_m = kHalfN;

	std::uint64_t r = rank_patterns_colex(_pext_u64(A, m), pop_m, a);
	pop_m -= a;
	m ^= A;

	r *= COMB[pop_m][b];
	r += rank_patterns_colex(_pext_u64(B, m), pop_m, b);
	pop_m -= b;
	m ^= B;

	r *= COMB[pop_m][c];
	r += rank_patterns_colex(_pext_u64(C, m), pop_m, c);
	return r;
}

inline void unrank_half3(
	std::uint64_t number,
	int a,
	int b,
	int c,
	std::uint64_t mask18,
	std::uint64_t& A,
	std::uint64_t& B,
	std::uint64_t& C) noexcept
{
	assert((mask18 & ~bb_board) == 0);
	assert(std::popcount(mask18) == kHalfN);
	assert(a >= 0 && b >= 0 && c >= 0);
	assert(a + b + c <= kHalfN);

	const int rem1 = kHalfN - a;
	const int rem2 = rem1 - b;
	const std::uint64_t radix_b = COMB[rem1][b];
	const std::uint64_t radix_c = COMB[rem2][c];
	[[maybe_unused]] const std::uint64_t total = COMB[kHalfN][a] * radix_b * radix_c;
	assert(number < total);

	std::uint64_t n = number;
	const std::uint64_t r_c = n % radix_c;
	n /= radix_c;
	const std::uint64_t r_b = n % radix_b;
	n /= radix_b;
	const std::uint64_t r_a = n;

	std::uint64_t m = mask18;
	int pop_m = kHalfN;

	const std::uint64_t comp_a = unrank_patterns_colex(r_a, pop_m, a);
	A = _pdep_u64(comp_a, m);
	m ^= A;
	pop_m -= a;

	const std::uint64_t comp_b = unrank_patterns_colex(r_b, pop_m, b);
	B = _pdep_u64(comp_b, m);
	m ^= B;
	pop_m -= b;

	const std::uint64_t comp_c = unrank_patterns_colex(r_c, pop_m, c);
	C = _pdep_u64(comp_c, m);
}

// -----------------------------------------------------------------------------
//  Canonical split metadata for 3 sets
// -----------------------------------------------------------------------------
struct CanonMeta3 {
	std::array<std::uint64_t, kSplitDomain> base{};   // base offset for each split
	std::array<std::uint64_t, kSplitDomain> block{};  // size for each split (0 if not canonical)
	std::array<std::uint64_t, kSplitDomain> WL{};     // ways for left-half
	std::array<std::uint64_t, kSplitDomain> WR{};     // ways for right-half
	std::array<std::uint64_t, kRadix> sum_a{};        // sum over bL,cL per aL
	std::array<std::uint64_t, kRadix * kRadix> sum_ab{}; // sum over cL per (aL,bL)
	std::uint64_t total = 0;
};

inline void fill_all_canon_meta(std::array<CanonMeta3, kSplitDomain>& out) noexcept {
	for (int a = 0; a <= kMaxCount; ++a) {
		for (int b = 0; b <= kMaxCount; ++b) {
			for (int c = 0; c <= kMaxCount; ++c) {
				CanonMeta3 meta{};
				const int key = split_index9(a, b, c);

				for (int aL = 0; aL <= a; ++aL) {
					for (int bL = 0; bL <= b; ++bL) {
						for (int cL = 0; cL <= c; ++cL) {
							const int aR = a - aL;
							const int bR = b - bL;
							const int cR = c - cL;
							const int sidx = split_index9(aL, bL, cL);

							const std::uint64_t wl = ways_half_counts3(aL, bL, cL);
							const std::uint64_t wr = ways_half_counts3(aR, bR, cR);
							meta.WL[static_cast<std::size_t>(sidx)] = wl;
							meta.WR[static_cast<std::size_t>(sidx)] = wr;

							std::uint64_t blk = 0;
							if (wl == 0 || wr == 0) {
								blk = 0;
							}
							else {
								const int cmp = lex_cmp3(aL, bL, cL, aR, bR, cR);
								if (cmp > 0) {
									blk = 0;
								}
								else if (cmp < 0) {
									blk = wl * wr;
								}
								else {
									blk = wl * (wl + 1) / 2;
								}
							}

							meta.block[static_cast<std::size_t>(sidx)] = blk;
							meta.sum_a[static_cast<std::size_t>(aL)] += blk;
							meta.sum_ab[static_cast<std::size_t>(aL * kRadix + bL)] += blk;
						}
					}
				}

				// base offsets in lex order over (aL,bL,cL)
				std::uint64_t pref = 0;
				for (int aL = 0; aL <= a; ++aL) {
					for (int bL = 0; bL <= b; ++bL) {
						for (int cL = 0; cL <= c; ++cL) {
							const int sidx = split_index9(aL, bL, cL);
							meta.base[static_cast<std::size_t>(sidx)] = pref;
							pref += meta.block[static_cast<std::size_t>(sidx)];
						}
					}
				}
				meta.total = pref;
				assert(meta.total == canonical_total_states_for_counts3(a, b, c));

				out[static_cast<std::size_t>(key)] = meta;
			}
		}
	}
}

[[nodiscard]] inline const std::array<CanonMeta3, kSplitDomain>& all_canon_meta() noexcept {
	struct Holder {
		std::array<CanonMeta3, kSplitDomain> meta{};
		Holder() noexcept { fill_all_canon_meta(meta); }
	};
	static const Holder holder;
	return holder.meta;
}

// -----------------------------------------------------------------------------
//  Triangular indexing for (i,j) with 0<=i<=j<M
// -----------------------------------------------------------------------------
[[nodiscard]] inline std::uint64_t tri_rank(std::uint64_t i, std::uint64_t j, std::uint64_t M) noexcept {
	assert(i <= j);
	assert(j < M);
	// offset(i) = i*(2*M - i + 1)/2
	const std::uint64_t off = i * (2 * M - i + 1) / 2;
	return off + (j - i);
}

inline void tri_unrank(std::uint64_t r, std::uint64_t M, std::uint64_t& i, std::uint64_t& j) noexcept {
	assert(M > 0);
	[[maybe_unused]] const std::uint64_t total_pairs = M * (M + 1) / 2;
	assert(r < total_pairs);

	// Find largest i such that offset(i) <= r,
	// offset(i) = i*(2*M - i + 1)/2
	std::uint64_t lo = 0, hi = M;
	while (lo + 1 < hi) {
		const std::uint64_t mid = (lo + hi) >> 1;
		const std::uint64_t off = mid * (2 * M - mid + 1) / 2;
		if (off <= r) lo = mid; else hi = mid;
	}
	i = lo;
	const std::uint64_t off_i = i * (2 * M - i + 1) / 2;
	j = i + (r - off_i);
	assert(i <= j && j < M);
}

} // namespace

// -----------------------------------------------------------------------------
//  Public API
// -----------------------------------------------------------------------------
std::uint64_t states_for_counts(std::uint8_t pop_a, std::uint8_t pop_b, std::uint8_t pop_c) noexcept {
	assert(pop_a <= kMaxCount);
	assert(pop_b <= kMaxCount);
	assert(pop_c <= kMaxCount);
	return canonical_total_states_for_counts3(pop_a, pop_b, pop_c);
}

std::uint64_t rank_triplet_canon(std::uint64_t A, std::uint64_t B, std::uint64_t C) noexcept {
	assert(((A | B | C) & ~bb_board) == 0);
	assert((A & B) == 0);
	assert((A & C) == 0);
	assert((B & C) == 0);

	int a = std::popcount(A);
	int b = std::popcount(B);
	int c = std::popcount(C);
	assert(a <= kMaxCount);
	assert(b <= kMaxCount);
	assert(c <= kMaxCount);

	const auto& meta = all_canon_meta()[static_cast<std::size_t>(split_index9(a, b, c))];
	assert(meta.total == canonical_total_states_for_counts3(a, b, c));

	// Left half directly.
	std::uint64_t AL = A & bb_left;
	std::uint64_t BL = B & bb_left;
	std::uint64_t CL = C & bb_left;

	// Right half mirrored into left.
	std::uint64_t AR = mirror_lr_u64(A) & bb_left;
	std::uint64_t BR = mirror_lr_u64(B) & bb_left;
	std::uint64_t CR = mirror_lr_u64(C) & bb_left;

	int aL = std::popcount(AL);
	int bL = std::popcount(BL);
	int cL = std::popcount(CL);
	int aR = a - aL;
	int bR = b - bL;
	int cR = c - cL;

	int cmp = lex_cmp3(aL, bL, cL, aR, bR, cR);

	std::uint64_t rL = rank_half3(AL, BL, CL, bb_left);
	std::uint64_t rR = rank_half3(AR, BR, CR, bb_left);

	// Canonicalize by (split counts) then (rank).
	if (cmp > 0) {
		std::swap(AL, AR);
		std::swap(BL, BR);
		std::swap(CL, CR);
		std::swap(aL, aR);
		std::swap(bL, bR);
		std::swap(cL, cR);
		std::swap(rL, rR);
		cmp = -cmp;
	}
	else if (cmp == 0 && rL > rR) {
		std::swap(rL, rR);
	}

	const int sidx = split_index9(aL, bL, cL);
	const std::uint64_t base = meta.base[static_cast<std::size_t>(sidx)];
	const std::uint64_t WL = meta.WL[static_cast<std::size_t>(sidx)];
	const std::uint64_t WR = meta.WR[static_cast<std::size_t>(sidx)];
	assert(WL > 0 && WR > 0);

	if (cmp < 0) {
		const std::uint64_t within = rL * WR + rR;
		return base + within;
	}

	// cmp == 0
	assert(WL == WR);
	assert(rL <= rR);
	const std::uint64_t within = tri_rank(rL, rR, WL);
	return base + within;
}

void unrank_triplet_canon(
	std::uint64_t number,
	std::uint8_t pop_a,
	std::uint8_t pop_b,
	std::uint8_t pop_c,
	std::uint64_t& A,
	std::uint64_t& B,
	std::uint64_t& C) noexcept
{
	const int a = pop_a;
	const int b = pop_b;
	const int c = pop_c;
	assert(a >= 0 && a <= kMaxCount);
	assert(b >= 0 && b <= kMaxCount);
	assert(c >= 0 && c <= kMaxCount);

	const auto& meta = all_canon_meta()[static_cast<std::size_t>(split_index9(a, b, c))];
	assert(number < meta.total);

	std::uint64_t r = number;

	int aL = 0;
	while (aL <= a && r >= meta.sum_a[static_cast<std::size_t>(aL)]) {
		r -= meta.sum_a[static_cast<std::size_t>(aL)];
		++aL;
	}
	assert(aL <= a);

	int bL = 0;
	while (bL <= b && r >= meta.sum_ab[static_cast<std::size_t>(aL * kRadix + bL)]) {
		r -= meta.sum_ab[static_cast<std::size_t>(aL * kRadix + bL)];
		++bL;
	}
	assert(bL <= b);

	int cL = 0;
	for (;; ++cL) {
		assert(cL <= c);
		const int sidx = split_index9(aL, bL, cL);
		const std::uint64_t blk = meta.block[static_cast<std::size_t>(sidx)];
		if (r < blk) break;
		r -= blk;
	}

	const int aR = a - aL;
	const int bR = b - bL;
	const int cR = c - cL;
	const int sidx = split_index9(aL, bL, cL);
	const std::uint64_t WL = meta.WL[static_cast<std::size_t>(sidx)];
	const std::uint64_t WR = meta.WR[static_cast<std::size_t>(sidx)];
	assert(WL > 0 && WR > 0);

	const int cmp = lex_cmp3(aL, bL, cL, aR, bR, cR);
	assert(cmp <= 0);

	std::uint64_t rankL = 0, rankR = 0;
	if (cmp < 0) {
		assert(r < WL * WR);
		rankL = r / WR;
		rankR = r % WR;
	}
	else {
		assert(WL == WR);
		tri_unrank(r, WL, rankL, rankR);
	}

	// decode halves on bb_left coordinates
	std::uint64_t AL = 0, BL = 0, CL = 0;
	std::uint64_t AR = 0, BR = 0, CR = 0;
	unrank_half3(rankL, aL, bL, cL, bb_left, AL, BL, CL);
	unrank_half3(rankR, aR, bR, cR, bb_left, AR, BR, CR);

	// build full board: right half is mirror of bb_left-coordinate patterns
	A = AL | mirror_lr_u64(AR);
	B = BL | mirror_lr_u64(BR);
	C = CL | mirror_lr_u64(CR);

	assert(((A | B | C) & ~bb_board) == 0);
	assert((A & B) == 0);
	assert((A & C) == 0);
	assert((B & C) == 0);
	assert(std::popcount(A) == a);
	assert(std::popcount(B) == b);
	assert(std::popcount(C) == c);
}
