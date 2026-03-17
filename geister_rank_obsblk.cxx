// geister_rank_obsblk.cxx
//   Rank/unrank for a "perfect-information" Geister tablebase layout optimised for
//   practical probing under unknown opponent colors.
//
// Motivation (practical probing):
//   In real play, the opponent piece colors on-board are unknown, but the remaining
//   counts (ob/or) are known from captures/escapes. Therefore probing a single
//   observed position typically enumerates all opponent colorings consistent with
//   those counts.
//
// New layout ("observation block" / obsblk):
//   For fixed material counts (pb,pr,ob,or):
//     - Outer key   : LR-canonical rank of the 3-set observation
//           (A=my blue, B=my red, U=opponent pieces with unknown color)
//           where |U| = ob+or.
//     - Inner key   : rank of which |U|-subset of U is opponent-red (size=or).
//     - Linear index: outer * C(|U|, or) + inner.
//
//   This makes the C(|U|,or) queries for a single observed position land on a
//   contiguous block in the .bin file.
//
// Symmetry note:
//   Outer observation is LR-quotiented (canonical). Opponent coloring is *not*
//   additionally quotiented inside the block. Therefore, when the observation is
//   LR-symmetric, both mirror-related colorings are stored (duplicate values)
//   which is intended.

module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>

#include <immintrin.h> // _pext_u64 / _pdep_u64

export module geister_rank_obsblk;

import geister_rank_triplet;

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Opponent on-board piece count: k = ob+or, 2..8.
export [[nodiscard]] std::uint64_t obsblk_block_size(int pop_ob, int pop_or) noexcept;

// #entries in the obsblk domain for the given perfect-information material.
//   = states_for_counts(pb,pr,ob+or) * C(ob+or, or)
export [[nodiscard]] std::uint64_t obsblk_states_for_counts(
	int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept;

// Rank a perfect-information 4-bitboard position into the obsblk domain for
// its material counts (pb,pr,ob,or).
//
// Returns:
//   idx in [0, obsblk_states_for_counts(pb,pr,ob,or)).
export [[nodiscard]] std::uint64_t rank_geister_perfect_information_obsblk(
	std::uint64_t bb_player_blue,
	std::uint64_t bb_player_red,
	std::uint64_t bb_opponent_blue,
	std::uint64_t bb_opponent_red) noexcept;

// Unrank an obsblk index into LR-canonical bitboards.
//
// NOTE:
//   The returned (player,opponent) bitboards are in the same LR-canonical
//   observation coordinate system used by rank_geister_perfect_information_obsblk.
export void unrank_geister_perfect_information_obsblk(
	std::uint64_t number,
	int pop_pb, int pop_pr, int pop_ob, int pop_or,
	std::uint64_t& bb_player_blue,
	std::uint64_t& bb_player_red,
	std::uint64_t& bb_opponent_blue,
	std::uint64_t& bb_opponent_red) noexcept;

module:private;

namespace {

// 6x6 board embedded in 8x8 (same as other modules).
[[maybe_unused]] inline constexpr std::uint64_t bb_board = 0x007E'7E7E'7E7E'7E7E'00ULL;

// -----------------------------------------------------------------------------
// Small nCk table for n<=8 (enough for k=ob+or<=8).
// -----------------------------------------------------------------------------

constexpr int MAXN = 8;

consteval auto make_comb_table() {
	std::array<std::array<std::uint64_t, MAXN + 1>, MAXN + 1> C{};
	for (int n = 0; n <= MAXN; ++n) {
		C[n].fill(0);
		C[n][0] = 1;
		for (int k = 1; k <= MAXN; ++k) {
			if (k > n) {
				C[n][k] = 0;
			}
			else if (k == n) {
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
// LR mirror on embedded 8x8: reverse bits within each byte.
// (Same implementation style as geister_rank/geister_rank_triplet.)
// -----------------------------------------------------------------------------

consteval auto make_rev8_table() -> std::array<std::uint8_t, 256> {
	std::array<std::uint8_t, 256> t{};
	for (int x = 0; x < 256; ++x) {
		std::uint8_t v = static_cast<std::uint8_t>(x);
		std::uint8_t r = 0;
		for (int i = 0; i < 8; ++i) {
			r = static_cast<std::uint8_t>((r << 1) | (v & 1));
			v >>= 1;
		}
		t[static_cast<std::size_t>(x)] = r;
	}
	return t;
}

inline constexpr auto REV8 = make_rev8_table();

[[nodiscard]] inline std::uint64_t mirror_lr_u64(std::uint64_t x) noexcept {
	std::uint64_t y = 0;
	for (int b = 0; b < 8; ++b) {
		const std::uint8_t byte = static_cast<std::uint8_t>((x >> (b * 8)) & 0xFF);
		y |= (std::uint64_t)REV8[byte] << (b * 8);
	}
	return y;
}

// -----------------------------------------------------------------------------
// Colex ranking for k-bit patterns of weight S (S<=4, k<=8).
// -----------------------------------------------------------------------------

[[nodiscard]] inline std::uint64_t rank_patterns_colex(std::uint64_t x, [[maybe_unused]] int k, int S) noexcept {
	assert(0 <= k && k <= MAXN);
	assert(0 <= S && S <= 4);
	assert(S <= k);
	assert(std::popcount(x) == S);
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

[[nodiscard]] inline std::uint64_t unrank_patterns_colex(std::uint64_t r, int k, int S) noexcept {
	assert(0 <= k && k <= MAXN);
	assert(0 <= S && S <= 4);
	assert(S <= k);
	assert(r < COMB[k][S]);
	if (S == 0) return 0;

	std::uint64_t x = 0;
	int p = k;
	for (int i = S; i >= 1; --i) {
		// Find largest p' in [i-1, p-1] with C[p'][i] <= r.
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

[[nodiscard]] inline std::uint64_t comb_small(int n, int k) noexcept {
	assert(0 <= n && n <= MAXN);
	assert(0 <= k && k <= n);
	return COMB[n][k];
}

} // namespace

// -----------------------------------------------------------------------------
// Exported function definitions
// -----------------------------------------------------------------------------

std::uint64_t obsblk_block_size(int pop_ob, int pop_or) noexcept {
	assert(1 <= pop_ob && pop_ob <= 4);
	assert(1 <= pop_or && pop_or <= 4);
	const int k = pop_ob + pop_or;
	assert(2 <= k && k <= 8);
	return comb_small(k, pop_or);
}

std::uint64_t obsblk_states_for_counts(
	int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept
{
	assert(1 <= pop_pb && pop_pb <= 4);
	assert(1 <= pop_pr && pop_pr <= 4);
	assert(1 <= pop_ob && pop_ob <= 4);
	assert(1 <= pop_or && pop_or <= 4);
	const int k = pop_ob + pop_or;
	const std::uint64_t outer = states_for_counts(
		static_cast<std::uint8_t>(pop_pb),
		static_cast<std::uint8_t>(pop_pr),
		static_cast<std::uint8_t>(k));
	const std::uint64_t inner = comb_small(k, pop_or);
	return outer * inner;
}

std::uint64_t rank_geister_perfect_information_obsblk(
	std::uint64_t bb_player_blue,
	std::uint64_t bb_player_red,
	std::uint64_t bb_opponent_blue,
	std::uint64_t bb_opponent_red) noexcept
{
	// Basic invariants.
	assert(((bb_player_blue | bb_player_red | bb_opponent_blue | bb_opponent_red) & ~bb_board) == 0);
	assert((bb_player_blue & bb_player_red) == 0);
	assert((bb_player_blue & bb_opponent_blue) == 0);
	assert((bb_player_blue & bb_opponent_red) == 0);
	assert((bb_player_red & bb_opponent_blue) == 0);
	assert((bb_player_red & bb_opponent_red) == 0);
	assert((bb_opponent_blue & bb_opponent_red) == 0);

	const int pop_pb = std::popcount(bb_player_blue);
	const int pop_pr = std::popcount(bb_player_red);
	const int pop_ob = std::popcount(bb_opponent_blue);
	const int pop_or = std::popcount(bb_opponent_red);
	assert(1 <= pop_pb && pop_pb <= 4);
	assert(1 <= pop_pr && pop_pr <= 4);
	assert(1 <= pop_ob && pop_ob <= 4);
	assert(1 <= pop_or && pop_or <= 4);

	const std::uint64_t U = bb_opponent_blue | bb_opponent_red;
	const int k = pop_ob + pop_or;
	assert(std::popcount(U) == k);
	assert(2 <= k && k <= 8);

	// Outer rank is mirror-invariant, but inner depends on the chosen canonical orientation.
	const std::uint64_t outer = rank_triplet_canon(bb_player_blue, bb_player_red, U);
	const std::uint64_t inner_domain = comb_small(k, pop_or);

	// Recover the canonical observation representative to know whether we mirrored.
	std::uint64_t pb_can = 0, pr_can = 0, U_can = 0;
	unrank_triplet_canon(
		outer,
		static_cast<std::uint8_t>(pop_pb),
		static_cast<std::uint8_t>(pop_pr),
		static_cast<std::uint8_t>(k),
		pb_can, pr_can, U_can);

	// If the unranked canonical differs from the input, the canonical representative is the LR mirror.
	const bool mirrored = (pb_can != bb_player_blue) || (pr_can != bb_player_red) || (U_can != U);

	const std::uint64_t or_can = mirrored ? mirror_lr_u64(bb_opponent_red) : bb_opponent_red;
	assert((or_can & ~U_can) == 0);
	assert(std::popcount(or_can) == pop_or);

	const std::uint64_t comp_red = _pext_u64(or_can, U_can);
	const std::uint64_t inner = rank_patterns_colex(comp_red, k, pop_or);
	assert(inner < inner_domain);

	return outer * inner_domain + inner;
}

void unrank_geister_perfect_information_obsblk(
	std::uint64_t number,
	int pop_pb, int pop_pr, int pop_ob, int pop_or,
	std::uint64_t& bb_player_blue,
	std::uint64_t& bb_player_red,
	std::uint64_t& bb_opponent_blue,
	std::uint64_t& bb_opponent_red) noexcept
{
	assert(1 <= pop_pb && pop_pb <= 4);
	assert(1 <= pop_pr && pop_pr <= 4);
	assert(1 <= pop_ob && pop_ob <= 4);
	assert(1 <= pop_or && pop_or <= 4);
	const int k = pop_ob + pop_or;
	assert(2 <= k && k <= 8);

	const std::uint64_t inner_domain = comb_small(k, pop_or);
	[[maybe_unused]] const std::uint64_t outer_domain = states_for_counts(
		static_cast<std::uint8_t>(pop_pb),
		static_cast<std::uint8_t>(pop_pr),
		static_cast<std::uint8_t>(k));
	assert(number < outer_domain * inner_domain);

	const std::uint64_t outer = number / inner_domain;
	const std::uint64_t inner = number % inner_domain;
	assert(outer < outer_domain);

	std::uint64_t U_can = 0;
	unrank_triplet_canon(
		outer,
		static_cast<std::uint8_t>(pop_pb),
		static_cast<std::uint8_t>(pop_pr),
		static_cast<std::uint8_t>(k),
		bb_player_blue, bb_player_red, U_can);

	const std::uint64_t comp_red = unrank_patterns_colex(inner, k, pop_or);
	bb_opponent_red = _pdep_u64(comp_red, U_can);
	bb_opponent_blue = U_can ^ bb_opponent_red;

	assert(std::popcount(bb_player_blue) == pop_pb);
	assert(std::popcount(bb_player_red) == pop_pr);
	assert(std::popcount(bb_opponent_blue) == pop_ob);
	assert(std::popcount(bb_opponent_red) == pop_or);
	assert(((bb_player_blue | bb_player_red | bb_opponent_blue | bb_opponent_red) & ~bb_board) == 0);
	assert((bb_player_blue & bb_player_red) == 0);
	assert((bb_player_blue & bb_opponent_blue) == 0);
	assert((bb_player_blue & bb_opponent_red) == 0);
	assert((bb_player_red & bb_opponent_blue) == 0);
	assert((bb_player_red & bb_opponent_red) == 0);
	assert((bb_opponent_blue & bb_opponent_red) == 0);
}
