module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <iostream>

export module geister_random_player;

import geister_core;
import geister_interface;

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Random player (interface-probing implementation).
//
// Contract:
//   - Inputs are a board-observation snapshot from the side-to-move POV.
//   - The returned move is always an on-board move (never an escape move).
//   - Capture flags inside geister_core::move are always zero.
//   - The caller is responsible for terminal checks (including escape).
//
// rng_seed:
//   - If rng_seed != 0, it is used as the random seed.
//   - If rng_seed == 0, a deterministic seed is derived from (bb_my_blue, bb_my_red).
//     This keeps the default behaviour reproducible.
export [[nodiscard]] move random_player(
	std::uint64_t bb_my_blue,
	std::uint64_t bb_my_red,
	std::uint64_t bb_opponent_unknown,
	int pop_captured_opponent_red,
	std::uint64_t rng_seed = 0);

// -----------------------------------------------------------------------------
// Non-exported helpers (kept outside the private fragment so the warning logic
// stays near the public entrypoint).
// -----------------------------------------------------------------------------

[[nodiscard]] inline bool random_player_escape_available_warn_if_needed(
	const std::uint64_t bb_my_blue) noexcept
{
	if (!escape_available(bb_my_blue)) return false;

	// Caller should normally have handled this as a terminal action.
	std::cerr
		<< "[geister_random_player] WARNING: escape is available (blue on A1/F1). "
		<< "Caller should handle escape instead of invoking random_player().\n";
	return true;
}

module:private;

// -----------------------------------------------------------------------------
// Implementations
// -----------------------------------------------------------------------------

namespace {

// SplitMix64: small, fast, reproducible 64-bit PRNG.
// Public domain reference: Sebastiano Vigna.
[[nodiscard]] inline std::uint64_t splitmix64_next(std::uint64_t& state) noexcept {
	state += 0x9E3779B97F4A7C15ULL;
	std::uint64_t z = state;
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
	z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
	return z ^ (z >> 31);
}

[[nodiscard]] inline std::uint64_t default_seed_from_position(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red) noexcept
{
	// Deterministic hash (only uses my pieces, per interface-probing intent).
	std::uint64_t s = 0xD1B54A32D192ED03ULL;
	s ^= bb_my_blue + 0x9E3779B97F4A7C15ULL + (s << 6) + (s >> 2);
	s ^= bb_my_red + 0x9E3779B97F4A7C15ULL + (s << 6) + (s >> 2);
	return s;
}

[[nodiscard]] inline move pick_random_move(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red,
	std::uint64_t seed) {
	player_board me(bb_my_red, bb_my_blue);

	std::array<move, 32> moves{};
	const int n = me.gen_moves(/*bb_opponent_red=*/0ULL, /*bb_opponent_blue=*/0ULL, moves);
	assert(n > 0);
	if (n <= 0) return move{};

	std::uint64_t state = seed;
	const std::uint64_t r = splitmix64_next(state);
	const int idx = static_cast<int>(r % static_cast<std::uint64_t>(n));

	// Capture flags are intentionally zero, since opponent colors are unknown.
	moves[idx].m &= static_cast<std::uint16_t>(~(CAPTURE_RED_FLAG | CAPTURE_BLUE_FLAG));
	return moves[idx];
}

} // namespace

move random_player(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red,
	const std::uint64_t bb_opponent_unknown,
	const int pop_captured_opponent_red,
	const std::uint64_t rng_seed)
{
	// -------------------------------------------------------------------------
	// Debug-only invariant checks (disabled under -DNDEBUG).
	// -------------------------------------------------------------------------
	assert((bb_my_blue & bb_my_red) == 0);
	assert(((bb_my_blue | bb_my_red) & ~bb_board) == 0);
	assert((bb_opponent_unknown & ~bb_board) == 0);
	assert(((bb_my_blue | bb_my_red) & bb_opponent_unknown) == 0);

	assert(0 <= pop_captured_opponent_red && pop_captured_opponent_red <= 3);
	const int opp_remaining = static_cast<int>(std::popcount(bb_opponent_unknown));
	const int opp_captured_total = 8 - opp_remaining;
	assert(0 <= opp_remaining && opp_remaining <= 8);
	assert(0 <= opp_captured_total && opp_captured_total <= 8);
	assert(pop_captured_opponent_red <= opp_captured_total);

	// Non-terminal domain consistency (optional but useful for catching caller bugs).
	// Opponent remaining pieces must be in [5-k, 8-k] for k in [0,3].
	// (At least 1 blue remaining + (4-k) reds remaining.)
	assert(opp_remaining >= 5 - pop_captured_opponent_red);
	assert(opp_remaining <= 8 - pop_captured_opponent_red);

	// -------------------------------------------------------------------------
	// Warn (do not assert) if escape is available.
	// -------------------------------------------------------------------------
	(void)random_player_escape_available_warn_if_needed(bb_my_blue);

	// -------------------------------------------------------------------------
	// Select a random legal on-board move.
	// -------------------------------------------------------------------------
	const std::uint64_t seed = (rng_seed != 0) ? rng_seed : default_seed_from_position(bb_my_blue, bb_my_red);
	return pick_random_move(bb_my_blue, bb_my_red, seed);
}
