module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>

export module geister_proven_escape;

import geister_core;
import geister_interface;

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------
//
// Conservative on-board move finder for a provable escape race.
//
// Naming rationale:
//   - "proven" is more precise than "confident": if this function returns a
//     move, the move is intended to be sound under a deliberately conservative
//     sufficient condition (false negatives are acceptable; false positives are
//     not).
//
// Signature matches geister_random_player::random_player.
//
// Model used (all conservative):
//   (1) Static-blocker assumption for our currently occupied squares:
//       after the chosen first move, every square that is occupied by either
//       side in the *current* observation is treated as permanently unusable
//       for the charging runner. This intentionally ignores plans such as
//       "move blocker A away, then pass runner B through".
//   (2) Purple assumption for the opponent:
//       every opponent piece is treated as dangerous for escape-race purposes.
//   (3) Opponent threat times are optimistic for the opponent:
//       a square S is considered unsafe at our step t if some opponent piece is
//       within Manhattan distance <= t from S.
//   (4) Opponent escape-race time is also optimistic for the opponent:
//       we ignore blockers and ask whether some opponent piece could reach A6/F6
//       (from the current POV) and then spend one more turn to escape.
//
// Returned move:
//   - always an on-board move (never an escape move)
//   - capture flags are cleared
//   - std::nullopt means "no move certified by this sufficient condition"
//
// NOTE:
//   As in geister_random_player, the caller is expected to handle the immediate
//   terminal/escape case separately. If escape is already available now, the
//   winning action is the protocol-level escape move, which cannot be expressed
//   as geister_core::move.
export [[nodiscard]] std::optional<move> proven_escape_move(
	std::uint64_t bb_my_blue,
	std::uint64_t bb_my_red,
	std::uint64_t bb_opponent_unknown,
	int pop_captured_opponent_red,
	std::uint64_t rng_seed = 0);

module:private;

namespace {

inline constexpr std::uint64_t kMyExitMask =
	(1ULL << POSITIONS::A1) |
	(1ULL << POSITIONS::F1);

inline constexpr std::uint64_t kOppExitMask =
	(1ULL << POSITIONS::A6) |
	(1ULL << POSITIONS::F6);

inline constexpr std::uint8_t kInfTurns = 0xFFu;

[[nodiscard]] constexpr int abs_i(const int x) noexcept {
	return (x < 0) ? -x : x;
}

[[nodiscard]] constexpr int rank_of_sq(const int sq) noexcept { return sq / 8; }
[[nodiscard]] constexpr int file_of_sq(const int sq) noexcept { return sq % 8; }

[[nodiscard]] constexpr int manhattan_sq(const int a, const int b) noexcept {
	return abs_i(rank_of_sq(a) - rank_of_sq(b)) + abs_i(file_of_sq(a) - file_of_sq(b));
}

[[nodiscard]] inline std::uint8_t clamp_turns_u8(const int v) noexcept {
	if (v <= 0) return 0;
	if (v >= static_cast<int>(kInfTurns)) return kInfTurns;
	return static_cast<std::uint8_t>(v);
}

[[nodiscard]] inline bool escape_available_warn_if_needed(
	const std::uint64_t bb_my_blue) noexcept
{
	if (!escape_available(bb_my_blue)) return false;

	std::cerr
		<< "[geister_proven_escape] WARNING: escape is available (blue on A1/F1). "
		<< "Caller should handle the protocol-level escape action instead of invoking "
		<< "proven_escape_move().\n";
	return true;
}

[[nodiscard]] consteval std::array<std::uint64_t, 8> make_opponent_escape_turn_masks() {
	std::array<std::uint64_t, 8> masks{};
	for (int sq = 0; sq < 64; ++sq) {
		const std::uint64_t bb_sq = (1ULL << static_cast<std::uint64_t>(sq));
		if ((bb_board & bb_sq) == 0) continue;

		const int d_a = manhattan_sq(sq, static_cast<int>(POSITIONS::A6));
		const int d_f = manhattan_sq(sq, static_cast<int>(POSITIONS::F6));
		const int turns = 1 + ((d_a < d_f) ? d_a : d_f); // +1 for the actual escape action.
		masks[static_cast<std::size_t>(turns - 1)] |= bb_sq;
	}
	return masks;
}

inline constexpr auto kOpponentEscapeTurnMasks = make_opponent_escape_turn_masks();

[[nodiscard]] consteval bool validate_opponent_escape_turn_masks() {
	std::uint64_t covered = 0;
	for (const std::uint64_t m : kOpponentEscapeTurnMasks) {
		if ((covered & m) != 0) return false;
		covered |= m;
	}
	return covered == bb_board;
}

static_assert(validate_opponent_escape_turn_masks());
static_assert((kOpponentEscapeTurnMasks[0] & kOppExitMask) == kOppExitMask);

[[nodiscard]] inline std::uint8_t fastest_opponent_escape_turn(
	const std::uint64_t bb_opponent_unknown) noexcept
{
	for (std::uint8_t t = 1; t <= 8; ++t) {
		if ((bb_opponent_unknown & kOpponentEscapeTurnMasks[static_cast<std::size_t>(t - 1)]) != 0) {
			return t;
		}
	}
	return kInfTurns;
}

// Compute optimistic opponent arrival times (in opponent turns from *now*) to
// every board square using Manhattan distance from the current opponent pieces.
//
// Interpretation:
//   threat[sq] == m  means: some opponent piece could be on sq by its m-th turn
//   under the conservative (opponent-favouring) approximation.
//
// We only use these values on squares that are initially empty, but computing
// them for all board squares keeps the code simple.
[[nodiscard]] inline std::array<std::uint8_t, 64> compute_opponent_threat_turns(
	const std::uint64_t bb_opponent_unknown) noexcept
{
	std::array<std::uint8_t, 64> out{};
	out.fill(kInfTurns);

	for (std::uint64_t bb_opp = bb_opponent_unknown; bb_opp; bb_opp &= (bb_opp - 1)) {
		const int from = static_cast<int>(std::countr_zero(bb_opp));
		for (std::uint64_t bb = bb_board; bb; bb &= (bb - 1)) {
			const int sq = static_cast<int>(std::countr_zero(bb));
			const std::uint8_t cand = clamp_turns_u8(manhattan_sq(from, sq));
			if (cand < out[static_cast<std::size_t>(sq)]) {
				out[static_cast<std::size_t>(sq)] = cand;
			}
		}
	}

	return out;
}

[[nodiscard]] inline move clear_capture_flags(move m) noexcept {
	m.m &= static_cast<std::uint16_t>(~(CAPTURE_RED_FLAG | CAPTURE_BLUE_FLAG));
	return m;
}

// Search the shortest certified route for the chosen runner after committing the
// first move `first`.
//
// Distances are counted in *our on-board moves from the current position*.
// Therefore:
//   - the destination of the first move has distance 1,
//   - if an exit square is reached at distance L, the actual escape action would
//     happen on our next turn, after the opponent has already taken L turns.
//   - hence the race condition is: L < opponent_escape_turn.
//
// The route is restricted to squares that are empty in the current observation.
// All currently occupied squares (ours and the opponent's) are treated as
// permanently blocked for the route search.
[[nodiscard]] inline std::optional<int> certified_escape_length_after_first_move(
	const move first,
	const std::uint64_t bb_initial_blocked,
	const std::array<std::uint8_t, 64>& opponent_threat_turns,
	const std::uint8_t opponent_escape_turn) noexcept
{
	const int from = static_cast<int>(first.get_from());
	const int start = static_cast<int>(first.get_to());

	(void)from; // kept for readability / future instrumentation.

	const std::uint64_t bb_start = (1ULL << static_cast<std::uint64_t>(start));
	if ((bb_board & bb_start) == 0) return std::nullopt;
	if ((bb_initial_blocked & bb_start) != 0) return std::nullopt; // would imply a capture or invalid move.

	// The runner occupies `start` after our 1st move from the current position.
	if (opponent_threat_turns[static_cast<std::size_t>(start)] <= 1) {
		return std::nullopt;
	}

	std::array<std::uint8_t, 64> dist{};
	dist.fill(kInfTurns);

	// 6x6 board => at most 36 reachable squares.
	std::array<int, 36> queue{};
	int q_head = 0;
	int q_tail = 0;

	dist[static_cast<std::size_t>(start)] = 1;
	queue[static_cast<std::size_t>(q_tail++)] = start;

	while (q_head < q_tail) {
		const int sq = queue[static_cast<std::size_t>(q_head++)];
		const int cur_d = static_cast<int>(dist[static_cast<std::size_t>(sq)]);

		const std::uint64_t bb_sq = (1ULL << static_cast<std::uint64_t>(sq));
		if ((bb_sq & kMyExitMask) != 0) {
			if (cur_d < static_cast<int>(opponent_escape_turn)) {
				return cur_d;
			}
			continue;
		}

		for (int dir = 0; dir < 4; ++dir) {
			const int nsq = sq + static_cast<int>(DIFF_SQUARE[dir]);
			if (nsq < 0 || nsq >= 64) continue;

			const std::uint64_t bb_nsq = (1ULL << static_cast<std::uint64_t>(nsq));
			if ((bb_board & bb_nsq) == 0) continue;
			if ((bb_initial_blocked & bb_nsq) != 0) continue;

			const int nd = cur_d + 1;
			if (nd >= static_cast<int>(kInfTurns)) continue;
			if (dist[static_cast<std::size_t>(nsq)] <= nd) continue;

			// Unsafe if Purple can occupy the square no later than our arrival step.
			if (opponent_threat_turns[static_cast<std::size_t>(nsq)] <= nd) continue;

			dist[static_cast<std::size_t>(nsq)] = static_cast<std::uint8_t>(nd);
			queue[static_cast<std::size_t>(q_tail++)] = nsq;
		}
	}

	return std::nullopt;
}

inline void debug_validate_inputs(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red,
	const std::uint64_t bb_opponent_unknown,
	const int pop_captured_opponent_red)
{
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

	// Non-terminal domain consistency (same intent as geister_random_player).
	assert(opp_remaining >= 5 - pop_captured_opponent_red);
	assert(opp_remaining <= 8 - pop_captured_opponent_red);
}

} // namespace

std::optional<move> proven_escape_move(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red,
	const std::uint64_t bb_opponent_unknown,
	const int pop_captured_opponent_red,
	const std::uint64_t rng_seed)
{
	(void)rng_seed; // kept only for signature compatibility with random_player.

	debug_validate_inputs(
		bb_my_blue,
		bb_my_red,
		bb_opponent_unknown,
		pop_captured_opponent_red);

	// Match the rest of the project: immediate escape is a caller-handled case.
	if (escape_available_warn_if_needed(bb_my_blue)) {
		return std::nullopt;
	}

	// Runtime guards (return nullopt instead of relying only on asserts).
	if ((bb_my_blue & bb_my_red) != 0) return std::nullopt;
	if (((bb_my_blue | bb_my_red) & ~bb_board) != 0) return std::nullopt;
	if ((bb_opponent_unknown & ~bb_board) != 0) return std::nullopt;
	if (((bb_my_blue | bb_my_red) & bb_opponent_unknown) != 0) return std::nullopt;

	if (pop_captured_opponent_red < 0 || pop_captured_opponent_red > 3) return std::nullopt;

	const int opp_remaining = static_cast<int>(std::popcount(bb_opponent_unknown));
	const int opp_captured_total = 8 - opp_remaining;
	if (opp_remaining < 0 || opp_remaining > 8) return std::nullopt;
	if (opp_captured_total < 0 || opp_captured_total > 8) return std::nullopt;
	if (pop_captured_opponent_red > opp_captured_total) return std::nullopt;
	if (opp_remaining < (5 - pop_captured_opponent_red)) return std::nullopt;
	if (opp_remaining > (8 - pop_captured_opponent_red)) return std::nullopt;

	// No runner candidate.
	if (bb_my_blue == 0) return std::nullopt;

	const std::uint64_t bb_initial_blocked = bb_my_blue | bb_my_red | bb_opponent_unknown;
	const auto opponent_threat_turns = compute_opponent_threat_turns(bb_opponent_unknown);
	const std::uint8_t opponent_escape_turn = fastest_opponent_escape_turn(bb_opponent_unknown);

	player_board me(bb_my_red, bb_my_blue);
	std::array<move, 32> moves{};
	const int n_moves = me.gen_moves(/*bb_opponent_red=*/0ULL, /*bb_opponent_blue=*/0ULL, moves);
	if (n_moves <= 0) return std::nullopt;

	std::optional<move> best_move = std::nullopt;
	int best_len = 1'000'000;

	for (int i = 0; i < n_moves; ++i) {
		move m = clear_capture_flags(moves[static_cast<std::size_t>(i)]);
		const std::uint64_t bb_from = (1ULL << m.get_from());
		const std::uint64_t bb_to = (1ULL << m.get_to());

		// Only consider charging a blue piece.
		if ((bb_my_blue & bb_from) == 0) continue;

		// Captures are intentionally rejected in this conservative sufficient
		// condition. We only route through squares that are empty now.
		if ((bb_opponent_unknown & bb_to) != 0) continue;

		const auto maybe_len = certified_escape_length_after_first_move(
			m,
			bb_initial_blocked,
			opponent_threat_turns,
			opponent_escape_turn);
		if (!maybe_len.has_value()) continue;

		const int len = *maybe_len;
		if (!best_move.has_value() || len < best_len || (len == best_len && m.m < best_move->m)) {
			best_move = m;
			best_len = len;
		}
	}

	return best_move;
}
