module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>

export module geister_purple_winning;

import geister_core;
import geister_interface;
import geister_tb_handler;

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------
//
// Find an on-board move that is certified winning under the Purple DTW
// tablebase model.
//
// Model / scope:
//   - We only use N-side purple tables via geister_tb::probe_purple().
//   - The current side-to-move is interpreted as "Normal".
//   - The opponent on-board pieces are treated as Purple pieces.
//   - Since no P-side table is probed, we certify a root move by enumerating all
//     Purple replies and requiring every reply to reach a Normal-to-move child
//     that is still winning in the purple tablebase.
//
// Move choice policy:
//   - Among certified winning root moves, choose the move with the smallest DTW
//     under the tablebase convention.
//   - For a non-immediate winning move, this is equivalent to minimizing
//       max_reply(child_normal_dtw) + 2.
//   - Ties are broken deterministically by the core on-board move encoding with
//     capture flags cleared.
//
// Returned move:
//   - always an on-board `protocol_move` (never an escape move)
//   - std::nullopt means "not certified as purple-tablebase-winning" or
//     "required tablebase not loaded"
//
// NOTE:
//   As with geister_random_player / geister_proven_escape, the caller is
//   expected to handle the immediate protocol-level escape action separately.
export [[nodiscard]] std::optional<protocol_move> purple_winning_move(
	std::uint64_t bb_my_blue,
	std::uint64_t bb_my_red,
	std::uint64_t bb_opponent_unknown,
	int pop_captured_opponent_red,
	std::uint64_t rng_seed = 0);

module:private;

namespace {

[[nodiscard]] inline bool is_win_value(const std::uint8_t v) noexcept {
	return (v != 0U) && ((v & 1U) != 0U);
}

[[nodiscard]] inline move clear_capture_flags(move m) noexcept {
	m.m &= static_cast<std::uint16_t>(~(CAPTURE_RED_FLAG | CAPTURE_BLUE_FLAG));
	return m;
}

[[nodiscard]] inline bool escape_available_warn_if_needed(
	const std::uint64_t bb_my_blue) noexcept
{
	if (!escape_available(bb_my_blue)) return false;

	std::cerr
		<< "[geister_purple_winning] WARNING: escape is available (blue on A1/F1). "
		<< "Caller should handle the protocol-level escape action instead of invoking "
		<< "purple_winning_move().\n";
	return true;
}

[[nodiscard]] inline perfect_information_geister make_purple_root_position(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red,
	const std::uint64_t bb_opponent_unknown)
{
	return perfect_information_geister(
		player_board(bb_my_red, bb_my_blue),
		player_board(/*red=*/0ULL, /*blue=*/bb_opponent_unknown));
}

// Terminal checks matching geister_purple_tb.cpp semantics.

[[nodiscard]] inline bool normal_immediate_win(const perfect_information_geister& pos) noexcept {
	if ((pos.bb_player.bb_blue & ((1ULL << POSITIONS::A1) | (1ULL << POSITIONS::F1))) != 0ULL) return true;
	if (pos.bb_player.bb_red == 0ULL) return true;
	if (pos.bb_opponent.bb_blue == 0ULL) return true; // no purple pieces remain
	return false;
}

[[nodiscard]] inline bool normal_immediate_loss(
	const perfect_information_geister& pos,
	const std::uint8_t k) noexcept
{
	if (pos.bb_player.bb_blue == 0ULL) return true;
	if (k >= 4U) return true;
	return false;
}

[[nodiscard]] inline bool purple_immediate_win(
	const perfect_information_geister& pos,
	const std::uint8_t /*k_unused*/) noexcept
{
	if ((pos.bb_player.bb_blue & ((1ULL << POSITIONS::A1) | (1ULL << POSITIONS::F1))) != 0ULL) return true;
	if (pos.bb_opponent.bb_blue == 0ULL) return true; // captured all Normal blues
	return false;
}

[[nodiscard]] inline bool purple_immediate_loss(
	const perfect_information_geister& pos,
	const std::uint8_t k) noexcept
{
	if (pos.bb_opponent.bb_red == 0ULL) return true; // Purple captured all reds => Normal wins
	if (k >= 4U) return false;                       // Normal already lost; not a Purple loss case
	if (pos.bb_player.bb_blue == 0ULL) return true; // Purple has no pieces
	return false;
}

inline void debug_validate_inputs(
	[[maybe_unused]] const std::uint64_t bb_my_blue,
	[[maybe_unused]] const std::uint64_t bb_my_red,
	const std::uint64_t bb_opponent_unknown,
	[[maybe_unused]] const int pop_captured_opponent_red)
{
	assert((bb_my_blue & bb_my_red) == 0ULL);
	assert(((bb_my_blue | bb_my_red) & ~bb_board) == 0ULL);
	assert((bb_opponent_unknown & ~bb_board) == 0ULL);
	assert(((bb_my_blue | bb_my_red) & bb_opponent_unknown) == 0ULL);
	assert(0 <= pop_captured_opponent_red && pop_captured_opponent_red <= 3);

	const int opp_remaining = static_cast<int>(std::popcount(bb_opponent_unknown));
	[[maybe_unused]] const int opp_captured_total = 8 - opp_remaining;

	assert(0 <= opp_remaining && opp_remaining <= 8);
	assert(0 <= opp_captured_total && opp_captured_total <= 8);
	assert(pop_captured_opponent_red <= opp_captured_total);

	// Purple runtime non-terminal domain consistency.
	assert(opp_remaining >= 5 - pop_captured_opponent_red);
	assert(opp_remaining <= 8 - pop_captured_opponent_red);
}

} // namespace

std::optional<protocol_move> purple_winning_move(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red,
	const std::uint64_t bb_opponent_unknown,
	const int pop_captured_opponent_red,
	const std::uint64_t rng_seed)
{
	(void)rng_seed; // kept for signature compatibility with proven_escape_move.

	debug_validate_inputs(
		bb_my_blue,
		bb_my_red,
		bb_opponent_unknown,
		pop_captured_opponent_red);

	if (escape_available_warn_if_needed(bb_my_blue)) {
		return std::nullopt;
	}

	// Runtime guards.
	if ((bb_my_blue & bb_my_red) != 0ULL) return std::nullopt;
	if (((bb_my_blue | bb_my_red) & ~bb_board) != 0ULL) return std::nullopt;
	if ((bb_opponent_unknown & ~bb_board) != 0ULL) return std::nullopt;
	if (((bb_my_blue | bb_my_red) & bb_opponent_unknown) != 0ULL) return std::nullopt;
	if (pop_captured_opponent_red < 0 || pop_captured_opponent_red > 3) return std::nullopt;

	const int opp_remaining = static_cast<int>(std::popcount(bb_opponent_unknown));
	const int opp_captured_total = 8 - opp_remaining;
	if (opp_remaining < 0 || opp_remaining > 8) return std::nullopt;
	if (opp_captured_total < 0 || opp_captured_total > 8) return std::nullopt;
	if (pop_captured_opponent_red > opp_captured_total) return std::nullopt;
	if (opp_remaining < (5 - pop_captured_opponent_red)) return std::nullopt;
	if (opp_remaining > (8 - pop_captured_opponent_red)) return std::nullopt;

	perfect_information_geister root = make_purple_root_position(
		bb_my_blue, bb_my_red, bb_opponent_unknown);
	const std::uint8_t k_root = static_cast<std::uint8_t>(pop_captured_opponent_red);

	// Caller is expected not to invoke us on terminal positions.
	if (normal_immediate_win(root)) return std::nullopt;
	if (normal_immediate_loss(root, k_root)) return std::nullopt;

	const auto root_v = geister_tb::probe_purple(geister_tb::purple_position{ root, k_root });
	if (!root_v.has_value()) return std::nullopt;
	if (!is_win_value(*root_v)) return std::nullopt;

	std::array<move, 32> root_moves{};
	const int n_root_moves = root.bb_player.gen_moves(
		/*opp_red=*/0ULL,
		/*opp_blue=*/root.bb_opponent.bb_blue,
		root_moves);
	if (n_root_moves <= 0) return std::nullopt;

	std::optional<move> best_root_move = std::nullopt;
	int best_root_dtw = 1'000'000;

	for (int i = 0; i < n_root_moves; ++i) {
		const move root_move = clear_capture_flags(root_moves[static_cast<std::size_t>(i)]);
		const bool captures_purple = root_moves[static_cast<std::size_t>(i)].if_capture_blue();
		const std::uint8_t k_after_root = static_cast<std::uint8_t>(k_root + (captures_purple ? 1U : 0U));
		if (k_after_root >= 4U) {
			// Capturing the 4th purple-as-red loses immediately for Normal.
			continue;
		}

		perfect_information_geister after_root = root;
		perfect_information_geister::do_move(root_moves[static_cast<std::size_t>(i)], after_root);

		if (purple_immediate_win(after_root, k_after_root)) {
			continue; // root move loses immediately / allows immediate Purple escape
		}
		if (purple_immediate_loss(after_root, k_after_root)) {
			const int cand_root_dtw = 1;
			if (!best_root_move.has_value()
				|| cand_root_dtw < best_root_dtw
				|| (cand_root_dtw == best_root_dtw && root_move.m < best_root_move->m)) {
				best_root_move = root_move;
				best_root_dtw = cand_root_dtw;
			}
			continue;
		}

		std::array<move, 32> replies{};
		const int n_replies = after_root.bb_player.gen_moves(
			after_root.bb_opponent.bb_red,
			after_root.bb_opponent.bb_blue,
			replies);
		if (n_replies <= 0) {
			continue; // should not happen for non-terminal positions
		}

		bool certified = true;
		int worst_child_dtw = 0;

		for (int ri = 0; ri < n_replies; ++ri) {
			perfect_information_geister child = after_root;
			perfect_information_geister::do_move(replies[static_cast<std::size_t>(ri)], child);

			if (normal_immediate_loss(child, k_after_root)) {
				certified = false;
				break;
			}

			int child_dtw = 0;
			if (normal_immediate_win(child)) {
				child_dtw = 1;
			}
			else {
				const auto child_v = geister_tb::probe_purple(
					geister_tb::purple_position{ child, k_after_root });
				if (!child_v.has_value() || !is_win_value(*child_v)) {
					certified = false;
					break;
				}
				child_dtw = static_cast<int>(*child_v);
			}

			if (child_dtw > worst_child_dtw) {
				worst_child_dtw = child_dtw;
			}
		}

		if (!certified) continue;

		// Root Normal move -> Purple node -> worst Normal child.
		const int cand_root_dtw = worst_child_dtw + 2;
		if (!best_root_move.has_value()
			|| cand_root_dtw < best_root_dtw
			|| (cand_root_dtw == best_root_dtw && root_move.m < best_root_move->m)) {
			best_root_move = root_move;
			best_root_dtw = cand_root_dtw;
		}
	}

	if (!best_root_move.has_value()) return std::nullopt;
	return to_protocol_move(*best_root_move);
}
