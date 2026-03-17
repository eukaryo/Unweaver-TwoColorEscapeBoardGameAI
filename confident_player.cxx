module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>
#include <vector>

export module confident_player;

import geister_core;
import geister_interface;
import geister_tb_handler;

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Confident player (tablebase-guided example using perfect-information TBs).
//
// Signature matches geister_random_player::random_player, except:
//   - return type is std::optional<std::vector<protocol_move>>.
//
// Behaviour:
//   (1) Enumerate all opponent colorings consistent with the public observation.
//   (2) For each legal move, probe the perfect-information tablebase on every
//       consistent 1-ply child position.
//   (3) Consider only moves whose child positions are fully covered by the
//       loaded perfect-information tablebase files.
//   (4) Among those moves, apply the ranking policy documented below and return
//       all ties.
//
// Ranking policy:
//   - First minimize the number of opponent colorings that make the move losing.
//   - If every fully covered move still loses for at least one coloring, then
//     among the minimum-loss moves maximize the minimum distance-to-loss.
//   - Otherwise, among zero-loss moves maximize the number of winning colorings;
//     ties are broken by minimizing the maximum distance-to-win.
//
// Returns std::nullopt when:
//   - escape is already available (caller should handle it directly),
//   - the perfect-information runtime is not ready yet,
//   - no legal move exists, or
//   - no move is fully covered by the currently loaded perfect-information TBs.
export [[nodiscard]] std::optional<std::vector<protocol_move>> confident_player(
	std::uint64_t bb_my_blue,
	std::uint64_t bb_my_red,
	std::uint64_t bb_opponent_unknown,
	int pop_captured_opponent_red,
	std::uint64_t rng_seed = 0);

// -----------------------------------------------------------------------------
// Non-exported helpers kept outside the private fragment so the warning logic
// stays near the public entrypoint.
// -----------------------------------------------------------------------------

[[nodiscard]] inline bool confident_escape_available_warn_if_needed(
	const std::uint64_t bb_my_blue) noexcept
{
	if (!escape_available(bb_my_blue)) return false;

	std::cerr
		<< "[confident_player] WARNING: escape is available (blue on A1/F1). "
		<< "Caller should handle escape instead of invoking confident_player().\n";
	return true;
}

module:private;

namespace {

// -----------------------------------------------------------------------------
// Asynchronous tablebase preload helper
// -----------------------------------------------------------------------------

inline void request_background_tb_load() noexcept {
	geister_tb::start_background_load();
}

// -----------------------------------------------------------------------------
// Opponent-color enumeration utilities
// -----------------------------------------------------------------------------

inline constexpr int kMaxOppPieces = 8;

[[nodiscard]] inline int extract_squares(
	std::uint64_t bb,
	std::array<int, kMaxOppPieces>& out_sq) noexcept
{
	int n = 0;
	while (bb) {
		const int sq = static_cast<int>(std::countr_zero(bb));
		out_sq[n++] = sq;
		bb &= (bb - 1);
	}
	return n;
}

[[nodiscard]] inline std::vector<std::uint64_t> enumerate_red_subsets(
	const std::uint64_t bb_opp_all,
	const int opp_red_count)
{
	std::vector<std::uint64_t> out;
	if (opp_red_count < 0) return out;
	if (opp_red_count == 0) {
		out.push_back(0ULL);
		return out;
	}

	std::array<int, kMaxOppPieces> sq{};
	const int n = extract_squares(bb_opp_all, sq);
	if (opp_red_count > n) return out;
	if (opp_red_count == n) {
		out.push_back(bb_opp_all);
		return out;
	}

	// n <= 8, so brute force over 2^n (<=256) is fine.
	const std::uint32_t limit = (n >= 32)
		? 0xFFFF'FFFFu
		: (1u << static_cast<std::uint32_t>(n));
	for (std::uint32_t mask = 0; mask < limit; ++mask) {
		if (std::popcount(mask) != opp_red_count) continue;
		std::uint64_t bb_red = 0;
		for (int i = 0; i < n; ++i) {
			if (mask & (1u << static_cast<std::uint32_t>(i))) {
				bb_red |= (1ULL << static_cast<std::uint64_t>(sq[i]));
			}
		}
		out.push_back(bb_red);
	}
	return out;
}

// -----------------------------------------------------------------------------
// Perfect-information evaluation for tablebase-guided move ranking
// -----------------------------------------------------------------------------

struct perfect_eval {
	move m{};
	int probed_patterns = 0;
	int missing_patterns = 0;
	int lose_patterns = 0;           // #patterns where "I" will surely lose
	int win_patterns = 0;            // #patterns where "I" will surely win
	std::uint8_t lose_dtw_min = 255; // min(DTL) over losing patterns
	std::uint8_t win_dtw_max = 0;    // max(DTW) over winning patterns
};

} // namespace

// -----------------------------------------------------------------------------
// Implementation
// -----------------------------------------------------------------------------

std::optional<std::vector<protocol_move>> confident_player(
	const std::uint64_t bb_my_blue,
	const std::uint64_t bb_my_red,
	const std::uint64_t bb_opponent_unknown,
	const int pop_captured_opponent_red,
	const std::uint64_t rng_seed)
{
	(void)rng_seed; // kept only for signature-compatibility

	// -------------------------------------------------------------------------
	// Debug-only invariant checks (disabled under -DNDEBUG).
	// -------------------------------------------------------------------------
	assert((bb_my_blue & bb_my_red) == 0);
	assert(((bb_my_blue | bb_my_red) & ~bb_board) == 0);
	assert((bb_opponent_unknown & ~bb_board) == 0);
	assert(((bb_my_blue | bb_my_red) & bb_opponent_unknown) == 0);

	assert(0 <= pop_captured_opponent_red && pop_captured_opponent_red <= 4);
	const int opp_remaining = static_cast<int>(std::popcount(bb_opponent_unknown));
	const int opp_captured_total = 8 - opp_remaining;
	assert(0 <= opp_remaining && opp_remaining <= 8);
	assert(0 <= opp_captured_total && opp_captured_total <= 8);
	assert(pop_captured_opponent_red <= opp_captured_total);

	// Non-terminal domain consistency (useful for catching caller bugs).
	// Opponent remaining pieces must be in [5-k, 8-k] for k in [0,3].
	// (At least 1 blue remaining + (4-k) reds remaining.)
	if (0 <= pop_captured_opponent_red && pop_captured_opponent_red <= 3) {
		assert(opp_remaining >= 5 - pop_captured_opponent_red);
		assert(opp_remaining <= 8 - pop_captured_opponent_red);
	}

	// -------------------------------------------------------------------------
	// Warn and refuse if escape is available.
	// -------------------------------------------------------------------------
	if (confident_escape_available_warn_if_needed(bb_my_blue)) {
		return std::nullopt;
	}

	// -------------------------------------------------------------------------
	// Kick off asynchronous TB preload if the caller has not done so yet.
	// -------------------------------------------------------------------------
	request_background_tb_load();
	if (!geister_tb::is_ready()) {
		return std::nullopt;
	}

	// -------------------------------------------------------------------------
	// Generate legal on-board moves (capture flags cleared).
	// -------------------------------------------------------------------------
	player_board me(bb_my_red, bb_my_blue);
	std::array<move, 32> moves{};
	const int n_moves = me.gen_moves(/*bb_opponent_red=*/0ULL, /*bb_opponent_blue=*/0ULL, moves);
	if (n_moves <= 0) {
		return std::nullopt;
	}

	// -------------------------------------------------------------------------
	// Enumerate opponent colorings consistent with current public info.
	// -------------------------------------------------------------------------
	if (pop_captured_opponent_red < 0 || pop_captured_opponent_red > 4) {
		return std::nullopt;
	}
	const int opp_red_remaining = 4 - pop_captured_opponent_red;
	const int opp_total_remaining = static_cast<int>(std::popcount(bb_opponent_unknown));
	const int opp_blue_remaining = opp_total_remaining - opp_red_remaining;

	// If counts are inconsistent, we cannot enumerate any consistent coloring.
	if (opp_red_remaining < 0 || opp_red_remaining > 4) return std::nullopt;
	if (opp_blue_remaining < 0 || opp_blue_remaining > 4) return std::nullopt;

	const auto opp_red_subsets = enumerate_red_subsets(bb_opponent_unknown, opp_red_remaining);
	if (opp_red_subsets.empty()) {
		return std::nullopt;
	}

	// Prebuild opponent boards for each coloring.
	std::vector<player_board> opp_boards;
	opp_boards.reserve(opp_red_subsets.size());
	for (const std::uint64_t bb_opp_red : opp_red_subsets) {
		const std::uint64_t bb_opp_blue = bb_opponent_unknown ^ bb_opp_red;
		try {
			opp_boards.emplace_back(/*red=*/bb_opp_red, /*blue=*/bb_opp_blue);
		}
		catch (...) {
			// Should not happen if enumeration is correct.
			return std::nullopt;
		}
	}

	const player_board my_board(/*red=*/bb_my_red, /*blue=*/bb_my_blue);

	// -------------------------------------------------------------------------
	// Evaluate each move by probing the perfect-information tablebase on every
	// consistent opponent coloring.
	// -------------------------------------------------------------------------
	std::vector<perfect_eval> evals;
	evals.reserve(static_cast<std::size_t>(n_moves));

	for (int i = 0; i < n_moves; ++i) {
		move m = moves[static_cast<std::size_t>(i)];
		m.m &= static_cast<std::uint16_t>(~(CAPTURE_RED_FLAG | CAPTURE_BLUE_FLAG));

		perfect_eval e{};
		e.m = m;

		for (const auto& opp_board : opp_boards) {
			perfect_information_geister pos{ my_board, opp_board };
			perfect_information_geister::do_move(m, pos);

			const auto v_opt = geister_tb::probe_perfect_information(pos);
			if (!v_opt.has_value()) {
				++e.missing_patterns;
				continue;
			}

			++e.probed_patterns;
			const std::uint8_t v = *v_opt;
			if (v == 0) continue;

			if (v & 1U) {
				// side-to-move (opponent) sure-win => we sure-lose
				++e.lose_patterns;
				if (v < e.lose_dtw_min) e.lose_dtw_min = v;
			}
			else {
				// side-to-move (opponent) sure-loss => we sure-win
				++e.win_patterns;
				if (v > e.win_dtw_max) e.win_dtw_max = v;
			}
		}

		evals.push_back(e);
	}

	if (evals.empty()) {
		return std::nullopt;
	}

	// Require complete coverage across all consistent opponent colorings.
	std::vector<const perfect_eval*> covered;
	covered.reserve(evals.size());
	for (const auto& e : evals) {
		if (e.missing_patterns == 0 && e.probed_patterns > 0) {
			covered.push_back(&e);
		}
	}
	if (covered.empty()) {
		return std::nullopt;
	}

	// (i) Minimize #lose_patterns.
	int min_lose = covered.front()->lose_patterns;
	for (const auto* e : covered) {
		if (e->lose_patterns < min_lose) min_lose = e->lose_patterns;
	}

	std::vector<protocol_move> out;

	if (min_lose != 0) {
		// (ii-a) No zero-lose move exists.
		// Among moves with minimal lose_patterns, maximize min(distance_to_lose).
		std::uint8_t best_min_dtl = 0;
		for (const auto* e : covered) {
			if (e->lose_patterns != min_lose) continue;
			if (e->lose_dtw_min > best_min_dtl) best_min_dtl = e->lose_dtw_min;
		}
		for (const auto* e : covered) {
			if (e->lose_patterns != min_lose) continue;
			if (e->lose_dtw_min != best_min_dtl) continue;
			out.push_back(to_protocol_move(e->m));
		}
		return out;
	}

	// Here, there exists at least one move with lose_patterns == 0.
	int max_win = 0;
	for (const auto* e : covered) {
		if (e->lose_patterns != 0) continue;
		if (e->win_patterns > max_win) max_win = e->win_patterns;
	}

	if (max_win == 0) {
		// (ii-b) There is a zero-lose move, but all of them have zero win patterns.
		// Return all fully covered zero-lose moves without further tie-break.
		for (const auto* e : covered) {
			if (e->lose_patterns == 0) out.push_back(to_protocol_move(e->m));
		}
		return out;
	}

	// Among zero-lose moves, maximize win_patterns; tie-break by minimizing
	// max(distance_to_win).
	std::uint8_t best_max_dtw = 255;
	for (const auto* e : covered) {
		if (e->lose_patterns != 0) continue;
		if (e->win_patterns != max_win) continue;
		if (e->win_dtw_max < best_max_dtw) best_max_dtw = e->win_dtw_max;
	}

	for (const auto* e : covered) {
		if (e->lose_patterns != 0) continue;
		if (e->win_patterns != max_win) continue;
		if (e->win_dtw_max != best_max_dtw) continue;
		out.push_back(to_protocol_move(e->m));
	}
	return out;
}
