#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include <thread>
import tablebase_io;
import geister_core;
import geister_rank_triplet;

// -----------------------------------------------------------------------------
//  Runtime utilities (timing / progress)
// -----------------------------------------------------------------------------

namespace {
	using Clock = std::chrono::steady_clock;

	static std::mutex g_log_mutex;

	[[nodiscard]] static inline double seconds_between(const Clock::time_point& a, const Clock::time_point& b) noexcept {
		return std::chrono::duration<double>(b - a).count();
	}

	[[nodiscard]] static inline std::string format_duration_hms(double sec) {
		if (!(sec >= 0.0)) sec = 0.0;
		const uint64_t total = static_cast<uint64_t>(sec + 0.5); // round to nearest second
		const uint64_t s = total % 60;
		const uint64_t m = (total / 60) % 60;
		const uint64_t h = total / 3600;
		std::ostringstream oss;
		if (h > 0) {
			oss << h << 'h';
			oss << (m < 10 ? "0" : "") << m << 'm';
			oss << (s < 10 ? "0" : "") << s << 's';
			return oss.str();
		}
		if (m > 0) {
			oss << m << 'm';
			oss << (s < 10 ? "0" : "") << s << 's';
			return oss.str();
		}
		oss << total << 's';
		return oss.str();
	}

	static inline void log_line_locked(std::ostream& os, const std::string& line) {
		std::lock_guard<std::mutex> lk(g_log_mutex);
		os << line << std::endl;
	}

	class PeriodicProgressReporter {
	public:
		PeriodicProgressReporter(std::string label,
			uint64_t total,
			const std::atomic<uint64_t>* processed,
			int interval_sec)
			: label_(std::move(label)), total_(total), processed_(processed), interval_sec_(interval_sec)
		{
			if (interval_sec_ <= 0) return;
			start_ = Clock::now();
			last_report_time_ = start_;
			last_processed_ = 0;
			worker_ = std::thread([this]() { this->run(); });
		}

		PeriodicProgressReporter(const PeriodicProgressReporter&) = delete;
		PeriodicProgressReporter& operator=(const PeriodicProgressReporter&) = delete;

		~PeriodicProgressReporter() {
			stop();
		}

		void stop() {
			if (interval_sec_ <= 0) return;
			bool expected = false;
			if (!done_.compare_exchange_strong(expected, true)) {
				return; // already stopped
			}
			if (worker_.joinable()) worker_.join();
		}

	private:
		void run() {
			while (!done_.load(std::memory_order_relaxed)) {
				std::this_thread::sleep_for(std::chrono::seconds(interval_sec_));
				if (done_.load(std::memory_order_relaxed)) break;
				report();
			}
			// final report is intentionally omitted; callers already print stage summaries.
		}

		void report() {
			const auto now = Clock::now();
			const uint64_t cur = processed_->load(std::memory_order_relaxed);
			const double elapsed = seconds_between(start_, now);
			const double delta_t = seconds_between(last_report_time_, now);
			const uint64_t delta_p = (cur >= last_processed_) ? (cur - last_processed_) : 0;
			const double speed = (delta_t > 0.0) ? (double(delta_p) / delta_t) : 0.0;
			const double pct = (total_ > 0) ? (100.0 * double(cur) / double(total_)) : 0.0;

			std::ostringstream oss;
			oss.setf(std::ios::fixed);
			oss.precision(1);
			oss << "[PROGRESS] " << label_ << " "
				<< pct << "% (" << cur << "/" << total_ << ") "
				<< (speed / 1e6) << " Mpos/s "
				<< "elapsed=" << format_duration_hms(elapsed);
			log_line_locked(std::cout, oss.str());

			last_report_time_ = now;
			last_processed_ = cur;
		}

		std::string label_;
		uint64_t total_ = 0;
		const std::atomic<uint64_t>* processed_ = nullptr;
		int interval_sec_ = 0;
		std::atomic<bool> done_{ false };
		std::thread worker_;

		Clock::time_point start_{};
		Clock::time_point last_report_time_{};
		uint64_t last_processed_ = 0;
	};
}

// -----------------------------------------------------------------------------
//  Geister bitboard representation
// -----------------------------------------------------------------------------
// Board: 6x6 embedded inside 8x8. We use indices 0..63 (row-major).
// Valid squares are (file=1..6, rank=1..6) => bits 8*rank + file.
// Canonical orientation: side-to-move's home row is rank=1.

static constexpr int BOARD_W = 6;
static constexpr int BOARD_H = 6;

// 8x8 bitboard mask for the 6x6 playable area.
// Rows (from MSB to LSB): rank7 .. rank0
// rank7: 00000000
// rank6..rank1: 01111110
// rank0: 00000000
static constexpr uint64_t BB_BOARD =
0b00000000'01111110'01111110'01111110'01111110'01111110'01111110'00000000ULL;

static constexpr int SQ_A1 = 8 * 1 + 1;
static constexpr int SQ_F1 = 8 * 1 + 6;

static constexpr uint64_t BB_EXIT = (1ULL << SQ_A1) | (1ULL << SQ_F1);


// -----------------------------------------------------------------------------
//  Move / board / position:
//   - Reuse module: geister_core (move, player_board, perfect_information_geister)
// -----------------------------------------------------------------------------

using Move = move;
using PlayerBoard = player_board;
using CanonicalPosition = perfect_information_geister;

[[nodiscard]] static inline CanonicalPosition make_position(const PlayerBoard& player, const PlayerBoard& opponent) noexcept {
	CanonicalPosition p{ player, opponent };
	assert((p.bb_player.bb_piece & p.bb_opponent.bb_piece) == 0);
	assert(((p.bb_player.bb_piece | p.bb_opponent.bb_piece) & ~BB_BOARD) == 0);
	return p;
}

[[nodiscard]] static inline CanonicalPosition do_move(const CanonicalPosition& pos, const Move m) noexcept {
	CanonicalPosition next = pos;
	CanonicalPosition::do_move(m, next);
	return next;
}

// -----------------------------------------------------------------------------
//  Purple tablebase keying
// -----------------------------------------------------------------------------
enum class TurnKind : uint8_t {
	NormalToMove = 0,
	PurpleToMove = 1,
};

struct PurpleMaterialKey {
	uint8_t k = 0;   // number of purple pieces captured by Normal so far (0..3). Capturing 4 => Normal loses.
	uint8_t pb = 0;  // Normal blue on board (1..4)
	uint8_t pr = 0;  // Normal red on board (1..4)
	uint8_t pp = 0;  // Purple pieces on board (1..8 within total<=9)

	[[nodiscard]] int total() const { return int(pb) + int(pr) + int(pp); }
};

static inline int min_pp_for_k(uint8_t k) noexcept {
	// Opponent has 4 red pieces in the real game.
	// Given we have already captured k opponent-red pieces, opponent-red remaining = 4-k.
	// For a non-terminal position, opponent must still have at least 1 blue remaining.
	// => pp >= (4-k) + 1 = 5-k.
	return 5 - static_cast<int>(k);
}

static inline int max_pp_for_k(uint8_t k) noexcept {
	// Opponent starts with 8 total pieces, and we have already captured k red pieces,
	// so opponent remaining pieces cannot exceed 8-k.
	return 8 - static_cast<int>(k);
}

static inline bool key_in_domain(const PurpleMaterialKey& key) noexcept {
	if (key.k > 3) return false;
	if (key.pb < 1 || key.pb > 4) return false;
	if (key.pr < 1 || key.pr > 4) return false;

	// pp (opponent remaining pieces) constraints:
	// - pp >= 2 (opponent has at least 1 blue and 1 red in any non-terminal real position)
	// - pp >= 5-k (>= 1 blue remaining + (4-k) reds remaining)
	// - pp <= 8-k (cannot have more pieces remaining than initial 8 minus captured reds)
	if (key.pp < 2) return false;

	const int minpp = min_pp_for_k(key.k);
	const int maxpp = max_pp_for_k(key.k);
	if (static_cast<int>(key.pp) < minpp) return false;
	if (static_cast<int>(key.pp) > maxpp) return false;

	// Global cap (for practical construction limits)
	if (key.total() > 9) return false;

	return true;
}

static inline void validate_key(const PurpleMaterialKey& key) {
	if (key_in_domain(key)) return;

	std::ostringstream oss;
	oss << "invalid purple material key:"
		<< " k=" << static_cast<int>(key.k)
		<< " pb=" << static_cast<int>(key.pb)
		<< " pr=" << static_cast<int>(key.pr)
		<< " pp=" << static_cast<int>(key.pp)
		<< " total=" << key.total()
		<< " (constraints: k in [0,3], pb/pr in [1,4], pp in [max(2,5-k), 8-k], total<=9)";
	throw std::runtime_error(oss.str());
}

static inline std::string make_purple_filename(const PurpleMaterialKey& key, TurnKind turn) {
	// Example: tb_purple_N_k2_pb1_pr1_pp7.txt
	std::ostringstream oss;
	oss << "tb_purple_" << (turn == TurnKind::NormalToMove ? 'N' : 'P')
		<< "_k" << int(key.k)
		<< "_pb" << int(key.pb)
		<< "_pr" << int(key.pr)
		<< "_pp" << int(key.pp)
		<< ".txt";
	return oss.str();
}

static inline std::string make_purple_filename_bin(const PurpleMaterialKey& key, TurnKind turn) {
	std::filesystem::path p = make_purple_filename(key, turn);
	p.replace_extension(".bin");
	return p.string();
}

// -----------------------------------------------------------------------------
//  Tablebase I/O (hex line format, 1 byte per entry encoded as "xx\\n")
//   - Delegate to module: tablebase_io
// -----------------------------------------------------------------------------

[[nodiscard]] inline bool tablebase_file_looks_valid(const std::string& filename, uint64_t expected_entries) noexcept {
	return tbio::tablebase_file_looks_valid(std::filesystem::path(filename), expected_entries);
}

[[nodiscard]] inline bool tablebase_bin_looks_valid(const std::string& filename, uint64_t expected_entries) noexcept {
	return tbio::tablebase_bin_looks_valid(std::filesystem::path(filename), expected_entries);
}

[[nodiscard]] inline std::vector<uint8_t> load_tablebase_hex_lines(
	const std::string& filename,
	const uint64_t expected_entries_u64)
{
	// max_value defaults to 210 in module: tablebase_io
	return tbio::load_tablebase_hex_lines_streaming(
		std::filesystem::path(filename),
		expected_entries_u64);
}

inline void write_tablebase_hex_lines(const std::vector<uint8_t>& tb, const std::string& filename) {
	tbio::write_tablebase_hex_lines_streaming(tb, std::filesystem::path(filename));
}

inline void write_tablebase_bin(const std::vector<uint8_t>& tb, const std::string& filename) {
	tbio::write_tablebase_bin_streaming(tb, std::filesystem::path(filename));
}

// Normal-to-move table stores:
//   - player = Normal (bb_blue/bb_red)
//   - opponent = Purple (all stored in opponent.bb_blue, opponent.bb_red=0)
[[nodiscard]] static inline uint64_t rank_purple_normal_turn(const CanonicalPosition& pos) noexcept {
	return rank_triplet_canon(pos.bb_player.bb_blue, pos.bb_player.bb_red, pos.bb_opponent.bb_blue);
}

// Purple-to-move table stores:
//   - player = Purple (all stored in player.bb_blue, player.bb_red=0)
//   - opponent = Normal (bb_blue/bb_red)
[[nodiscard]] static inline uint64_t rank_purple_purple_turn(const CanonicalPosition& pos) noexcept {
	return rank_triplet_canon(pos.bb_player.bb_blue, pos.bb_opponent.bb_blue, pos.bb_opponent.bb_red);
}

static inline CanonicalPosition unrank_purple_normal_turn(uint64_t idx, uint8_t pb, uint8_t pr, uint8_t pp) noexcept {
	assert(idx < states_for_counts(pb, pr, pp));
	uint64_t bb_b = 0, bb_r = 0, bb_p = 0;
	unrank_triplet_canon(idx, pb, pr, pp, bb_b, bb_r, bb_p);

	PlayerBoard player{ bb_r, bb_b };
	PlayerBoard opp{ 0, bb_p };
	return make_position(player, opp);
}

static inline CanonicalPosition unrank_purple_purple_turn(uint64_t idx, uint8_t pb, uint8_t pr, uint8_t pp) noexcept {
	assert(idx < states_for_counts(pb, pr, pp));
	uint64_t bb_p = 0, bb_b = 0, bb_r = 0;
	unrank_triplet_canon(idx, pp, pb, pr, bb_p, bb_b, bb_r);

	PlayerBoard player{ 0, bb_p };
	PlayerBoard opp{ bb_r, bb_b };
	return make_position(player, opp);
}

// -----------------------------------------------------------------------------
//  DTW/DTL encoding helpers
// -----------------------------------------------------------------------------
static constexpr uint8_t TB_DRAW = 0;
static constexpr uint8_t TB_MAX_DEPTH = 210; // inclusive

static inline bool is_win_value(uint8_t v) { return (v != 0) && (v & 1); }
static inline bool is_loss_value(uint8_t v) { return (v != 0) && ((v & 1) == 0); }

// -----------------------------------------------------------------------------
//  Terminal checks for purple variant
// -----------------------------------------------------------------------------
static inline bool normal_immediate_win(const CanonicalPosition& pos) {
	// Normal-to-move. Immediate win if:
	//  - Normal has blue on an exit square (treated as "DTW=1" at turn start)
	//  - Normal has no red (opponent captured all reds) [not in our pb/pr>=1 domain]
	//  - Purple has no pieces (no threats)
	if (pos.bb_player.bb_blue & BB_EXIT) return true;
	if (pos.bb_player.bb_red == 0) return true;
	if (pos.bb_opponent.bb_blue == 0) return true; // purple has no pieces
	return false;
}

static inline bool normal_immediate_loss(const CanonicalPosition& pos, uint8_t k) {
	// Normal-to-move. Immediate loss if:
	//  - Normal has no blue [not in pb>=1 domain]
	//  - k>=4 (already captured 4 purple-as-red)
	if (pos.bb_player.bb_blue == 0) return true;
	if (k >= 4) return true;
	return false;
}

static inline bool purple_immediate_win(const CanonicalPosition& pos, uint8_t /*k_unused*/) {
	// Purple-to-move. Immediate win if:
	//  - Purple has a piece on exit squares (DTW=1 at turn start)
	//  - Normal has no blue (captured all blues)
	if (pos.bb_player.bb_blue & BB_EXIT) return true;
	if (pos.bb_opponent.bb_blue == 0) return true;
	return false;
}

static inline bool purple_immediate_loss(const CanonicalPosition& pos, uint8_t k) {
	// Purple-to-move. Immediate loss if:
	//  - Normal has no red (purple captured all reds => Normal wins)
	//  - k>=4 (Normal already captured 4 reds => Normal lost => Purple already won). In our tables k<=3.
	if (pos.bb_opponent.bb_red == 0) return true;
	if (k >= 4) return false; // would be win, not loss
	if (pos.bb_player.bb_blue == 0) return true; // purple has no pieces
	return false;
}

// -----------------------------------------------------------------------------
//  Dependency loading for a given material key
// -----------------------------------------------------------------------------
struct PurpleDeps {
	// Child tables for capture moves (nullptr if not needed for this material).
	// All are loaded from disk.

	// When Normal captures a purple piece:
	//   child material = (k+1, pb, pr, pp-1), Purple-to-move.
	std::optional<std::vector<uint8_t>> child_purple_kplus_ppminus_p_turn;

	// When Purple captures a Normal blue:
	//   child material = (k, pb-1, pr, pp), Normal-to-move.
	std::optional<std::vector<uint8_t>> child_normal_pbminus_n_turn;

	// When Purple captures a Normal red:
	//   child material = (k, pb, pr-1, pp), Normal-to-move.
	std::optional<std::vector<uint8_t>> child_normal_prminus_n_turn;
};

static PurpleDeps load_dependencies_or_throw(const PurpleMaterialKey& key) {
	PurpleDeps deps;

	// Normal captures purple: needs child if pp>=2 and k<=2.
	if (key.pp >= 2 && key.k <= 2) {
		PurpleMaterialKey child{ static_cast<uint8_t>(key.k + 1), key.pb, key.pr, static_cast<uint8_t>(key.pp - 1) };
		validate_key(child);
		uint64_t n_entries = states_for_counts(child.pb, child.pr, child.pp);
		std::string fn = make_purple_filename(child, TurnKind::PurpleToMove);
		if (!tablebase_file_looks_valid(fn, n_entries)) {
			throw std::runtime_error("missing dependency table: " + fn);
		}
		deps.child_purple_kplus_ppminus_p_turn = load_tablebase_hex_lines(fn, n_entries);
	}

	// Purple captures normal blue: needs child if pb>=2.
	if (key.pb >= 2) {
		PurpleMaterialKey child{ key.k, static_cast<uint8_t>(key.pb - 1), key.pr, key.pp };
		validate_key(child);
		uint64_t n_entries = states_for_counts(child.pb, child.pr, child.pp);
		std::string fn = make_purple_filename(child, TurnKind::NormalToMove);
		if (!tablebase_file_looks_valid(fn, n_entries)) {
			throw std::runtime_error("missing dependency table: " + fn);
		}
		deps.child_normal_pbminus_n_turn = load_tablebase_hex_lines(fn, n_entries);
	}

	// Purple captures normal red: needs child if pr>=2.
	if (key.pr >= 2) {
		PurpleMaterialKey child{ key.k, key.pb, static_cast<uint8_t>(key.pr - 1), key.pp };
		validate_key(child);
		uint64_t n_entries = states_for_counts(child.pb, child.pr, child.pp);
		std::string fn = make_purple_filename(child, TurnKind::NormalToMove);
		if (!tablebase_file_looks_valid(fn, n_entries)) {
			throw std::runtime_error("missing dependency table: " + fn);
		}
		deps.child_normal_prminus_n_turn = load_tablebase_hex_lines(fn, n_entries);
	}

	return deps;
}

// -----------------------------------------------------------------------------
//  Builder (pair iterative depth propagation)
// -----------------------------------------------------------------------------

struct PurpleBuiltTables {
	std::vector<uint8_t> tb_normal; // Normal-to-move, key.k
	std::vector<uint8_t> tb_purple; // Purple-to-move, key.k
};

static PurpleBuiltTables build_purple_tables_iterative_pair_openmp(const PurpleMaterialKey& key, const PurpleDeps& deps, int progress_sec) {
	validate_key(key);
	const uint64_t entries = states_for_counts(key.pb, key.pr, key.pp);

	PurpleBuiltTables out;
	out.tb_normal.assign(static_cast<size_t>(entries), 0);
	out.tb_purple.assign(static_cast<size_t>(entries), 0);

	std::vector<uint8_t> next_normal(entries, 0);
	std::vector<uint8_t> next_purple(entries, 0);

	// For convenience
	const std::vector<uint8_t>* dep_normal_capture_purple_child = nullptr;
	if (deps.child_purple_kplus_ppminus_p_turn) dep_normal_capture_purple_child = &*deps.child_purple_kplus_ppminus_p_turn;

	const std::vector<uint8_t>* dep_purple_capture_blue_child = nullptr;
	if (deps.child_normal_pbminus_n_turn) dep_purple_capture_blue_child = &*deps.child_normal_pbminus_n_turn;

	const std::vector<uint8_t>* dep_purple_capture_red_child = nullptr;
	if (deps.child_normal_prminus_n_turn) dep_purple_capture_red_child = &*deps.child_normal_prminus_n_turn;

	const auto build_start = Clock::now();
	auto last_progress = build_start;

	for (uint8_t depth = 1; depth <= TB_MAX_DEPTH; ++depth) {
		// Normal-to-move update
#pragma omp parallel for schedule(dynamic)
		for (int64_t idx = 0; idx < static_cast<int64_t>(entries); ++idx) {
			const uint8_t cur_v = out.tb_normal[static_cast<size_t>(idx)];
			if (cur_v != 0) {
				next_normal[static_cast<size_t>(idx)] = cur_v;
				continue;
			}

			CanonicalPosition pos = unrank_purple_normal_turn(idx, key.pb, key.pr, key.pp);

			if (normal_immediate_win(pos)) {
				next_normal[static_cast<size_t>(idx)] = 1;
				continue;
			}
			if (normal_immediate_loss(pos, key.k)) {
				next_normal[static_cast<size_t>(idx)] = 2;
				continue;
			}

			std::array<Move, 32> moves;
			int mcnt = pos.bb_player.gen_moves(/*opp_red=*/0, /*opp_blue=*/pos.bb_opponent.bb_blue, moves);

			// Depth-1 immediate win checks
			if (depth == 1) {
				bool win_now = false;
				for (int mi = 0; mi < mcnt; ++mi) {
					const Move& m = moves[mi];
					if (m.if_capture_blue()) {
						// Captured a purple piece.
						if (key.k + 1 >= 4) {
							// Capturing the 4th red => immediate loss, not win.
							continue;
						}
						if (key.pp == 1) {
							// Capturing the last purple piece => treat as immediate win.
							win_now = true;
							break;
						}
					}
				}
				if (win_now) {
					next_normal[static_cast<size_t>(idx)] = 1;
					continue;
				}
			}

			if (depth & 1) {
				// Winning depth: exists child == depth-1 (even)
				bool found = false;
				for (int mi = 0; mi < mcnt; ++mi) {
					const Move& m = moves[mi];
					uint8_t childv = 0;

					if (m.if_capture_blue()) {
						// Normal captures purple.
						if (key.k + 1 >= 4) {
							// Capturing the 4th red => opponent (Purple) wins in 1.
							childv = 1;
						}
						else if (key.pp == 1) {
							// Capturing last purple would have been depth==1 win, already handled.
							childv = 0;
						}
						else {
							if (!dep_normal_capture_purple_child) {
								throw std::runtime_error("internal: missing dep for normal capture purple");
							}
							CanonicalPosition nxt = do_move(pos, m);
							uint64_t cidx = rank_purple_purple_turn(nxt);
							childv = (*dep_normal_capture_purple_child)[static_cast<size_t>(cidx)];
						}
					}
					else {
						// Non-capture: same material, purple-to-move.
						CanonicalPosition nxt = do_move(pos, m);
						uint64_t cidx = rank_purple_purple_turn(nxt);
						childv = out.tb_purple[static_cast<size_t>(cidx)];
					}

					if (childv == depth - 1 && is_loss_value(childv)) {
						found = true;
						break;
					}
				}
				next_normal[static_cast<size_t>(idx)] = found ? depth : 0;
			}
			else {
				// Losing depth: all children are odd and max odd == depth-1.
				bool forced = true;
				uint8_t max_odd = 0;
				for (int mi = 0; mi < mcnt; ++mi) {
					const Move& m = moves[mi];
					uint8_t childv = 0;

					if (m.if_capture_blue()) {
						// Normal captures purple.
						if (key.k + 1 >= 4) {
							// You lose immediately.
							childv = 1;
						}
						else if (key.pp == 1) {
							// Capturing last purple would win immediately (depth==1), so not forced loss.
							forced = false;
							break;
						}
						else {
							if (!dep_normal_capture_purple_child) {
								throw std::runtime_error("internal: missing dep for normal capture purple");
							}
							CanonicalPosition nxt = do_move(pos, m);
							uint64_t cidx = rank_purple_purple_turn(nxt);
							childv = (*dep_normal_capture_purple_child)[static_cast<size_t>(cidx)];
						}
					}
					else {
						CanonicalPosition nxt = do_move(pos, m);
						uint64_t cidx = rank_purple_purple_turn(nxt);
						childv = out.tb_purple[static_cast<size_t>(cidx)];
					}

					if (childv == 0) {
						forced = false;
						break;
					}
					if (is_loss_value(childv)) {
						// Current player has a winning move -> not forced loss.
						forced = false;
						break;
					}
					// childv is odd
					if (childv > max_odd) max_odd = childv;
				}
				next_normal[static_cast<size_t>(idx)] = (forced && max_odd == depth - 1) ? depth : 0;
			}
		}

		// Purple-to-move update
#pragma omp parallel for schedule(dynamic)
		for (int64_t idx = 0; idx < static_cast<int64_t>(entries); ++idx) {
			const uint8_t cur_v = out.tb_purple[static_cast<size_t>(idx)];
			if (cur_v != 0) {
				next_purple[static_cast<size_t>(idx)] = cur_v;
				continue;
			}

			CanonicalPosition pos = unrank_purple_purple_turn(idx, key.pb, key.pr, key.pp);

			if (purple_immediate_win(pos, key.k)) {
				next_purple[static_cast<size_t>(idx)] = 1;
				continue;
			}
			if (purple_immediate_loss(pos, key.k)) {
				next_purple[static_cast<size_t>(idx)] = 2;
				continue;
			}

			std::array<Move, 32> moves;
			int mcnt = pos.bb_player.gen_moves(/*opp_red=*/pos.bb_opponent.bb_red, /*opp_blue=*/pos.bb_opponent.bb_blue, moves);

			if (depth == 1) {
				bool win_now = false;
				for (int mi = 0; mi < mcnt; ++mi) {
					const Move& m = moves[mi];
					if (m.if_capture_blue()) {
						// Purple captures a normal blue.
						if (key.pb == 1) {
							win_now = true;
							break;
						}
					}
				}
				if (win_now) {
					next_purple[static_cast<size_t>(idx)] = 1;
					continue;
				}
			}

			if (depth & 1) {
				bool found = false;
				for (int mi = 0; mi < mcnt; ++mi) {
					const Move& m = moves[mi];
					uint8_t childv = 0;

					if (m.if_capture_blue()) {
						// capture normal blue
						if (key.pb == 1) {
							// immediate win handled at depth==1
							childv = 0;
						}
						else {
							if (!dep_purple_capture_blue_child) {
								throw std::runtime_error("internal: missing dep for purple capture blue");
							}
							CanonicalPosition nxt = do_move(pos, m);
							uint64_t cidx = rank_purple_normal_turn(nxt);
							childv = (*dep_purple_capture_blue_child)[static_cast<size_t>(cidx)];
						}
					}
					else if (m.if_capture_red()) {
						// capture normal red
						if (key.pr == 1) {
							// Capturing last normal red => Normal wins in 1.
							childv = 1;
						}
						else {
							if (!dep_purple_capture_red_child) {
								throw std::runtime_error("internal: missing dep for purple capture red");
							}
							CanonicalPosition nxt = do_move(pos, m);
							uint64_t cidx = rank_purple_normal_turn(nxt);
							childv = (*dep_purple_capture_red_child)[static_cast<size_t>(cidx)];
						}
					}
					else {
						// non-capture
						CanonicalPosition nxt = do_move(pos, m);
						uint64_t cidx = rank_purple_normal_turn(nxt);
						childv = out.tb_normal[static_cast<size_t>(cidx)];
					}

					if (childv == depth - 1 && is_loss_value(childv)) {
						found = true;
						break;
					}
				}
				next_purple[static_cast<size_t>(idx)] = found ? depth : 0;
			}
			else {
				bool forced = true;
				uint8_t max_odd = 0;
				for (int mi = 0; mi < mcnt; ++mi) {
					const Move& m = moves[mi];
					uint8_t childv = 0;

					if (m.if_capture_blue()) {
						// capture normal blue
						if (key.pb == 1) {
							// would have been immediate win, so if exists it's not forced loss.
							forced = false;
							break;
						}
						else {
							if (!dep_purple_capture_blue_child) {
								throw std::runtime_error("internal: missing dep for purple capture blue");
							}
							CanonicalPosition nxt = do_move(pos, m);
							uint64_t cidx = rank_purple_normal_turn(nxt);
							childv = (*dep_purple_capture_blue_child)[static_cast<size_t>(cidx)];
						}
					}
					else if (m.if_capture_red()) {
						// capture normal red
						if (key.pr == 1) {
							// Normal wins in 1.
							childv = 1;
						}
						else {
							if (!dep_purple_capture_red_child) {
								throw std::runtime_error("internal: missing dep for purple capture red");
							}
							CanonicalPosition nxt = do_move(pos, m);
							uint64_t cidx = rank_purple_normal_turn(nxt);
							childv = (*dep_purple_capture_red_child)[static_cast<size_t>(cidx)];
						}
					}
					else {
						CanonicalPosition nxt = do_move(pos, m);
						uint64_t cidx = rank_purple_normal_turn(nxt);
						childv = out.tb_normal[static_cast<size_t>(cidx)];
					}

					if (childv == 0) {
						forced = false;
						break;
					}
					if (is_loss_value(childv)) {
						forced = false;
						break;
					}
					if (childv > max_odd) max_odd = childv;
				}
				next_purple[static_cast<size_t>(idx)] = (forced && max_odd == depth - 1) ? depth : 0;
			}
		}

		out.tb_normal.swap(next_normal);
		out.tb_purple.swap(next_purple);

		if (progress_sec > 0) {
			const auto now = Clock::now();
			if (now - last_progress >= std::chrono::seconds(progress_sec)) {
				std::ostringstream oss;
				oss << "[BUILD] k=" << int(key.k)
					<< " pb=" << int(key.pb)
					<< " pr=" << int(key.pr)
					<< " pp=" << int(key.pp)
					<< " depth=" << int(depth) << "/" << int(TB_MAX_DEPTH)
					<< " elapsed=" << format_duration_hms(seconds_between(build_start, now));
				log_line_locked(std::cout, oss.str());
				last_progress = now;
			}
		}
	}

	return out;
}

// -----------------------------------------------------------------------------
//  Final backup (for consistency checking)
// -----------------------------------------------------------------------------
static uint8_t clamp_win_plus1(uint8_t child_even) {
	// child_even is even and <=210.
	uint16_t cand = uint16_t(child_even) + 1;
	if (cand > TB_MAX_DEPTH) return 0;
	return static_cast<uint8_t>(cand);
}

static uint8_t clamp_loss_plus1(uint8_t child_odd) {
	// child_odd is odd and <=209.
	uint16_t cand = uint16_t(child_odd) + 1;
	if (cand > TB_MAX_DEPTH) return 0;
	return static_cast<uint8_t>(cand);
}

static uint8_t backup_expected_normal(const PurpleMaterialKey& key,
	uint64_t idx,
	const std::vector<uint8_t>& tb_purple_same,
	const PurpleDeps& deps) {
	CanonicalPosition pos = unrank_purple_normal_turn(idx, key.pb, key.pr, key.pp);

	if (normal_immediate_win(pos)) return 1;
	if (normal_immediate_loss(pos, key.k)) return 2;

	std::array<Move, 32> moves;
	int mcnt = pos.bb_player.gen_moves(0, pos.bb_opponent.bb_blue, moves);

	uint8_t best_win = 0;
	bool any_draw = false;
	uint8_t max_loss = 0;

	for (int mi = 0; mi < mcnt; ++mi) {
		const Move& m = moves[mi];

		// Immediate terminal win moves
		if (m.if_capture_blue()) {
			if (key.k + 1 < 4 && key.pp == 1) {
				return 1;
			}
		}

		uint8_t childv = 0;

		if (m.if_capture_blue()) {
			// capture purple
			if (key.k + 1 >= 4) {
				childv = 1; // opponent wins
			}
			else if (key.pp == 1) {
				childv = 0; // already handled as immediate win
			}
			else {
				if (!deps.child_purple_kplus_ppminus_p_turn) {
					throw std::runtime_error("missing deps.child_purple_kplus_ppminus_p_turn");
				}
				CanonicalPosition nxt = do_move(pos, m);
				uint64_t cidx = rank_purple_purple_turn(nxt);
				childv = (*deps.child_purple_kplus_ppminus_p_turn)[static_cast<size_t>(cidx)];
			}
		}
		else {
			CanonicalPosition nxt = do_move(pos, m);
			uint64_t cidx = rank_purple_purple_turn(nxt);
			childv = tb_purple_same[static_cast<size_t>(cidx)];
		}

		if (childv == 0) {
			any_draw = true;
			continue;
		}
		if (is_loss_value(childv)) {
			uint8_t cand = clamp_win_plus1(childv);
			if (cand == 0) {
				any_draw = true;
			}
			else if (best_win == 0 || cand < best_win) {
				best_win = cand;
			}
		}
		else {
			// child is win for opponent
			uint8_t cand = clamp_loss_plus1(childv);
			if (cand == 0) {
				any_draw = true;
			}
			else if (cand > max_loss) {
				max_loss = cand;
			}
		}
	}

	if (best_win != 0) return best_win;
	if (any_draw) return 0;
	if (max_loss != 0) return max_loss;
	// Should not happen because we always have moves.
	throw std::runtime_error("backup_expected_normal: no moves?");
}

static uint8_t backup_expected_purple(const PurpleMaterialKey& key,
	uint64_t idx,
	const std::vector<uint8_t>& tb_normal_same,
	const PurpleDeps& deps) {
	CanonicalPosition pos = unrank_purple_purple_turn(idx, key.pb, key.pr, key.pp);

	if (purple_immediate_win(pos, key.k)) return 1;
	if (purple_immediate_loss(pos, key.k)) return 2;

	std::array<Move, 32> moves;
	int mcnt = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);

	uint8_t best_win = 0;
	bool any_draw = false;
	uint8_t max_loss = 0;

	for (int mi = 0; mi < mcnt; ++mi) {
		const Move& m = moves[mi];

		if (m.if_capture_blue()) {
			if (key.pb == 1) {
				return 1; // capture last blue
			}
		}

		uint8_t childv = 0;

		if (m.if_capture_blue()) {
			// capture normal blue
			if (key.pb == 1) {
				childv = 0; // already handled
			}
			else {
				if (!deps.child_normal_pbminus_n_turn) {
					throw std::runtime_error("missing deps.child_normal_pbminus_n_turn");
				}
				CanonicalPosition nxt = do_move(pos, m);
				uint64_t cidx = rank_purple_normal_turn(nxt);
				childv = (*deps.child_normal_pbminus_n_turn)[static_cast<size_t>(cidx)];
			}
		}
		else if (m.if_capture_red()) {
			// capture normal red
			if (key.pr == 1) {
				childv = 1; // normal wins
			}
			else {
				if (!deps.child_normal_prminus_n_turn) {
					throw std::runtime_error("missing deps.child_normal_prminus_n_turn");
				}
				CanonicalPosition nxt = do_move(pos, m);
				uint64_t cidx = rank_purple_normal_turn(nxt);
				childv = (*deps.child_normal_prminus_n_turn)[static_cast<size_t>(cidx)];
			}
		}
		else {
			CanonicalPosition nxt = do_move(pos, m);
			uint64_t cidx = rank_purple_normal_turn(nxt);
			childv = tb_normal_same[static_cast<size_t>(cidx)];
		}

		if (childv == 0) {
			any_draw = true;
			continue;
		}
		if (is_loss_value(childv)) {
			uint8_t cand = clamp_win_plus1(childv);
			if (cand == 0) {
				any_draw = true;
			}
			else if (best_win == 0 || cand < best_win) {
				best_win = cand;
			}
		}
		else {
			uint8_t cand = clamp_loss_plus1(childv);
			if (cand == 0) {
				any_draw = true;
			}
			else if (cand > max_loss) {
				max_loss = cand;
			}
		}
	}

	if (best_win != 0) return best_win;
	if (any_draw) return 0;
	if (max_loss != 0) return max_loss;
	throw std::runtime_error("backup_expected_purple: no moves?");
}


static void check_purple_tables_consistency_openmp(const PurpleMaterialKey& key,
	const std::vector<uint8_t>& tb_normal,
	const std::vector<uint8_t>& tb_purple,
	const PurpleDeps& deps,
	int progress_sec) {
	validate_key(key);

	const uint64_t entries = states_for_counts(key.pb, key.pr, key.pp);
	if (tb_normal.size() != entries || tb_purple.size() != entries) {
		throw std::runtime_error("table size mismatch in consistency check");
	}

	// Check normal-to-move table
	{
		std::atomic<uint64_t> bad_idx{ std::numeric_limits<uint64_t>::max() };
		std::atomic<uint64_t> processed{ 0 };
		{
			std::ostringstream lbl;
			lbl << "check(N-turn) k=" << int(key.k)
				<< " pb=" << int(key.pb)
				<< " pr=" << int(key.pr)
				<< " pp=" << int(key.pp);
			PeriodicProgressReporter reporter(lbl.str(), entries, &processed, progress_sec);

#pragma omp parallel
			{
				uint64_t local = 0;
#pragma omp for schedule(dynamic)
				for (int64_t i = 0; i < static_cast<int64_t>(entries); ++i) {
					if (bad_idx.load(std::memory_order_relaxed) != std::numeric_limits<uint64_t>::max()) continue;
					uint8_t expected = backup_expected_normal(key, i, tb_purple, deps);
					uint8_t actual = tb_normal[static_cast<size_t>(i)];
					if (expected != actual) {
						bad_idx.store(static_cast<uint64_t>(i), std::memory_order_relaxed);
					}
					++local;
					if ((local & 0x3FFFu) == 0) {
						processed.fetch_add(local, std::memory_order_relaxed);
						local = 0;
					}
				}
				if (local) processed.fetch_add(local, std::memory_order_relaxed);
			}

			reporter.stop();
		}

		uint64_t bi = bad_idx.load(std::memory_order_relaxed);
		if (bi != std::numeric_limits<uint64_t>::max()) {
			uint8_t expected = backup_expected_normal(key, bi, tb_purple, deps);
			uint8_t actual = tb_normal[static_cast<size_t>(bi)];
			std::cerr << "Consistency check failed (Normal-to-move): idx=" << bi
				<< " expected=" << int(expected) << " actual=" << int(actual)
				<< " for k=" << int(key.k) << " pb=" << int(key.pb)
				<< " pr=" << int(key.pr) << " pp=" << int(key.pp) << "\n";
			throw std::runtime_error("consistency check failed (Normal-to-move)");
		}
	}

	// Check purple-to-move table
	{
		std::atomic<uint64_t> bad_idx{ std::numeric_limits<uint64_t>::max() };
		std::atomic<uint64_t> processed{ 0 };
		{
			std::ostringstream lbl;
			lbl << "check(P-turn) k=" << int(key.k)
				<< " pb=" << int(key.pb)
				<< " pr=" << int(key.pr)
				<< " pp=" << int(key.pp);
			PeriodicProgressReporter reporter(lbl.str(), entries, &processed, progress_sec);

#pragma omp parallel
			{
				uint64_t local = 0;
#pragma omp for schedule(dynamic)
				for (int64_t i = 0; i < static_cast<int64_t>(entries); ++i) {
					if (bad_idx.load(std::memory_order_relaxed) != std::numeric_limits<uint64_t>::max()) continue;
					uint8_t expected = backup_expected_purple(key, i, tb_normal, deps);
					uint8_t actual = tb_purple[static_cast<size_t>(i)];
					if (expected != actual) {
						bad_idx.store(static_cast<uint64_t>(i), std::memory_order_relaxed);
					}
					++local;
					if ((local & 0x3FFFu) == 0) {
						processed.fetch_add(local, std::memory_order_relaxed);
						local = 0;
					}
				}
				if (local) processed.fetch_add(local, std::memory_order_relaxed);
			}

			reporter.stop();
		}

		uint64_t bi = bad_idx.load(std::memory_order_relaxed);
		if (bi != std::numeric_limits<uint64_t>::max()) {
			uint8_t expected = backup_expected_purple(key, bi, tb_normal, deps);
			uint8_t actual = tb_purple[static_cast<size_t>(bi)];
			std::cerr << "Consistency check failed (Purple-to-move): idx=" << bi
				<< " expected=" << int(expected) << " actual=" << int(actual)
				<< " for k=" << int(key.k) << " pb=" << int(key.pb)
				<< " pr=" << int(key.pr) << " pp=" << int(key.pp) << "\n";
			throw std::runtime_error("consistency check failed (Purple-to-move)");
		}
	}
}

// -----------------------------------------------------------------------------
//  Ensure/build function (builds both turns for a material key)
// -----------------------------------------------------------------------------

struct RuntimeOptions {
	int progress_sec = 0; // 0 disables periodic progress logs
};

static void ensure_purple_tables_for_key(const PurpleMaterialKey& key, const RuntimeOptions& opt) {
	validate_key(key);
	const uint64_t entries = states_for_counts(key.pb, key.pr, key.pp);

	const std::string fnN = make_purple_filename(key, TurnKind::NormalToMove);
	const std::string fnP = make_purple_filename(key, TurnKind::PurpleToMove);
	const std::string bnN = make_purple_filename_bin(key, TurnKind::NormalToMove);
	const std::string bnP = make_purple_filename_bin(key, TurnKind::PurpleToMove);

	const bool haveN = tablebase_file_looks_valid(fnN, entries);
	const bool haveP = tablebase_file_looks_valid(fnP, entries);
	const bool haveBN = tablebase_bin_looks_valid(bnN, entries);
	const bool haveBP = tablebase_bin_looks_valid(bnP, entries);

	const auto t_total0 = Clock::now();

	// Load dependencies (must already exist)
	const auto t_deps0 = Clock::now();
	PurpleDeps deps = load_dependencies_or_throw(key);
	const auto t_deps1 = Clock::now();
	const double deps_sec = seconds_between(t_deps0, t_deps1);

	if (haveN && haveP) {
		std::cout << "[SKIP] " << fnN << " and " << fnP << " exist; loading and checking..." << std::endl;

		const auto t_load0 = Clock::now();
		std::vector<uint8_t> tbN = load_tablebase_hex_lines(fnN, entries);
		std::vector<uint8_t> tbP = load_tablebase_hex_lines(fnP, entries);
		const auto t_load1 = Clock::now();
		const double load_sec = seconds_between(t_load0, t_load1);

		const auto t_check0 = Clock::now();
		check_purple_tables_consistency_openmp(key, tbN, tbP, deps, opt.progress_sec);
		const auto t_check1 = Clock::now();
		const double check_sec = seconds_between(t_check0, t_check1);

		const auto t_bin0 = Clock::now();
		// Ensure .bin exists (1 byte/entry) after verified .txt tables.
		if (!haveBN) {
			std::cout << "[BIN] write " << bnN << std::endl;
			write_tablebase_bin(tbN, bnN);
		}
		if (!haveBP) {
			std::cout << "[BIN] write " << bnP << std::endl;
			write_tablebase_bin(tbP, bnP);
		}
		const auto t_bin1 = Clock::now();
		const double bin_sec = seconds_between(t_bin0, t_bin1);

		const double total_sec = seconds_between(t_total0, Clock::now());
		std::ostringstream oss;
		oss << "[OK] " << fnN << " / " << fnP
			<< " total=" << format_duration_hms(total_sec)
			<< " (deps=" << format_duration_hms(deps_sec)
			<< ", load=" << format_duration_hms(load_sec)
			<< ", check=" << format_duration_hms(check_sec)
			<< ", bin=" << format_duration_hms(bin_sec) << ")";
		log_line_locked(std::cout, oss.str());
		return;
	}

	if (haveN != haveP) {
		std::cout << "[WARN] Incomplete .txt pair detected (" << fnN << " / " << fnP
			<< "); rebuilding both." << std::endl;
	}

	std::cout << "[BUILD] k=" << int(key.k)
		<< " pb=" << int(key.pb)
		<< " pr=" << int(key.pr)
		<< " pp=" << int(key.pp)
		<< " entries=" << entries << std::endl;

	const auto t_build0 = Clock::now();
	PurpleBuiltTables built = build_purple_tables_iterative_pair_openmp(key, deps, opt.progress_sec);
	const auto t_build1 = Clock::now();
	const double build_sec = seconds_between(t_build0, t_build1);

	const auto t_write0 = Clock::now();
	write_tablebase_hex_lines(built.tb_normal, fnN);
	write_tablebase_hex_lines(built.tb_purple, fnP);
	const auto t_write1 = Clock::now();
	const double write_sec = seconds_between(t_write0, t_write1);

	// Per your requested workflow: reload from disk for the consistency check.
	built.tb_normal.clear();
	built.tb_purple.clear();
	built.tb_normal.shrink_to_fit();
	built.tb_purple.shrink_to_fit();

	const auto t_reload0 = Clock::now();
	std::vector<uint8_t> tbN = load_tablebase_hex_lines(fnN, entries);
	std::vector<uint8_t> tbP = load_tablebase_hex_lines(fnP, entries);
	const auto t_reload1 = Clock::now();
	const double reload_sec = seconds_between(t_reload0, t_reload1);

	const auto t_check0 = Clock::now();
	check_purple_tables_consistency_openmp(key, tbN, tbP, deps, opt.progress_sec);
	const auto t_check1 = Clock::now();
	const double check_sec = seconds_between(t_check0, t_check1);

	const auto t_bin0 = Clock::now();
	// Ensure .bin exists after verified .txt tables.
	if (!tablebase_bin_looks_valid(bnN, entries)) {
		std::cout << "[BIN] write " << bnN << std::endl;
		write_tablebase_bin(tbN, bnN);
	}
	if (!tablebase_bin_looks_valid(bnP, entries)) {
		std::cout << "[BIN] write " << bnP << std::endl;
		write_tablebase_bin(tbP, bnP);
	}
	const auto t_bin1 = Clock::now();
	const double bin_sec = seconds_between(t_bin0, t_bin1);

	const double total_sec = seconds_between(t_total0, Clock::now());
	std::ostringstream oss;
	oss << "[OK] " << fnN << " / " << fnP
		<< " total=" << format_duration_hms(total_sec)
		<< " (deps=" << format_duration_hms(deps_sec)
		<< ", build=" << format_duration_hms(build_sec)
		<< ", write=" << format_duration_hms(write_sec)
		<< ", reload=" << format_duration_hms(reload_sec)
		<< ", check=" << format_duration_hms(check_sec)
		<< ", bin=" << format_duration_hms(bin_sec) << ")";
	log_line_locked(std::cout, oss.str());
}

// -----------------------------------------------------------------------------
//  CLI
// -----------------------------------------------------------------------------

struct ProgramOptions {
	int total_min = 3;
	int total_max = 6;
	RuntimeOptions rt{};
	std::optional<PurpleMaterialKey> single_key{};
};

static void print_usage(const char* argv0) {
	std::cout
		<< "Usage:\n"
		<< "  " << argv0 << " [options]\n\n"
		<< "Modes:\n"
		<< "  (default) Build all purple tables for totals in [--total-min, --total-max].\n"
		<< "  --key pb pr pp k        Build/check a single material key.\n\n"
		<< "Options:\n"
		<< "  --total-min N           Minimum total pieces (default: 3).\n"
		<< "  --total-max N           Maximum total pieces (default: 6, max meaningful: 9).\n"
		<< "  --progress-sec S        Emit periodic progress logs every S seconds (default: 0=disabled).\n"
		<< "  --help, -h              Show this help.\n\n"
		<< "Compatibility:\n"
		<< "  If invoked with four positional integers (pb pr pp k), this is treated as --key.\n\n"
		<< "Examples:\n"
		<< "  " << argv0 << " --total-max 6 --progress-sec 30\n"
		<< "  " << argv0 << " --key 1 1 7 2\n";
}

static ProgramOptions parse_args_or_throw(int argc, char** argv) {
	ProgramOptions opt;

	// Legacy positional mode: pb pr pp k
	if (argc == 5) {
		bool any_flag = false;
		for (int i = 1; i < argc; ++i) {
			if (argv[i] && argv[i][0] == '-') { any_flag = true; break; }
		}
		if (!any_flag) {
			PurpleMaterialKey key;
			key.pb = static_cast<uint8_t>(std::stoi(argv[1]));
			key.pr = static_cast<uint8_t>(std::stoi(argv[2]));
			key.pp = static_cast<uint8_t>(std::stoi(argv[3]));
			key.k = static_cast<uint8_t>(std::stoi(argv[4]));
			opt.single_key = key;
			return opt;
		}
	}

	for (int i = 1; i < argc; ++i) {
		const std::string_view a(argv[i]);
		if (a == "--help" || a == "-h") {
			print_usage(argv[0]);
			std::exit(0);
		}
		else if (a == "--total-min") {
			if (i + 1 >= argc) throw std::runtime_error("--total-min requires an integer");
			opt.total_min = std::stoi(argv[++i]);
		}
		else if (a == "--total-max") {
			if (i + 1 >= argc) throw std::runtime_error("--total-max requires an integer");
			opt.total_max = std::stoi(argv[++i]);
		}
		else if (a == "--progress-sec") {
			if (i + 1 >= argc) throw std::runtime_error("--progress-sec requires an integer");
			opt.rt.progress_sec = std::stoi(argv[++i]);
		}
		else if (a == "--key") {
			if (i + 4 >= argc) throw std::runtime_error("--key requires 4 integers: pb pr pp k");
			PurpleMaterialKey key;
			key.pb = static_cast<uint8_t>(std::stoi(argv[++i]));
			key.pr = static_cast<uint8_t>(std::stoi(argv[++i]));
			key.pp = static_cast<uint8_t>(std::stoi(argv[++i]));
			key.k = static_cast<uint8_t>(std::stoi(argv[++i]));
			opt.single_key = key;
		}
		else {
			std::ostringstream oss;
			oss << "unknown argument: " << a;
			throw std::runtime_error(oss.str());
		}
	}

	if (opt.total_min < 0) throw std::runtime_error("--total-min must be >= 0");
	if (opt.total_max < 0) throw std::runtime_error("--total-max must be >= 0");
	if (opt.total_min > opt.total_max) throw std::runtime_error("--total-min must be <= --total-max");
	if (opt.total_max > 9) {
		// Values >9 are not meaningful due to key.total() <= 9 and pp<=9.
		opt.total_max = 9;
	}
	if (opt.rt.progress_sec < 0) throw std::runtime_error("--progress-sec must be >= 0");

	return opt;
}

// -----------------------------------------------------------------------------
//  Main: build all purple tables (default total range: 3..6)
// -----------------------------------------------------------------------------
int main(int argc, char** argv) {
	try {
		// Pre-warm geister_rank_triplet's internal canonical meta before any OpenMP region.
		{
			uint64_t a = 0, b = 0, c = 0;
			unrank_triplet_canon(0, 1, 1, 2, a, b, c);
		}

		const ProgramOptions opt = parse_args_or_throw(argc, argv);

		if (opt.single_key.has_value()) {
			validate_key(*opt.single_key);
			ensure_purple_tables_for_key(*opt.single_key, opt.rt);
			return 0;
		}

		for (int total = opt.total_min; total <= opt.total_max; ++total) {
			for (int k = 3; k >= 0; --k) {
				for (int pb = 1; pb <= 4; ++pb) {
					for (int pr = 1; pr <= 4; ++pr) {
						int pp = total - pb - pr;
						if (pp < 0) continue;
						if (pp > 9) continue;

						PurpleMaterialKey key;
						key.k = static_cast<uint8_t>(k);
						key.pb = static_cast<uint8_t>(pb);
						key.pr = static_cast<uint8_t>(pr);
						key.pp = static_cast<uint8_t>(pp);

						// Prune unreachable / non-needed domains (pp constraints depending on k).
						if (!key_in_domain(key)) continue;

						ensure_purple_tables_for_key(key, opt.rt);
					}
				}
			}
		}

		return 0;
	}
	catch (const std::exception& e) {
		std::cerr << "Fatal: " << e.what() << std::endl;
		return 1;
	}
}
