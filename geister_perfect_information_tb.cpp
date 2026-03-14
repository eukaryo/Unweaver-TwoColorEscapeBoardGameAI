// geister.cpp
//   Perfect-information Geister endgame tablebase generator.

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <limits>
#include <mutex>
#include <string_view>
#include <system_error>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <omp.h>

import tablebase_io;
import geister_rank;
import geister_rank_obsblk;
import geister_core;

// ============================================================
//  Global constants
// ============================================================

constexpr int kMaxDepth = 210;
constexpr int kOmpChunk = 1 << 20; // OpenMP schedule chunk

// ============================================================
//  Runtime configuration (CLI)
// ============================================================

struct RuntimeConfig {
	// Build range:
	int upto_total = 6;               // x_upto_total(upto_total)
	uint16_t start_id = 0;            // inclusive
	uint16_t end_id = UINT16_MAX;     // exclusive (clamped by x_upto_total)
	bool run_single_id = false;
	uint16_t single_id = 0;

	// Performance knobs:
	bool skip_swap_duplicates = true; // Improvement A
	int progress_interval_sec = 30;   // Improvement F (0 disables intra-check progress)
	uint64_t progress_chunk = (1ULL << 20); // work chunk for progress reporting

	// Algorithm:
	int max_depth = kMaxDepth;        // must be <= kMaxDepth (default: 210)

	// Logging:
	bool verbose = true;

	// Debug / compatibility output:
	bool write_txt = false;      // default: do not emit .txt side files
};

static RuntimeConfig g_cfg;

// ============================================================
//  Logging / timing helpers (thread-safe)
// ============================================================

namespace tbutil {

using Clock = std::chrono::steady_clock;
static const Clock::time_point g_program_start = Clock::now();
static std::mutex g_log_mutex;

[[nodiscard]] inline std::string format_hms(double sec) {
	if (sec < 0) sec = 0;
	uint64_t s = (uint64_t)sec;
	const uint64_t h = s / 3600; s %= 3600;
	const uint64_t m = s / 60; s %= 60;
	std::ostringstream oss;
	oss << std::setw(2) << std::setfill('0') << h << ":"
		<< std::setw(2) << std::setfill('0') << m << ":"
		<< std::setw(2) << std::setfill('0') << s;
	return oss.str();
}

template <class... Ts>
inline void log_line(Ts&&... xs) {
	const auto now = Clock::now();
	const double sec = std::chrono::duration<double>(now - g_program_start).count();

	std::lock_guard<std::mutex> lock(g_log_mutex);
	std::cout << "[T+" << format_hms(sec) << "] ";
	(std::cout << ... << xs);
	std::cout << std::endl;
}

[[nodiscard]] inline std::string material_label(uint16_t id, const Count4& c) {
	std::ostringstream oss;
	oss << "id" << std::setw(3) << std::setfill('0') << (int)id
		<< "_pb" << (int)c.pop_pb
		<< "pr" << (int)c.pop_pr
		<< "ob" << (int)c.pop_ob
		<< "or" << (int)c.pop_or;
	return oss.str();
}

[[nodiscard]] inline std::string format_gib(uint64_t bytes) {
	const double gib = (double)bytes / (1024.0 * 1024.0 * 1024.0);
	std::ostringstream oss;
	oss << std::fixed << std::setprecision(2) << gib << " GiB";
	return oss.str();
}

[[nodiscard]] inline bool parse_u64(std::string_view s, uint64_t& out) {
	out = 0;
	const char* b = s.data();
	const char* e = s.data() + s.size();
	auto [ptr, ec] = std::from_chars(b, e, out, 10);
	return (ec == std::errc() && ptr == e);
}

[[nodiscard]] inline uint64_t must_parse_u64(std::string_view s, const char* what) {
	uint64_t v = 0;
	if (!parse_u64(s, v)) {
		std::ostringstream oss;
		oss << "invalid " << what << ": '" << s << "'";
		throw std::runtime_error(oss.str());
	}
	return v;
}

inline void print_usage(const char* argv0) {
	std::cout
		<< "Usage: " << argv0 << " [options]\n"
		<< "\n"
		<< "Options:\n"
		<< "  --upto-total N        Build all material IDs with total pieces <= N (default: 6)\n"
		<< "  --id ID               Build/check a single material ID (and its swap pair)\n"
		<< "  --range A B           Build/check IDs in [A, B] (inclusive)\n"
		<< "  --start-id A          Build/check IDs in [A, end)\n"
		<< "  --end-id B            Build/check IDs in [start, B) (end is exclusive)\n"
		<< "  --no-skip-pairs       Do not skip swap-duplicate IDs (default: skip duplicates)\n"
		<< "  --progress-sec S      Progress print interval during consistency check (default: 30; 0 disables)\n"
		<< "  --write-txt           Also emit legacy/debug .txt files (default: off)\n"
		<< "  --no-write-txt        Do not emit .txt files (explicitly keep default)\n"
		<< "  -h, --help            Show this help\n"
		<< std::flush;
}

} // namespace tbutil


// ============================================================
//  Small error-handling utilities
// ============================================================

[[noreturn]] inline void tb_fail(const char* msg) {
	throw std::runtime_error(msg);
}
inline void tb_ensure(bool ok, const char* msg) {
	if (!ok) tb_fail(msg);
}

// ============================================================
//  Parallel failure flag (do not throw inside OpenMP loops)
// ============================================================

enum class TbFailKind : uint32_t {
	None = 0,
	NoLegalMoves = 1,
	UnexpectedException = 2,
	ValueMismatch = 3,
	InvalidInput = 4,
};

static inline const char* tb_fail_kind_to_cstr(TbFailKind k) noexcept {
	switch (k) {
	case TbFailKind::None: return "None";
	case TbFailKind::NoLegalMoves: return "NoLegalMoves";
	case TbFailKind::UnexpectedException: return "UnexpectedException";
	case TbFailKind::ValueMismatch: return "ValueMismatch";
	case TbFailKind::InvalidInput: return "InvalidInput";
	default: return "Unknown";
	}
}

struct ParallelFailFlag {
	std::atomic<uint64_t> first_bad_idx{ UINT64_MAX };
	std::atomic<uint32_t> kind{ (uint32_t)TbFailKind::None };

	inline bool has_failed() const noexcept {
		return first_bad_idx.load(std::memory_order_relaxed) != UINT64_MAX;
	}

	inline void fail(uint64_t idx, TbFailKind k) noexcept {
		uint64_t expected = UINT64_MAX;
		if (first_bad_idx.compare_exchange_strong(expected, idx, std::memory_order_relaxed)) {
			kind.store((uint32_t)k, std::memory_order_relaxed);
		}
	}

	inline uint64_t bad_idx() const noexcept {
		return first_bad_idx.load(std::memory_order_relaxed);
	}

	inline TbFailKind bad_kind() const noexcept {
		return (TbFailKind)kind.load(std::memory_order_relaxed);
	}
};

// ============================================================
//  Tablebase filename and file check
// ============================================================

[[nodiscard]] inline std::string make_tablebase_filename(uint16_t id, const Count4& c) {
	std::ostringstream oss;
	oss << "id" << std::setw(3) << std::setfill('0') << (int)id
		<< "_pb" << (int)c.pop_pb
		<< "pr" << (int)c.pop_pr
		<< "ob" << (int)c.pop_ob
		<< "or" << (int)c.pop_or
		<< ".txt";
	return oss.str();
}

[[nodiscard]] inline std::string make_tablebase_filename_bin(uint16_t id, const Count4& c) {
	std::ostringstream oss;
	oss << "id" << std::setw(3) << std::setfill('0') << (int)id
		<< "_pb" << (int)c.pop_pb
		<< "pr" << (int)c.pop_pr
		<< "ob" << (int)c.pop_ob
		<< "or" << (int)c.pop_or
		<< "_obsblk.bin";
	return oss.str();
}


[[nodiscard]] inline bool tablebase_file_looks_valid(const std::string& filename, uint64_t expected_entries) noexcept {
	// Delegate to module: tablebase_io
	return tbio::tablebase_file_looks_valid(std::filesystem::path(filename), expected_entries);
}

[[nodiscard]] inline bool tablebase_bin_looks_valid(const std::string& filename, uint64_t expected_entries) noexcept {
	// Delegate to module: tablebase_io
	return tbio::tablebase_bin_looks_valid(std::filesystem::path(filename), expected_entries);
}

[[nodiscard]] inline std::vector<uint8_t> load_tablebase_bin(const std::string& filename, const uint64_t expected_entries_u64) {
	return tbio::load_tablebase_bin_streaming(
		std::filesystem::path(filename),
		expected_entries_u64,
		static_cast<std::uint8_t>(kMaxDepth));
}

[[nodiscard]] inline std::vector<uint8_t> load_tablebase_bin_obsblk_as_legacy(
	const std::string& filename,
	const Count4& c,
	const uint64_t expected_entries_u64)
{
	auto obsblk = load_tablebase_bin(filename, expected_entries_u64);
	std::vector<uint8_t> legacy((size_t)expected_entries_u64, 0);

	for (uint64_t legacy_idx = 0; legacy_idx < expected_entries_u64; ++legacy_idx) {
		uint64_t pb = 0, pr = 0, ob = 0, oc = 0;
		unrank_geister_perfect_information(legacy_idx, c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or, pb, pr, ob, oc);
		const uint64_t obsblk_idx = rank_geister_perfect_information_obsblk(pb, pr, ob, oc);
		legacy[(size_t)legacy_idx] = obsblk[(size_t)obsblk_idx];
	}
	return legacy;
}

// ============================================================
//  Text table I/O (streaming only)
//  - Supports "xx\n" or "xx\r\n"
//  - Writer emits "xx\n" in binary mode.
//  - Implementation lives in module: tablebase_io
// ============================================================

[[nodiscard]] inline std::vector<uint8_t> load_tablebase_hex_lines(
	const std::string& filename,
	const uint64_t expected_entries_u64)
{
	return tbio::load_tablebase_hex_lines_streaming(
		std::filesystem::path(filename),
		expected_entries_u64,
		static_cast<std::uint8_t>(kMaxDepth));
}

inline void write_tablebase_hex_lines(const std::vector<uint8_t>& tb, const std::string& filename) {
	tbio::write_tablebase_hex_lines_streaming(tb, std::filesystem::path(filename));
}

inline void write_tablebase_bin_obsblk(const std::vector<uint8_t>& tb, const Count4& c, const std::string& filename) {
	const std::uint64_t legacy_entries = states_of(c);
	const std::uint64_t obsblk_entries = obsblk_states_for_counts(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
	if (tb.size() != legacy_entries) {
		throw std::runtime_error("write_tablebase_bin_obsblk: source table size mismatch");
	}
	if (legacy_entries != obsblk_entries) {
		throw std::runtime_error("write_tablebase_bin_obsblk: legacy/obsblk entry count mismatch");
	}

	const std::filesystem::path dst(filename);
	const std::filesystem::path tmp = dst.string() + ".tmp";
	std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
	if (!ofs) throw std::runtime_error("failed to open obsblk output file for write");

	constexpr std::size_t kChunk = 1u << 20;
	std::vector<std::uint8_t> buf;
	buf.reserve(kChunk);

	std::uint64_t bb_player_blue = 0;
	std::uint64_t bb_player_red = 0;
	std::uint64_t bb_opponent_blue = 0;
	std::uint64_t bb_opponent_red = 0;

	for (std::uint64_t obsblk_idx = 0; obsblk_idx < obsblk_entries; ++obsblk_idx) {
		unrank_geister_perfect_information_obsblk(
			obsblk_idx,
			static_cast<int>(c.pop_pb),
			static_cast<int>(c.pop_pr),
			static_cast<int>(c.pop_ob),
			static_cast<int>(c.pop_or),
			bb_player_blue,
			bb_player_red,
			bb_opponent_blue,
			bb_opponent_red);
		const std::uint64_t legacy_idx = rank_geister_perfect_information(
			bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);
		buf.push_back(tb[static_cast<std::size_t>(legacy_idx)]);
		if (buf.size() == kChunk) {
			ofs.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
			if (!ofs) throw std::runtime_error("failed while writing obsblk output file");
			buf.clear();
		}
	}

	if (!buf.empty()) {
		ofs.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
		if (!ofs) throw std::runtime_error("failed while finalizing obsblk output file");
	}
	ofs.close();
	if (!ofs) throw std::runtime_error("failed to close obsblk output file");

	std::error_code ec;
	std::filesystem::remove(dst, ec);
	ec.clear();
	std::filesystem::rename(tmp, dst, ec);
	if (ec) throw std::runtime_error("rename failed: " + ec.message());
}

// ============================================================
//  Lower-dependency tablebase cache (in-memory)
// ============================================================

struct TableView {
	const uint8_t* p = nullptr;
	uint64_t n = 0;

	[[nodiscard]] bool empty() const noexcept { return p == nullptr || n == 0; }
	[[nodiscard]] uint8_t get(uint64_t idx) const noexcept { return p[(size_t)idx]; }
};

static inline TableView make_view(const std::vector<uint8_t>& v) noexcept {
	if (v.empty()) return {};
	return TableView{ v.data(), (uint64_t)v.size() };
}

struct DepCache {
	std::unordered_map<uint16_t, std::vector<uint8_t>> cache;

	[[nodiscard]] TableView get(uint16_t id, uint64_t expected) {
		auto it = cache.find(id);
		if (it != cache.end()) {
			tb_ensure((uint64_t)it->second.size() == expected, "cached dep size mismatch");
			return make_view(it->second);
		}
		return {};
	}

	void put(uint16_t id, std::vector<uint8_t>&& v, uint64_t expected) {
		tb_ensure((uint64_t)v.size() == expected, "dep size mismatch at put()");
		cache.emplace(id, std::move(v));
	}
};

struct LowerDeps {
	std::vector<uint8_t> cap_blue;
	std::vector<uint8_t> cap_red;
};

struct LowerDepsView {
	TableView cap_blue;
	TableView cap_red;
};

static inline LowerDepsView make_view(const LowerDeps& d) noexcept {
	return LowerDepsView{ make_view(d.cap_blue), make_view(d.cap_red) };
}

// ============================================================
//  Load lower-table dependencies for a given material (if needed)
// ============================================================

LowerDeps load_lower_deps_for(const Count4& c, DepCache& dep_cache) {
	LowerDeps deps{};

	auto load_one_dep = [&](const Count4& dep) -> std::vector<uint8_t> {
		const uint16_t dep_id = material_id_of(dep);
		const uint64_t entries = states_of(dep);
		const std::string fn_bin = make_tablebase_filename_bin(dep_id, dep);
		const std::string fn_txt = make_tablebase_filename(dep_id, dep);

		TableView cached = dep_cache.get(dep_id, entries);
		if (!cached.empty()) {
			return std::vector<uint8_t>(cached.p, cached.p + (size_t)cached.n);
		}

		std::vector<uint8_t> v;
		if (tablebase_bin_looks_valid(fn_bin, entries)) {
			v = load_tablebase_bin_obsblk_as_legacy(fn_bin, dep, entries);
		}
		else if (tablebase_file_looks_valid(fn_txt, entries)) {
			v = load_tablebase_hex_lines(fn_txt, entries);
		}
		if (!v.empty()) {
			dep_cache.put(dep_id, std::vector<uint8_t>(v), entries);
		}
		return v;
	};

	// Capture blue decreases opponent blue count: (pb,pr,ob,or) -> (ob-1,or,pb,pr)
	if (c.pop_ob > 1) {
		const Count4 dep = Count4{ (uint8_t)(c.pop_ob - 1), c.pop_or, c.pop_pb, c.pop_pr };
		deps.cap_blue = load_one_dep(dep);
	}

	// Capture red decreases opponent red count: (pb,pr,ob,or) -> (ob,or-1,pb,pr)
	if (c.pop_or > 1) {
		const Count4 dep = Count4{ c.pop_ob, (uint8_t)(c.pop_or - 1), c.pop_pb, c.pop_pr };
		deps.cap_red = load_one_dep(dep);
	}

	return deps;
}

// ============================================================
//  Child index helper: rank after making a move
// ============================================================

inline uint64_t child_index_after_move(const perfect_information_geister& pos, const move m) {
	perfect_information_geister next = pos;
	perfect_information_geister::do_move(m, next);

	const uint64_t code = rank_geister_perfect_information(
		next.bb_player.bb_blue, next.bb_player.bb_red,
		next.bb_opponent.bb_blue, next.bb_opponent.bb_red);

	return code;
}

// ============================================================
//  Single-table iterative build (symmetric cases)
// ============================================================

std::vector<uint8_t> build_table_iterative_symmetric_openmp(
	const Count4& c,
	const LowerDepsView deps,
	const int max_depth)
{
	const uint64_t N = states_of(c);
	std::vector<uint8_t> cur((size_t)N, 0);
	std::vector<uint8_t> next((size_t)N, 0);

	ParallelFailFlag fail;

	for (int depth = 1; depth <= max_depth; ++depth) {
		std::fill(next.begin(), next.end(), 0);
		uint64_t newly_solved = 0;

#pragma omp parallel
		{
			uint64_t local_new = 0;

#pragma omp for schedule(dynamic, kOmpChunk)
			for (int64_t i = 0; i < (int64_t)N; ++i) {
				if (fail.has_failed()) continue;

				const uint64_t idx = (uint64_t)i;

				const uint8_t v = cur[(size_t)idx];
				if (v != 0) {
					next[(size_t)idx] = v;
					continue;
				}

				uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
				unrank_geister_perfect_information(
					idx, c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or,
					bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

				perfect_information_geister pos{
					player_board{bb_player_red, bb_player_blue},
					player_board{bb_opponent_red, bb_opponent_blue}
				};

				if (pos.is_immediate_win()) { next[(size_t)idx] = 1; ++local_new; continue; }
				if (pos.is_immediate_loss()) { next[(size_t)idx] = 2; ++local_new; continue; }

				std::array<move, 32> moves{};
				const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
				if (n <= 0) { fail.fail(idx, TbFailKind::NoLegalMoves); next[(size_t)idx] = 0; continue; }

				// Simple retrograde:
				//  odd depth: current is winning if exists a move to a losing state at depth-1
				//  even depth: current is losing if all moves go to winning states (odd) at depth-1
				uint8_t out = 0;

				if (depth == 1) {
					// immediate capture-blue to win if opponent has 1 blue
					if (c.pop_ob == 1) {
						for (int k = 0; k < n; ++k) {
							if (moves[k].if_capture_blue()) { out = 1; break; }
						}
					}
				}
				else if (depth & 1) {
					// winning ply
					for (int k = 0; k < n; ++k) {
						const move m = moves[k];

						// immediate terminal capture
						if (m.if_capture_blue() && c.pop_ob == 1) { out = 1; break; }

						uint8_t t = 0;

						// captures go to lower-material TB
						if (m.if_capture_blue()) {
							if (deps.cap_blue.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (c.pop_or == 1) continue; // illegal terminal (capture last red is bad for mover)
							if (deps.cap_red.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_red.get(child);
						}
						else {
							// non-capture swaps turn => same TB (symmetric), child idx is in same array
							const uint64_t child = child_index_after_move(pos, m);
							t = cur[(size_t)child];
						}

						// if child is losing in depth-1 => current is winning in depth
						if (t == (uint8_t)(depth - 1)) { out = (uint8_t)depth; break; }
					}
				}
				else {
					// losing ply: all moves must go to winning (odd depth-1) states
					bool forced = true;
					uint8_t max_odd = 0;

					for (int k = 0; k < n; ++k) {
						const move m = moves[k];

						if (m.if_capture_blue() && c.pop_ob == 1) { forced = false; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (deps.cap_blue.empty()) { forced = false; break; }
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (c.pop_or == 1) {
								// capturing last red => terminal loss for mover => not a winning child; treat as non-forcing
								t = 1;
							}
							else {
								if (deps.cap_red.empty()) { forced = false; break; }
								const uint64_t child = child_index_after_move(pos, m);
								t = deps.cap_red.get(child);
							}
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = cur[(size_t)child];
						}

						// any unknown or even (losing ply) child breaks forcing
						if (t == 0) { forced = false; break; }
						if ((t & 1U) == 0) { forced = false; break; }
						if (t > max_odd) max_odd = t;
					}

					if (forced && max_odd == (uint8_t)(depth - 1)) out = (uint8_t)depth;
				}

				next[(size_t)idx] = out;
				if (out != 0) ++local_new;
			}

#pragma omp atomic
			newly_solved += local_new;
		} // omp parallel

		if (fail.has_failed()) {
			std::ostringstream oss;
			oss << "iterative build failed at idx=" << fail.bad_idx()
				<< " kind=" << tb_fail_kind_to_cstr(fail.bad_kind());
			throw std::runtime_error(oss.str());
		}

		cur.swap(next);

		// early-out if solved all
		// (optional: could check if newly_solved==0)
		(void)newly_solved;
	}

	return cur;
}

// ============================================================
//  One-pass build by swapping finished table (A from B, or B from A)
// ============================================================

std::vector<uint8_t> build_table_onepass_from_swap_final_openmp(
	const Count4& target,
	const TableView swapped_final,
	const LowerDepsView deps,
	const int max_depth)
{
	// target is the material of the table we want to build.
	// swapped_final is the already-built table for swap_material(target).
	const uint64_t N = states_of(target);
	tb_ensure(swapped_final.n == N, "swapped table size mismatch");

	std::vector<uint8_t> out((size_t)N, 0);

	ParallelFailFlag fail;

#pragma omp parallel
	{
#pragma omp for schedule(dynamic, kOmpChunk)
		for (int64_t i = 0; i < (int64_t)N; ++i) {
			if (fail.has_failed()) continue;

			const uint64_t idx = (uint64_t)i;

			uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
			unrank_geister_perfect_information(
				idx, target.pop_pb, target.pop_pr, target.pop_ob, target.pop_or,
				bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

			perfect_information_geister pos{
				player_board{bb_player_red, bb_player_blue},
				player_board{bb_opponent_red, bb_opponent_blue}
			};

			if (pos.is_immediate_win()) { out[(size_t)idx] = 1; continue; }
			if (pos.is_immediate_loss()) { out[(size_t)idx] = 2; continue; }

			std::array<move, 32> moves{};
			const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
			if (n <= 0) { fail.fail(idx, TbFailKind::NoLegalMoves); out[(size_t)idx] = 0; continue; }

			uint8_t solved = 0;

			// Evaluate with already-solved swapped table:
			// Non-capture move leads to swap of turn, so child lies in swapped table.
			// Capture moves lead to lower-material deps.
			// We must compute exact DTW using same iterative logic, but since swapped table already final,
			// we can derive exact value by brute forcing depth from 1..max_depth.
			// (This is intentionally identical to the original one-pass code.)
			for (int depth = 1; depth <= max_depth; ++depth) {
				uint8_t outd = 0;

				if (depth == 1) {
					if (target.pop_ob == 1) {
						for (int k = 0; k < n; ++k) {
							if (moves[k].if_capture_blue()) { outd = 1; break; }
						}
					}
				}
				else if (depth & 1) {
					for (int k = 0; k < n; ++k) {
						const move m = moves[k];

						if (m.if_capture_blue() && target.pop_ob == 1) { outd = 1; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (deps.cap_blue.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (target.pop_or == 1) continue;
							if (deps.cap_red.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_red.get(child);
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = swapped_final.get(child);
						}
						if (t == (uint8_t)(depth - 1)) { outd = (uint8_t)depth; break; }
					}
				}
				else {
					bool forced = true;
					uint8_t max_odd = 0;

					for (int k = 0; k < n; ++k) {
						const move m = moves[k];

						if (m.if_capture_blue() && target.pop_ob == 1) { forced = false; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (deps.cap_blue.empty()) { forced = false; break; }
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (target.pop_or == 1) t = 1;
							else {
								if (deps.cap_red.empty()) { forced = false; break; }
								const uint64_t child = child_index_after_move(pos, m);
								t = deps.cap_red.get(child);
							}
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = swapped_final.get(child);
						}

						if (t == 0) { forced = false; break; }
						if ((t & 1U) == 0) { forced = false; break; }
						if (t > max_odd) max_odd = t;
					}

					if (forced && max_odd == (uint8_t)(depth - 1)) outd = (uint8_t)depth;
				}

				if (outd != 0) { solved = outd; break; }
			}

			out[(size_t)idx] = solved;
		}
	} // omp parallel

	if (fail.has_failed()) {
		std::ostringstream oss;
		oss << "one-pass build failed at idx=" << fail.bad_idx()
			<< " kind=" << tb_fail_kind_to_cstr(fail.bad_kind());
		throw std::runtime_error(oss.str());
	}

	return out;
}

// ============================================================
//  Consistency check
// ============================================================

void check_table_consistency_openmp(
	const Count4& c,
	const TableView cur,
	const TableView swapped,
	const LowerDepsView deps,
	const int max_depth)
{
	const uint64_t N = states_of(c);
	tb_ensure(cur.n == N, "table size mismatch");
	tb_ensure(swapped.n == N, "swapped table size mismatch");

	const uint16_t id = material_id_of(c);
	const std::string label = tbutil::material_label(id, c);

	ParallelFailFlag fail;

	std::atomic<uint64_t> next_idx{ 0 };
	std::atomic<uint64_t> processed{ 0 };
	std::atomic<int64_t> last_print_ms{ 0 };

	const tbutil::Clock::time_point t0 = tbutil::Clock::now();
	const int64_t interval_ms = (int64_t)std::max(0, g_cfg.progress_interval_sec) * 1000;

#pragma omp parallel
	{
		while (!fail.has_failed()) {
			const uint64_t start = next_idx.fetch_add(g_cfg.progress_chunk, std::memory_order_relaxed);
			if (start >= N) break;

			const uint64_t end = std::min<uint64_t>(N, start + g_cfg.progress_chunk);

			uint64_t local_done = 0;

			for (uint64_t idx = start; idx < end; ++idx) {
				if (fail.has_failed()) break;
				++local_done;

				const uint8_t v = cur.get(idx);

				uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
				unrank_geister_perfect_information(
					idx, c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or,
					bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

				perfect_information_geister pos{
					player_board{bb_player_red, bb_player_blue},
					player_board{bb_opponent_red, bb_opponent_blue}
				};

				uint8_t ref = 0;

				if (pos.is_immediate_win()) ref = 1;
				else if (pos.is_immediate_loss()) ref = 2;
				else {
					std::array<move, 32> moves{};
					const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
					if (n <= 0) { fail.fail(idx, TbFailKind::NoLegalMoves); break; }

					// ------------------------------------------------------------
					// One-pass expected-value check (Improvement E)
					//   - If any child is losing for the side-to-move (even t), current is winning with min(t+1).
					//   - Else if all children are winning (odd t), current is losing with max(t+1).
					//   - Otherwise (any unknown / beyond-horizon), current is 0 (unresolved/draw under this max_depth).
					// ------------------------------------------------------------
					uint8_t best_win = 0; // smallest odd DTW for current player
					uint8_t max_odd = 0;  // largest odd child value
					bool has_unknown = false;

					for (int k = 0; k < n; ++k) {
						const move m = moves[k];

						// Capturing the opponent's last blue ends the game immediately (win in 1 ply).
						if (m.if_capture_blue() && c.pop_ob == 1) {
							best_win = 1;
							break;
						}

						uint8_t t = 0;

						if (m.if_capture_blue()) {
							if (deps.cap_blue.empty()) { has_unknown = true; continue; }
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							// Capturing the opponent's last red is an immediate loss for the mover:
							// in the child position, the side-to-move is immediately winning (DTW=1).
							if (c.pop_or == 1) {
								t = 1;
							}
							else {
								if (deps.cap_red.empty()) { has_unknown = true; continue; }
								const uint64_t child = child_index_after_move(pos, m);
								t = deps.cap_red.get(child);
							}
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = swapped.get(child);
						}

						if (t == 0) { has_unknown = true; continue; }

						if ((t & 1U) == 0) {
							const uint16_t cand = (uint16_t)t + 1U;
							if (cand <= (uint16_t)max_depth) {
								const uint8_t cand8 = (uint8_t)cand;
								if (best_win == 0 || cand8 < best_win) best_win = cand8;
							}
							else {
								// Beyond the depth horizon => behaves like "unknown" w.r.t. this max_depth.
								has_unknown = true;
							}
						}
						else {
							if (t > max_odd) max_odd = t;
						}
					}

					if (best_win != 0) {
						ref = best_win;
					}
					else if (!has_unknown) {
						const uint16_t loss = (uint16_t)max_odd + 1U;
						ref = (loss <= (uint16_t)max_depth) ? (uint8_t)loss : 0;
					}
					else {
						ref = 0;
					}
				}

				if (ref != v) {
					fail.fail(idx, TbFailKind::ValueMismatch);
					break;
				}
			}

			const uint64_t done = processed.fetch_add(local_done, std::memory_order_relaxed) + local_done;

			// ------------------------------------------------------------
			// Progress print (Improvement F)
			//   - One log line per interval across all threads.
			// ------------------------------------------------------------
			if (interval_ms > 0 && !fail.has_failed()) {
				const tbutil::Clock::time_point now = tbutil::Clock::now();
				const int64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - t0).count();

				int64_t last = last_print_ms.load(std::memory_order_relaxed);
				if (ms - last >= interval_ms) {
					if (last_print_ms.compare_exchange_strong(last, ms, std::memory_order_relaxed)) {
						const double pct = (double)done * 100.0 / (double)N;
						const double sec = (double)ms / 1000.0;
						const double rate = (sec > 0.0) ? (double)done / sec : 0.0;

						std::ostringstream oss;
						oss << "[CHK] " << label << " "
							<< std::fixed << std::setprecision(2) << pct << "% "
							<< "(" << done << "/" << N << ") "
							<< std::setprecision(2) << (rate / 1e6) << " Mpos/s";
						tbutil::log_line(oss.str());
					}
				}
			}
		} // while
	} // omp parallel

	if (fail.has_failed()) {
		const uint64_t bad = fail.bad_idx();
		const TbFailKind kind = fail.bad_kind();

		uint8_t v = 0, ref = 0;
		int n_moves = -1;

		// Recompute details serially for a clearer error message.
		{
			v = cur.get(bad);

			uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
			unrank_geister_perfect_information(
				bad, c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or,
				bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

			perfect_information_geister pos{
				player_board{bb_player_red, bb_player_blue},
				player_board{bb_opponent_red, bb_opponent_blue}
			};

			if (pos.is_immediate_win()) ref = 1;
			else if (pos.is_immediate_loss()) ref = 2;
			else {
				std::array<move, 32> moves{};
				const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
				n_moves = n;
				if (n <= 0) {
					ref = 0;
				}
				else {
					uint8_t best_win = 0;
					uint8_t max_odd = 0;
					bool has_unknown = false;

					for (int k = 0; k < n; ++k) {
						const move m = moves[k];

						if (m.if_capture_blue() && c.pop_ob == 1) { best_win = 1; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (deps.cap_blue.empty()) { has_unknown = true; continue; }
							const uint64_t child = child_index_after_move(pos, m);
							t = deps.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (c.pop_or == 1) t = 1;
							else {
								if (deps.cap_red.empty()) { has_unknown = true; continue; }
								const uint64_t child = child_index_after_move(pos, m);
								t = deps.cap_red.get(child);
							}
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = swapped.get(child);
						}

						if (t == 0) { has_unknown = true; continue; }

						if ((t & 1U) == 0) {
							const uint16_t cand = (uint16_t)t + 1U;
							if (cand <= (uint16_t)max_depth) {
								const uint8_t cand8 = (uint8_t)cand;
								if (best_win == 0 || cand8 < best_win) best_win = cand8;
							}
							else {
								has_unknown = true;
							}
						}
						else {
							if (t > max_odd) max_odd = t;
						}
					}

					if (best_win != 0) ref = best_win;
					else if (!has_unknown) {
						const uint16_t loss = (uint16_t)max_odd + 1U;
						ref = (loss <= (uint16_t)max_depth) ? (uint8_t)loss : 0;
					}
					else ref = 0;
				}
			}
		}

		std::ostringstream oss;
		oss << "consistency check failed for " << label
			<< " at idx=" << bad
			<< " kind=" << tb_fail_kind_to_cstr(kind);
		if (kind == TbFailKind::ValueMismatch) {
			oss << " value=" << (int)v << " expected=" << (int)ref;
		}
		if (kind == TbFailKind::NoLegalMoves) {
			oss << " n_moves=" << n_moves;
		}
		throw std::runtime_error(oss.str());
	}
}


// ============================================================
//  Pair iterative build (A and B swapped domain, non-symmetric)
// ============================================================

struct BuiltPair {
	std::vector<uint8_t> A;
	std::vector<uint8_t> B;
};

BuiltPair build_table_iterative_pair_openmp(
	const Count4& a,
	const Count4& b,
	const LowerDepsView depsA,
	const LowerDepsView depsB,
	const int max_depth)
{
	const uint64_t N = states_of(a);
	tb_ensure(states_of(b) == N, "A/B domain sizes differ (bug)");

	std::vector<uint8_t> curA((size_t)N, 0), curB((size_t)N, 0);
	std::vector<uint8_t> nextA((size_t)N, 0), nextB((size_t)N, 0);

	ParallelFailFlag fail;

	for (int depth = 1; depth <= max_depth; ++depth) {
		std::fill(nextA.begin(), nextA.end(), 0);
		std::fill(nextB.begin(), nextB.end(), 0);

#pragma omp parallel
		{
			uint64_t newly_solved = 0;

#pragma omp for schedule(dynamic, kOmpChunk)
			for (int64_t i = 0; i < (int64_t)N; ++i) {
				if (fail.has_failed()) continue;

				const uint64_t idx = (uint64_t)i;
				const uint8_t v = curA[(size_t)idx];
				if (v != 0) { nextA[(size_t)idx] = v; continue; }

				uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
				unrank_geister_perfect_information(
					idx, a.pop_pb, a.pop_pr, a.pop_ob, a.pop_or,
					bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

				perfect_information_geister pos{
					player_board{bb_player_red, bb_player_blue},
					player_board{bb_opponent_red, bb_opponent_blue}
				};

				if (pos.is_immediate_win()) { nextA[(size_t)idx] = 1; ++newly_solved; continue; }
				if (pos.is_immediate_loss()) { nextA[(size_t)idx] = 2; ++newly_solved; continue; }

				std::array<move, 32> moves{};
				const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
				if (n <= 0) { fail.fail(idx, TbFailKind::NoLegalMoves); nextA[(size_t)idx] = 0; continue; }

				uint8_t out = 0;

				if (depth == 1) {
					if (a.pop_ob == 1) {
						for (int k = 0; k < n; ++k) if (moves[k].if_capture_blue()) { out = 1; break; }
					}
				}
				else if (depth & 1) {
					for (int k = 0; k < n; ++k) {
						const move m = moves[k];
						if (m.if_capture_blue() && a.pop_ob == 1) { out = 1; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (depsA.cap_blue.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = depsA.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (a.pop_or == 1) continue;
							if (depsA.cap_red.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = depsA.cap_red.get(child);
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = curB[(size_t)child]; // A -> B
						}

						if (t == (uint8_t)(depth - 1)) { out = (uint8_t)depth; break; }
					}
				}
				else {
					bool forced = true;
					uint8_t max_odd = 0;

					for (int k = 0; k < n; ++k) {
						const move m = moves[k];
						if (m.if_capture_blue() && a.pop_ob == 1) { forced = false; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (depsA.cap_blue.empty()) { forced = false; break; }
							const uint64_t child = child_index_after_move(pos, m);
							t = depsA.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (a.pop_or == 1) t = 1;
							else {
								if (depsA.cap_red.empty()) { forced = false; break; }
								const uint64_t child = child_index_after_move(pos, m);
								t = depsA.cap_red.get(child);
							}
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = curB[(size_t)child]; // A -> B
						}

						if (t == 0) { forced = false; break; }
						if ((t & 1U) == 0) { forced = false; break; }
						if (t > max_odd) max_odd = t;
					}

					if (forced && max_odd == (uint8_t)(depth - 1)) out = (uint8_t)depth;
				}

				nextA[(size_t)idx] = out;
				if (out != 0) ++newly_solved;
			}

#pragma omp for schedule(dynamic, kOmpChunk)
			for (int64_t i = 0; i < (int64_t)N; ++i) {
				if (fail.has_failed()) continue;

				const uint64_t idx = (uint64_t)i;
				const uint8_t v = curB[(size_t)idx];
				if (v != 0) { nextB[(size_t)idx] = v; continue; }

				uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
				unrank_geister_perfect_information(
					idx, b.pop_pb, b.pop_pr, b.pop_ob, b.pop_or,
					bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

				perfect_information_geister pos{
					player_board{bb_player_red, bb_player_blue},
					player_board{bb_opponent_red, bb_opponent_blue}
				};

				if (pos.is_immediate_win()) { nextB[(size_t)idx] = 1; ++newly_solved; continue; }
				if (pos.is_immediate_loss()) { nextB[(size_t)idx] = 2; ++newly_solved; continue; }

				std::array<move, 32> moves{};
				const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
				if (n <= 0) { fail.fail(idx, TbFailKind::NoLegalMoves); nextB[(size_t)idx] = 0; continue; }

				uint8_t out = 0;

				if (depth == 1) {
					if (b.pop_ob == 1) {
						for (int k = 0; k < n; ++k) if (moves[k].if_capture_blue()) { out = 1; break; }
					}
				}
				else if (depth & 1) {
					for (int k = 0; k < n; ++k) {
						const move m = moves[k];
						if (m.if_capture_blue() && b.pop_ob == 1) { out = 1; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (depsB.cap_blue.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = depsB.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (b.pop_or == 1) continue;
							if (depsB.cap_red.empty()) continue;
							const uint64_t child = child_index_after_move(pos, m);
							t = depsB.cap_red.get(child);
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = curA[(size_t)child]; // B -> A
						}

						if (t == (uint8_t)(depth - 1)) { out = (uint8_t)depth; break; }
					}
				}
				else {
					bool forced = true;
					uint8_t max_odd = 0;

					for (int k = 0; k < n; ++k) {
						const move m = moves[k];
						if (m.if_capture_blue() && b.pop_ob == 1) { forced = false; break; }

						uint8_t t = 0;
						if (m.if_capture_blue()) {
							if (depsB.cap_blue.empty()) { forced = false; break; }
							const uint64_t child = child_index_after_move(pos, m);
							t = depsB.cap_blue.get(child);
						}
						else if (m.if_capture_red()) {
							if (b.pop_or == 1) t = 1;
							else {
								if (depsB.cap_red.empty()) { forced = false; break; }
								const uint64_t child = child_index_after_move(pos, m);
								t = depsB.cap_red.get(child);
							}
						}
						else {
							const uint64_t child = child_index_after_move(pos, m);
							t = curA[(size_t)child];
						}

						if (t == 0) { forced = false; break; }
						if ((t & 1U) == 0) { forced = false; break; }
						if (t > max_odd) max_odd = t;
					}

					if (forced && max_odd == (uint8_t)(depth - 1)) out = (uint8_t)depth;
				}

				nextB[(size_t)idx] = out;
				if (out != 0) ++newly_solved;
			}
		} // omp parallel

		if (fail.has_failed()) {
			std::ostringstream oss;
			oss << "iterative pair build failed at idx=" << fail.bad_idx()
				<< " kind=" << tb_fail_kind_to_cstr(fail.bad_kind());
			throw std::runtime_error(oss.str());
		}

		curA.swap(nextA);
		curB.swap(nextB);
	}

	return BuiltPair{ std::move(curA), std::move(curB) };
}

// ============================================================
//  Top-level: ensure/build/check for a material id (and its swap)
// ============================================================

void ensure_tablebase_for_material_id(uint16_t id) {
	const auto t_func0 = tbutil::Clock::now();
	auto sec_since = [&](const tbutil::Clock::time_point& t0) -> double {
		return std::chrono::duration<double>(tbutil::Clock::now() - t0).count();
	};

	const Count4 A = unrank_material_configuration(id);
	const Count4 B = swap_material(A);

	const uint16_t idA = id;
	const uint16_t idB = material_id_of(B);

	const bool symmetric = (idA == idB);

	const uint64_t NA = states_of(A);
	const uint64_t NB = states_of(B);
	tb_ensure(NB == NA, "swap domain size mismatch (bug)");

	const std::string labelA = tbutil::material_label(idA, A);
	const std::string labelB = tbutil::material_label(idB, B);

	{
		// .txt is either 3 or 4 bytes/entry; we print LF-size as a baseline.
		const uint64_t txt_bytes = NA * 3ULL;
		const uint64_t bin_bytes = NA;

		std::ostringstream oss;
		oss << "[ID] begin " << labelA
			<< (symmetric ? " (symmetric)" : "")
			<< " swap=" << labelB
			<< " N=" << NA
			<< " txt≈" << tbutil::format_gib(txt_bytes)
			<< " bin≈" << tbutil::format_gib(bin_bytes);
		tbutil::log_line(oss.str());
	}

	const std::string fnA = make_tablebase_filename(idA, A);
	const std::string fnB = make_tablebase_filename(idB, B);

	// ------------------------------------------------------------
	// Existence check
	// ------------------------------------------------------------
	const std::string fnA_bin = make_tablebase_filename_bin(idA, A);
	const std::string fnB_bin = make_tablebase_filename_bin(idB, B);
	const auto t0_exist = tbutil::Clock::now();
	const bool haveA_bin = tablebase_bin_looks_valid(fnA_bin, NA);
	const bool haveB_bin = symmetric ? haveA_bin : tablebase_bin_looks_valid(fnB_bin, NB);
	const bool haveA_txt = tablebase_file_looks_valid(fnA, NA);
	const bool haveB_txt = symmetric ? haveA_txt : tablebase_file_looks_valid(fnB, NB);
	const bool haveA = haveA_bin || haveA_txt;
	const bool haveB = haveB_bin || haveB_txt;
	{
		std::ostringstream oss;
		oss << "[ID] exists? A[bin=" << (haveA_bin ? "yes" : "no") << ",txt=" << (haveA_txt ? "yes" : "no") << "]";
		if (!symmetric) oss << " B[bin=" << (haveB_bin ? "yes" : "no") << ",txt=" << (haveB_txt ? "yes" : "no") << "]";
		oss << " (" << std::fixed << std::setprecision(3) << sec_since(t0_exist) << "s)";
		tbutil::log_line(oss.str());
	}

	// ------------------------------------------------------------
	// Load lower-material dependencies (capture tables)
	// ------------------------------------------------------------
	const auto t0_deps = tbutil::Clock::now();

	DepCache dep_cache;
	const LowerDeps depsA_raw = load_lower_deps_for(A, dep_cache);
	const LowerDeps depsB_raw = symmetric ? LowerDeps{} : load_lower_deps_for(B, dep_cache);

	const LowerDepsView depsA = make_view(depsA_raw);
	const LowerDepsView depsB = symmetric ? LowerDepsView{} : make_view(depsB_raw);

	{
		std::ostringstream oss;
		oss << "[ID] deps loaded (" << std::fixed << std::setprecision(3) << sec_since(t0_deps) << "s)"
			<< " A[capB=" << depsA_raw.cap_blue.size() << ",capR=" << depsA_raw.cap_red.size() << "]";
		if (!symmetric) oss << " B[capB=" << depsB_raw.cap_blue.size() << ",capR=" << depsB_raw.cap_red.size() << "]";
		tbutil::log_line(oss.str());
	}

	std::vector<uint8_t> tbA;
	std::vector<uint8_t> tbB;

	// ------------------------------------------------------------
	// Load existing table if present (prefer _obsblk.bin; fall back to .txt)
	// ------------------------------------------------------------
	auto load_existing_table = [&](std::vector<uint8_t>& dst, const Count4& c, const std::string& fn_txt_local, const std::string& fn_bin_local, uint64_t entries, bool have_bin, bool have_txt) {
		if (!(have_bin || have_txt)) return;
		const auto t0 = tbutil::Clock::now();
		if (have_bin) {
			tbutil::log_line("[BIN] load ", fn_bin_local);
			dst = load_tablebase_bin_obsblk_as_legacy(fn_bin_local, c, entries);
			std::ostringstream oss;
			oss << "[BIN] loaded " << fn_bin_local << " (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
			tbutil::log_line(oss.str());
		}
		else {
			tbutil::log_line("[TXT] load ", fn_txt_local);
			dst = load_tablebase_hex_lines(fn_txt_local, entries);
			std::ostringstream oss;
			oss << "[TXT] loaded " << fn_txt_local << " (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
			tbutil::log_line(oss.str());
		}
	};

	load_existing_table(tbA, A, fnA, fnA_bin, NA, haveA_bin, haveA_txt);
	if (!symmetric) {
		load_existing_table(tbB, B, fnB, fnB_bin, NB, haveB_bin, haveB_txt);
	}

	// ------------------------------------------------------------
	// Build missing tables (.txt)
	// ------------------------------------------------------------
	if (!haveA && !haveB) {
		if (symmetric) {
			const auto t0 = tbutil::Clock::now();
			tbutil::log_line("[BUILD] iterative symmetric ", labelA);
			tbA = build_table_iterative_symmetric_openmp(A, depsA, g_cfg.max_depth);
			{
				std::ostringstream oss;
				oss << "[BUILD] done (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
				tbutil::log_line(oss.str());
			}

			if (g_cfg.write_txt) {
				const auto t1 = tbutil::Clock::now();
				tbutil::log_line("[TXT] write ", fnA);
				write_tablebase_hex_lines(tbA, fnA);
				{
					std::ostringstream oss;
					oss << "[TXT] wrote " << fnA << " (" << std::fixed << std::setprecision(3) << sec_since(t1) << "s)";
					tbutil::log_line(oss.str());
				}
			}
		}
		else {
			const auto t0 = tbutil::Clock::now();
			tbutil::log_line("[BUILD] iterative pair ", labelA, " <-> ", labelB);
			BuiltPair pair = build_table_iterative_pair_openmp(A, B, depsA, depsB, g_cfg.max_depth);
			tbA = std::move(pair.A);
			tbB = std::move(pair.B);
			{
				std::ostringstream oss;
				oss << "[BUILD] done (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
				tbutil::log_line(oss.str());
			}

			if (g_cfg.write_txt) {
				{
					const auto t1 = tbutil::Clock::now();
					tbutil::log_line("[TXT] write ", fnA);
					write_tablebase_hex_lines(tbA, fnA);
					std::ostringstream oss;
					oss << "[TXT] wrote " << fnA << " (" << std::fixed << std::setprecision(3) << sec_since(t1) << "s)";
					tbutil::log_line(oss.str());
				}
				{
					const auto t1 = tbutil::Clock::now();
					tbutil::log_line("[TXT] write ", fnB);
					write_tablebase_hex_lines(tbB, fnB);
					std::ostringstream oss;
					oss << "[TXT] wrote " << fnB << " (" << std::fixed << std::setprecision(3) << sec_since(t1) << "s)";
					tbutil::log_line(oss.str());
				}
			}
		}
	}
	else if (!symmetric && !haveA && haveB) {
		tb_ensure(!tbB.empty(), "tbB should be loaded here");

		const auto t0 = tbutil::Clock::now();
		tbutil::log_line("[BUILD] one-pass from swap ", labelA, " <- ", labelB);
		tbA = build_table_onepass_from_swap_final_openmp(A, make_view(tbB), depsA, g_cfg.max_depth);
		{
			std::ostringstream oss;
			oss << "[BUILD] done (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
			tbutil::log_line(oss.str());
		}

		if (g_cfg.write_txt) {
			const auto t1 = tbutil::Clock::now();
			tbutil::log_line("[TXT] write ", fnA);
			write_tablebase_hex_lines(tbA, fnA);
			{
				std::ostringstream oss;
				oss << "[TXT] wrote " << fnA << " (" << std::fixed << std::setprecision(3) << sec_since(t1) << "s)";
				tbutil::log_line(oss.str());
			}
		}
	}
	else if (!symmetric && haveA && !haveB) {
		tb_ensure(!tbA.empty(), "tbA should be loaded here");

		const auto t0 = tbutil::Clock::now();
		tbutil::log_line("[BUILD] one-pass from swap ", labelB, " <- ", labelA);
		tbB = build_table_onepass_from_swap_final_openmp(B, make_view(tbA), depsB, g_cfg.max_depth);
		{
			std::ostringstream oss;
			oss << "[BUILD] done (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
			tbutil::log_line(oss.str());
		}

		if (g_cfg.write_txt) {
			const auto t1 = tbutil::Clock::now();
			tbutil::log_line("[TXT] write ", fnB);
			write_tablebase_hex_lines(tbB, fnB);
			{
				std::ostringstream oss;
				oss << "[TXT] wrote " << fnB << " (" << std::fixed << std::setprecision(3) << sec_since(t1) << "s)";
				tbutil::log_line(oss.str());
			}
		}
	}

	// ------------------------------------------------------------
	// Ensure both loaded for check
	// ------------------------------------------------------------
	if (tbA.empty()) {
		load_existing_table(tbA, A, fnA, fnA_bin, NA, haveA_bin, haveA_txt);
	}
	if (!symmetric && tbB.empty()) {
		load_existing_table(tbB, B, fnB, fnB_bin, NB, haveB_bin, haveB_txt);
	}

	// ------------------------------------------------------------
	// Full consistency check (intentionally strict)
	//   - Uses one-pass expected-value evaluation (Improvement E)
	//   - Prints progress periodically (Improvement F)
	// ------------------------------------------------------------
	if (symmetric) {
		const auto t0 = tbutil::Clock::now();
		tbutil::log_line("[CHK] start ", labelA);
		const TableView vA = make_view(tbA);
		check_table_consistency_openmp(A, vA, vA, depsA, g_cfg.max_depth);
		{
			std::ostringstream oss;
			oss << "[CHK] done  " << labelA << " (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
			tbutil::log_line(oss.str());
		}
	}
	else {
		{
			const auto t0 = tbutil::Clock::now();
			tbutil::log_line("[CHK] start ", labelA, " (swap=", labelB, ")");
			const TableView vA = make_view(tbA);
			const TableView vB = make_view(tbB);
			check_table_consistency_openmp(A, vA, vB, depsA, g_cfg.max_depth);
			std::ostringstream oss;
			oss << "[CHK] done  " << labelA << " (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
			tbutil::log_line(oss.str());
		}
		{
			const auto t0 = tbutil::Clock::now();
			tbutil::log_line("[CHK] start ", labelB, " (swap=", labelA, ")");
			const TableView vA = make_view(tbA);
			const TableView vB = make_view(tbB);
			check_table_consistency_openmp(B, vB, vA, depsB, g_cfg.max_depth);
			std::ostringstream oss;
			oss << "[CHK] done  " << labelB << " (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
			tbutil::log_line(oss.str());
		}
	}

	// ------------------------------------------------------------
	// Ensure corresponding .bin exists (1 byte/entry) after the .txt passes checks.
	// ------------------------------------------------------------
	if (!tablebase_bin_looks_valid(fnA_bin, NA)) {
		const auto t0 = tbutil::Clock::now();
		tbutil::log_line("[BIN] write ", fnA_bin);
		write_tablebase_bin_obsblk(tbA, A, fnA_bin);
		std::ostringstream oss;
		oss << "[BIN] wrote " << fnA_bin << " (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
		tbutil::log_line(oss.str());
	}
	if (!symmetric && !tablebase_bin_looks_valid(fnB_bin, NB)) {
		const auto t0 = tbutil::Clock::now();
		tbutil::log_line("[BIN] write ", fnB_bin);
		write_tablebase_bin_obsblk(tbB, B, fnB_bin);
		std::ostringstream oss;
		oss << "[BIN] wrote " << fnB_bin << " (" << std::fixed << std::setprecision(3) << sec_since(t0) << "s)";
		tbutil::log_line(oss.str());
	}

	{
		std::ostringstream oss;
		oss << "[ID] done  " << labelA
			<< " (" << std::fixed << std::setprecision(3) << sec_since(t_func0) << "s)";
		tbutil::log_line(oss.str());
	}
}


// ============================================================
//  main
// ============================================================

int main(int argc, char** argv) {
	try {
		// ------------------------------------------------------------
		// Parse CLI options (Improvement B)
		// ------------------------------------------------------------
		for (int i = 1; i < argc; ++i) {
			const std::string_view arg = argv[i];

			if (arg == "-h" || arg == "--help") {
				tbutil::print_usage(argv[0]);
				return 0;
			}
			else if (arg == "--upto-total") {
				if (i + 1 >= argc) tb_fail("missing value for --upto-total");
				const uint64_t v = tbutil::must_parse_u64(argv[++i], "--upto-total");
				if (v > (uint64_t)MAX_TOTAL) tb_fail("--upto-total out of range");
				g_cfg.upto_total = (int)v;
			}
			else if (arg == "--id") {
				if (i + 1 >= argc) tb_fail("missing value for --id");
				const uint64_t v = tbutil::must_parse_u64(argv[++i], "--id");
				if (v > UINT16_MAX) tb_fail("--id out of range");
				g_cfg.run_single_id = true;
				g_cfg.single_id = (uint16_t)v;
			}
			else if (arg == "--range") {
				if (i + 2 >= argc) tb_fail("missing values for --range");
				const uint64_t a = tbutil::must_parse_u64(argv[++i], "--range A");
				const uint64_t b = tbutil::must_parse_u64(argv[++i], "--range B");
				if (a > UINT16_MAX || b > UINT16_MAX) tb_fail("--range out of range");
				if (a > b) tb_fail("--range requires A <= B");
				g_cfg.start_id = (uint16_t)a;
				g_cfg.end_id = (b == UINT16_MAX) ? UINT16_MAX : (uint16_t)(b + 1);
			}
			else if (arg == "--start-id") {
				if (i + 1 >= argc) tb_fail("missing value for --start-id");
				const uint64_t a = tbutil::must_parse_u64(argv[++i], "--start-id");
				if (a > UINT16_MAX) tb_fail("--start-id out of range");
				g_cfg.start_id = (uint16_t)a;
			}
			else if (arg == "--end-id") {
				if (i + 1 >= argc) tb_fail("missing value for --end-id");
				const uint64_t b = tbutil::must_parse_u64(argv[++i], "--end-id");
				if (b > UINT16_MAX) tb_fail("--end-id out of range");
				g_cfg.end_id = (uint16_t)b; // end is exclusive
			}
			else if (arg == "--no-skip-pairs") {
				g_cfg.skip_swap_duplicates = false;
			}
			else if (arg == "--progress-sec") {
				if (i + 1 >= argc) tb_fail("missing value for --progress-sec");
				const uint64_t v = tbutil::must_parse_u64(argv[++i], "--progress-sec");
				if (v > 3600ULL) tb_fail("--progress-sec too large");
				g_cfg.progress_interval_sec = (int)v;
			}
			else if (arg == "--write-txt") {
				g_cfg.write_txt = true;
			}
			else if (arg == "--no-write-txt") {
				g_cfg.write_txt = false;
			}
			else {
				std::ostringstream oss;
				oss << "unknown option: " << arg;
				throw std::runtime_error(oss.str());
			}
		}

		// Always clamp to the hard-coded table value range.
		g_cfg.max_depth = kMaxDepth;

		// Pre-warm geister.rank's internal canonical meta before any OpenMP region.
		{
			uint64_t pb = 0, pr = 0, ob = 0, oc = 0;
			unrank_geister_perfect_information(0, 1, 1, 1, 1, pb, pr, ob, oc);
		}

		tbutil::log_line("[INIT] omp_max_threads=", omp_get_max_threads(),
			" upto_total=", g_cfg.upto_total,
			" start_id=", g_cfg.start_id,
			" end_id=", g_cfg.end_id,
			" skip_pairs=", (g_cfg.skip_swap_duplicates ? "yes" : "no"),
			" progress_sec=", g_cfg.progress_interval_sec,
			" write_txt=", (g_cfg.write_txt ? "yes" : "no"));

		if (g_cfg.run_single_id) {
			tbutil::log_line("[RUN] single id=", (int)g_cfg.single_id);
			ensure_tablebase_for_material_id(g_cfg.single_id);
			return 0;
		}

		// Build all tablebases with total pieces <= upto_total.
		const uint16_t end_id_total = x_upto_total(g_cfg.upto_total);

		const uint16_t start = (g_cfg.start_id < end_id_total) ? g_cfg.start_id : end_id_total;
		const uint16_t end = (g_cfg.end_id < end_id_total) ? g_cfg.end_id : end_id_total;

		tbutil::log_line("[RUN] id range [", (int)start, ", ", (int)end, ") out of [0, ", (int)end_id_total, ")");

		for (uint16_t id = start; id < end; ++id) {
			// Improvement A: skip swap-duplicate pairs that would be processed earlier in THIS run.
			if (g_cfg.skip_swap_duplicates) {
				const Count4 A = unrank_material_configuration(id);
				const uint16_t id_swap = material_id_of(swap_material(A));
				if (id_swap < id && id_swap >= start && id_swap < end) {
					continue;
				}
			}
			ensure_tablebase_for_material_id(id);
		}

		return 0;
	}
	catch (const std::exception& e) {
		// Keep stderr simple (stdout may be redirected for logs).
		std::cerr << "Fatal: " << e.what() << std::endl;
		return 1;
	}
}

