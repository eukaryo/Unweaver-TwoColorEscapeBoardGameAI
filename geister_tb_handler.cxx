// geister_tb_handler.cxx
//
// Runtime probe facade for Geister endgame tablebases.
//
// This runtime intentionally supports only perfect-information DTW tablebases.
//
// Supported tablebase formats:
//   - Raw .bin: headerless 1 byte/entry (direct mmap).
//   - Seekable zstd: .bin.zst / .bin.zstd (mmap compressed + seekable decode).
//
// Perfect-information tablebase naming:
//   - legacy (LR-canonical): id%03d_pb{pb}pr{pr}ob{ob}or{or}.bin
//   - obsblk (observation-block): id%03d_pb{pb}pr{pr}ob{ob}or{or}_obsblk.bin
//
// Priority when multiple candidates exist for the same material id:
//   1) obsblk format over legacy format
//   2) seekable-zstd over raw .bin
//
// Value encoding (1 byte):
//   0 unknown/draw, odd: win in that many plies, even: loss.

module;

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

import tablebase_io;
import geister_core;
import geister_rank;
import geister_rank_obsblk;

export module geister_tb_handler;

export namespace geister_tb {

	// Load (mmap) all recognised perfect-information tablebase files from the current directory.
	//
	// Supported on-disk representations:
	//   - *.bin
	//   - *.bin.zst / *.bin.zstd   (seekable zstd format)
	//
	// Behaviour:
	//   - Warn if any <=8-piece baseline files are missing.
	//   - Silently load >=9-piece tables if present.
	//   - Prefer obsblk over legacy when both exist.
	//   - Prefer seekable-zstd over raw .bin when both exist.
	//   - This is the synchronous path and may block until the scan + mmap completes.
	void load_all_bins();

	// Start asynchronous preload in a detached background thread.
	//
	// While loading is in progress, probe_perfect_information() behaves as if no
	// table were loaded yet.
	void start_background_load();

	// Whether a background or synchronous load has completed successfully.
	[[nodiscard]] bool is_ready() noexcept;

	// Whether a background or synchronous load attempt has failed.
	[[nodiscard]] bool load_failed() noexcept;

	// Probe perfect-information DTW tablebase.
	//  - Returns std::nullopt if the corresponding table file is not loaded.
	[[nodiscard]] std::optional<std::uint8_t> probe_perfect_information(
		const perfect_information_geister& pos) noexcept;


} // namespace geister_tb

module :private;

namespace geister_tb::detail {

	// ---------------------------------------------------------------------
	// Constants
	// ---------------------------------------------------------------------

	// Baseline completeness target:
	//  - We warn if any tablebase for total pieces <= this value is missing.
	inline constexpr int kBaselineMaxTotalPieces = 8;

	// Perfect-information material domain size (4^4 = 256).
	inline constexpr std::size_t kPerfectDomainTotal = static_cast<std::size_t>(DOMAIN_TOTAL);

	// ---------------------------------------------------------------------
	// Helpers: filename parsing
	// ---------------------------------------------------------------------

	[[nodiscard]] inline bool is_digit(char c) noexcept { return ('0' <= c && c <= '9'); }

	[[nodiscard]] inline bool parse_uint_fixed(std::string_view s, std::size_t& i, int digits, int& out) noexcept {
		if (i + static_cast<std::size_t>(digits) > s.size()) return false;
		int v = 0;
		for (int k = 0; k < digits; ++k) {
			const char c = s[i + static_cast<std::size_t>(k)];
			if (!is_digit(c)) return false;
			v = v * 10 + (c - '0');
		}
		i += static_cast<std::size_t>(digits);
		out = v;
		return true;
	}

	[[nodiscard]] inline bool parse_uint(std::string_view s, std::size_t& i, int& out) noexcept {
		if (i >= s.size() || !is_digit(s[i])) return false;
		int v = 0;
		while (i < s.size() && is_digit(s[i])) {
			v = v * 10 + (s[i] - '0');
			++i;
		}
		out = v;
		return true;
	}

	[[nodiscard]] inline bool consume(std::string_view s, std::size_t& i, std::string_view tok) noexcept {
		if (s.substr(i, tok.size()) != tok) return false;
		i += tok.size();
		return true;
	}

	[[nodiscard]] inline std::string make_perfect_bin_filename_legacy(std::uint16_t id, const Count4& c) {
		// Matches legacy generator: id%03d_pb{pb}pr{pr}ob{ob}or{or}.bin
		std::ostringstream oss;
		oss << "id";
		oss << std::setw(3) << std::setfill('0') << static_cast<int>(id);
		oss << "_pb" << static_cast<int>(c.pop_pb)
			<< "pr" << static_cast<int>(c.pop_pr)
			<< "ob" << static_cast<int>(c.pop_ob)
			<< "or" << static_cast<int>(c.pop_or)
			<< ".bin";
		return oss.str();
	}

	[[nodiscard]] inline std::string make_perfect_bin_filename_obsblk(std::uint16_t id, const Count4& c) {
		// Matches obsblk repack: id%03d_pb{pb}pr{pr}ob{ob}or{or}_obsblk.bin
		std::ostringstream oss;
		oss << "id";
		oss << std::setw(3) << std::setfill('0') << static_cast<int>(id);
		oss << "_pb" << static_cast<int>(c.pop_pb)
			<< "pr" << static_cast<int>(c.pop_pr)
			<< "ob" << static_cast<int>(c.pop_ob)
			<< "or" << static_cast<int>(c.pop_or)
			<< "_obsblk.bin";
		return oss.str();
	}

	[[nodiscard]] inline bool parse_perfect_bin_filename(
		std::string_view name,
		std::uint16_t& out_id,
		Count4& out_counts,
		bool& out_obsblk) noexcept
	{
		// idXYZ_pb{pb}pr{pr}ob{ob}or{or}[_obsblk].bin
		std::size_t i = 0;
		if (!consume(name, i, "id")) return false;

		int id = 0;
		if (!parse_uint_fixed(name, i, 3, id)) return false;
		if (!consume(name, i, "_pb")) return false;

		int pb = 0, pr = 0, ob = 0, orc = 0;
		if (!parse_uint(name, i, pb)) return false;
		if (!consume(name, i, "pr")) return false;
		if (!parse_uint(name, i, pr)) return false;
		if (!consume(name, i, "ob")) return false;
		if (!parse_uint(name, i, ob)) return false;
		if (!consume(name, i, "or")) return false;
		if (!parse_uint(name, i, orc)) return false;

		bool obsblk = false;
		if (consume(name, i, "_obsblk")) obsblk = true;

		if (!consume(name, i, ".bin")) return false;
		if (i != name.size()) return false;

		if (id < 0 || id >= DOMAIN_TOTAL) return false;
		if (pb < 1 || pb > 4) return false;
		if (pr < 1 || pr > 4) return false;
		if (ob < 1 || ob > 4) return false;
		if (orc < 1 || orc > 4) return false;

		out_id = static_cast<std::uint16_t>(id);
		out_counts = Count4{
			static_cast<std::uint8_t>(pb),
			static_cast<std::uint8_t>(pr),
			static_cast<std::uint8_t>(ob),
			static_cast<std::uint8_t>(orc)
		};
		out_obsblk = obsblk;
		return true;
	}

	// ---------------------------------------------------------------------
	// Storage
	// ---------------------------------------------------------------------

	enum class storage_kind : std::uint8_t {
		none = 0,
		raw_bin = 1,
		seekable_zstd = 2,
	};

	struct perfect_entry {
		storage_kind kind = storage_kind::none;

		// raw .bin
		tbio::mmap::mapped_file mf;

		// seekable zstd (.bin.zst/.bin.zstd)
		tbio::seekable_zstd::mapped_seekable_file zsf;

		Count4 counts{ 0,0,0,0 };
		bool obsblk = false;
		bool present = false;
	};

	inline std::array<perfect_entry, kPerfectDomainTotal> g_perfect{};

	inline std::once_flag g_load_once{};
	inline std::atomic<bool> g_loaded{ false };
	inline std::atomic<bool> g_load_failed{ false };
	inline std::atomic<bool> g_background_started{ false };

	[[nodiscard]] inline int candidate_score(bool obsblk, bool seekable) noexcept {
		// obsblk has larger impact than compression preference.
		return (obsblk ? 2 : 0) + (seekable ? 1 : 0);
	}

	[[nodiscard]] inline int entry_score(const perfect_entry& e) noexcept {
		return candidate_score(e.obsblk, e.kind == storage_kind::seekable_zstd);
	}

	[[nodiscard]] inline std::uint64_t mapped_bytes(const perfect_entry& e) noexcept {
		if (!e.present) return 0;
		switch (e.kind) {
		case storage_kind::raw_bin:
			return static_cast<std::uint64_t>(e.mf.size());
		case storage_kind::seekable_zstd:
			return static_cast<std::uint64_t>(e.zsf.compressed_size());
		default:
			return 0;
		}
	}

	void do_load_all_bins();

	inline void do_load_all_bins_guarded() {
		try {
			do_load_all_bins();
			g_load_failed.store(false, std::memory_order_release);
		}
		catch (...) {
			g_loaded.store(false, std::memory_order_release);
			g_load_failed.store(true, std::memory_order_release);
			throw;
		}
	}

	// ---------------------------------------------------------------------
	// Internal loader
	// ---------------------------------------------------------------------

	void warn_incomplete_baseline();

	inline void do_load_all_bins() {
		std::uint64_t loaded_files = 0;
		std::uint64_t loaded_bytes = 0;

		const std::filesystem::path dir = std::filesystem::current_path();

		for (const auto& ent : std::filesystem::directory_iterator(dir)) {
			std::error_code ec;
			if (!ent.is_regular_file(ec)) continue;

			const std::filesystem::path p = ent.path();

			// Accept:
			//   - *.bin
			//   - *.bin.zst / *.bin.zstd
			bool is_seekable = false;
			std::string parse_name;

			if (p.extension() == ".bin") {
				parse_name = p.filename().string();
				is_seekable = false;
			}
			else if ((p.extension() == ".zst" || p.extension() == ".zstd") && p.stem().extension() == ".bin") {
				// example: id000_..._obsblk.bin.zst  -> parse "id000_..._obsblk.bin"
				parse_name = p.stem().filename().string();
				is_seekable = true;
			}
			else {
				continue;
			}

			// Perfect-information (legacy or obsblk)
			{
				std::uint16_t id = 0;
				Count4 c{};
				bool obsblk = false;
				if (parse_perfect_bin_filename(parse_name, id, c, obsblk)) {
					// Cross-check id vs tuple (defensive)
					const std::uint16_t expected_id = rank_material_configuration(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
					if (expected_id != id) {
						std::cerr << "[WARN] ignore perfect table (id mismatch): " << p.filename().string()
							<< " parsed_id=" << id << " expected_id=" << expected_id << std::endl;
						continue;
					}

					const std::uint64_t entries = obsblk
						? obsblk_states_for_counts(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or)
						: states_of(c);

					auto& slot = g_perfect[static_cast<std::size_t>(id)];
					const int new_score = candidate_score(obsblk, is_seekable);
					const int old_score = slot.present ? entry_score(slot) : -1;

					// If this is not strictly better than what's already loaded, ignore it.
					if (slot.present && new_score <= old_score) {
						// Keep noisy logs minimal: only warn on exact duplicates.
						if (new_score == old_score) {
							std::cerr << "[WARN] duplicate perfect table ignored: " << p.filename().string() << std::endl;
						}
						continue;
					}

					try {
						// Open the candidate.
						if (is_seekable) {
							auto zsf = tbio::seekable_zstd::open_tablebase_seekable_zstd_readonly(p, entries);
							zsf.advise(tbio::mmap::advice::random);

							// Replace / install.
							if (slot.present) {
								loaded_bytes -= mapped_bytes(slot);
							}
							else {
								++loaded_files;
							}

							slot.zsf = std::move(zsf);
							slot.mf = {}; // release raw mapping if any
							slot.kind = storage_kind::seekable_zstd;
							slot.counts = c;
							slot.obsblk = obsblk;
							slot.present = true;

							loaded_bytes += mapped_bytes(slot);
						}
						else {
							auto mf = tbio::mmap::open_tablebase_bin_readonly(p, entries);
							mf.advise(tbio::mmap::advice::random);

							// Replace / install.
							if (slot.present) {
								loaded_bytes -= mapped_bytes(slot);
							}
							else {
								++loaded_files;
							}

							slot.mf = std::move(mf);
							slot.zsf = {}; // release seekable mapping if any
							slot.kind = storage_kind::raw_bin;
							slot.counts = c;
							slot.obsblk = obsblk;
							slot.present = true;

							loaded_bytes += mapped_bytes(slot);
						}
					}
					catch (const std::exception& e) {
						std::cerr << "[WARN] failed to load perfect table: " << p.filename().string()
							<< " (" << e.what() << ")" << std::endl;
					}
					continue;
				}
			}

			// Other files are ignored here (belief TB etc.).
		}

		g_loaded.store(true, std::memory_order_release);

		std::cerr << "[TB] loaded perfect-information tables: " << loaded_files
			<< ", total mapped bytes: " << loaded_bytes
			<< " (cwd=" << dir.string() << ")" << std::endl;

		warn_incomplete_baseline();
	}

	void warn_incomplete_baseline() {
		std::uint32_t expected = 0;
		std::uint32_t missing = 0;
		std::uint32_t listed = 0;

		for (int pb = 1; pb <= 4; ++pb) {
			for (int pr = 1; pr <= 4; ++pr) {
				for (int ob = 1; ob <= 4; ++ob) {
					for (int orc = 1; orc <= 4; ++orc) {
						const int total = pb + pr + ob + orc;
						if (total > kBaselineMaxTotalPieces) continue;
						++expected;

						const Count4 c{ (std::uint8_t)pb, (std::uint8_t)pr, (std::uint8_t)ob, (std::uint8_t)orc };
						const std::uint16_t id = rank_material_configuration(pb, pr, ob, orc);
						if (!g_perfect[static_cast<std::size_t>(id)].present) {
							++missing;
							if (listed < 12) {
								std::cerr << "[WARN] missing perfect table (baseline<=8): "
									<< make_perfect_bin_filename_obsblk(id, c)
									<< " (or legacy " << make_perfect_bin_filename_legacy(id, c) << ")"
									<< "  [either raw .bin or .bin.{zst,zstd}]"
									<< std::endl;
								++listed;
							}
						}
					}
				}
			}
		}

		if (missing != 0) {
			std::cerr << "[WARN] perfect-information baseline incomplete: missing "
				<< missing << " / " << expected
				<< " tables for total<= " << kBaselineMaxTotalPieces << std::endl;
		}
	}

} // namespace geister_tb::detail

// =============================================================================
// Exported functions (definitions)
// =============================================================================

namespace geister_tb {

	void load_all_bins() {
		// Idempotent + thread-safe.
		std::call_once(detail::g_load_once, []() {
			try {
				detail::do_load_all_bins_guarded();
			}
			catch (const std::exception& e) {
				std::cerr << "[TB][FATAL] load_all_bins failed: " << e.what() << std::endl;
				throw;
			}
		});
	}

	void start_background_load() {
		bool expected = false;
		if (!detail::g_background_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
			return;
		}
		std::thread([]() {
			try {
				load_all_bins();
			}
			catch (const std::exception& e) {
				std::cerr << "[TB] background load failed: " << e.what() << std::endl;
			}
			catch (...) {
				std::cerr << "[TB] background load failed: unknown exception" << std::endl;
			}
		}).detach();
	}

	bool is_ready() noexcept {
		return detail::g_loaded.load(std::memory_order_acquire);
	}

	bool load_failed() noexcept {
		return detail::g_load_failed.load(std::memory_order_acquire);
	}

	[[nodiscard]] std::optional<std::uint8_t> probe_perfect_information(
		const perfect_information_geister& pos) noexcept
	{
		// If not loaded, behave as "not found".
		if (!detail::g_loaded.load(std::memory_order_acquire)) return std::nullopt;

		// Terminal values are defined irrespective of whether a table exists.
		if (pos.is_immediate_win()) return static_cast<std::uint8_t>(1);
		if (pos.is_immediate_loss()) return static_cast<std::uint8_t>(2);

		const int pb = std::popcount(pos.bb_player.bb_blue);
		const int pr = std::popcount(pos.bb_player.bb_red);
		const int ob = std::popcount(pos.bb_opponent.bb_blue);
		const int orc = std::popcount(pos.bb_opponent.bb_red);

		// Outside the ranked domain (should have been terminal). Treat as missing.
		if (pb < 1 || pb > 4 || pr < 1 || pr > 4 || ob < 1 || ob > 4 || orc < 1 || orc > 4) {
			return std::nullopt;
		}

		const std::uint16_t id = rank_material_configuration(pb, pr, ob, orc);
		const auto& ent = detail::g_perfect[static_cast<std::size_t>(id)];
		if (!ent.present) return std::nullopt;

		const std::uint64_t idx = ent.obsblk
			? rank_geister_perfect_information_obsblk(
				pos.bb_player.bb_blue,
				pos.bb_player.bb_red,
				pos.bb_opponent.bb_blue,
				pos.bb_opponent.bb_red)
			: rank_geister_perfect_information(
				pos.bb_player.bb_blue,
				pos.bb_player.bb_red,
				pos.bb_opponent.bb_blue,
				pos.bb_opponent.bb_red);

		switch (ent.kind) {
		case detail::storage_kind::raw_bin: {
			const auto span = ent.mf.u8span();
			if (idx >= span.size()) return std::nullopt; // defensive
			return span[static_cast<std::size_t>(idx)];
		}
		case detail::storage_kind::seekable_zstd: {
			std::uint8_t v = 0;
			if (!ent.zsf.read_u8(idx, v)) return std::nullopt;
			return v;
		}
		default:
			return std::nullopt;
		}
	}

} // namespace geister_tb