// geister_tb_handler.cxx
//
// Runtime probe facade for Geister endgame tablebases.
//
// Supported runtime tablebases:
//   - Perfect-information DTW tablebases.
//   - Purple DTW tablebases for Normal-to-move only.
//
// Supported on-disk formats:
//   - Raw .bin: headerless 1 byte/entry (direct mmap).
//   - Seekable zstd: .bin.zst / .bin.zstd for perfect-information tables.
//   - Seekable zstd: .bin.zst only for purple tables.
//
// Perfect-information tablebase naming:
//   - legacy (LR-canonical): id%03d_pb{pb}pr{pr}ob{ob}or{or}.bin
//   - obsblk (observation-block): id%03d_pb{pb}pr{pr}ob{ob}or{or}_obsblk.bin
//
// Purple runtime naming:
//   - tb_purple_N_k{k}_pb{pb}_pr{pr}_pp{pp}.bin
//   - tb_purple_N_k{k}_pb{pb}_pr{pr}_pp{pp}.bin.zst
//
// Notes for purple runtime support:
//   - Only N-side single-file tables are considered.
//   - tb_purple_P_* is ignored.
//   - partitioned artifacts such as *_partXX.bin are ignored.
//   - .bin.zstd is intentionally ignored for purple tables.
//
// Value encoding (1 byte):
//   0 unknown/draw, odd: win in that many plies, even: loss.

module;

#include <algorithm>
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
import geister_rank_triplet;

export module geister_tb_handler;

export namespace geister_tb {

	// Load (mmap) all recognised runtime tablebase files from the current directory.
	//
	// Behaviour:
	//   - Warn if any <=8-piece perfect-information baseline files are missing.
	//   - Silently load >=9-piece perfect-information tables if present.
	//   - Silently load purple N-side single-file tables if present.
	//   - Perfect-information priority: obsblk over legacy, seekable over raw.
	//   - Purple priority: seekable .bin.zst over raw .bin.
	//   - This is the synchronous path and may block until the scan + mmap completes.
	void load_all_bins();

	// Start asynchronous preload in a detached background thread.
	//
	// While loading is in progress, both probe functions behave as if no table were loaded yet.
	void start_background_load();

	// Whether a background or synchronous load has completed successfully.
	[[nodiscard]] bool is_ready() noexcept;

	// Whether a background or synchronous load attempt has failed.
	[[nodiscard]] bool load_failed() noexcept;

	// Probe perfect-information DTW tablebase.
	//  - Returns std::nullopt if the corresponding table file is not loaded.
	[[nodiscard]] std::optional<std::uint8_t> probe_perfect_information(
		const perfect_information_geister& pos) noexcept;

	// Probe purple DTW tablebase (Normal-to-move only).
	//
	// Interpretation:
	//   - pos is the current perfect-information position from the side-to-move perspective.
	//   - The probe ignores the opponent's red/blue split and uses the union as purple pieces.
	//   - k is the number of opponent red pieces already captured by the side to move.
	//
	// Returns std::nullopt if the corresponding purple table file is not loaded.
	struct purple_position {
		perfect_information_geister pos;
		std::uint8_t k = 0;
	};

	[[nodiscard]] std::optional<std::uint8_t> probe_purple(
		const purple_position& req) noexcept;

} // namespace geister_tb

module :private;

namespace geister_tb::detail {

	// ---------------------------------------------------------------------
	// Constants
	// ---------------------------------------------------------------------

	// Baseline completeness target:
	//  - We warn if any perfect-information tablebase for total pieces <= this value is missing.
	inline constexpr int kBaselineMaxTotalPieces = 8;

	// Perfect-information material domain size (4^4 = 256).
	inline constexpr std::size_t kPerfectDomainTotal = static_cast<std::size_t>(DOMAIN_TOTAL);

	// Purple runtime material domain: k in [0,3], pb/pr in [0,4], pp in [0,8].
	inline constexpr std::size_t kPurpleDomainTotal = 4ULL * 5ULL * 5ULL * 9ULL;

	inline constexpr std::uint64_t kExitSquares =
		(1ULL << static_cast<int>(POSITIONS::A1)) |
		(1ULL << static_cast<int>(POSITIONS::F1));

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

	struct PurpleMaterialKey {
		std::uint8_t k = 0;
		std::uint8_t pb = 0;
		std::uint8_t pr = 0;
		std::uint8_t pp = 0;
	};

	[[nodiscard]] inline bool purple_runtime_key_in_domain(const PurpleMaterialKey& key) noexcept {
		if (key.k > 3) return false;
		if (key.pb < 1 || key.pb > 4) return false;
		if (key.pr < 1 || key.pr > 4) return false;
		if (key.pp < 2 || key.pp > 8) return false;

		const int min_pp = std::max<int>(2, 5 - static_cast<int>(key.k));
		const int max_pp = 8 - static_cast<int>(key.k);
		if (static_cast<int>(key.pp) < min_pp) return false;
		if (static_cast<int>(key.pp) > max_pp) return false;

		return true;
	}

	[[nodiscard]] inline bool parse_purple_bin_filename(
		std::string_view name,
		char& out_turn,
		PurpleMaterialKey& out_key) noexcept
	{
		// tb_purple_N_k3_pb4_pr1_pp5.bin
		// Partitioned artifacts are intentionally not recognised here.
		std::size_t i = 0;
		if (!consume(name, i, "tb_purple_")) return false;
		if (i >= name.size()) return false;
		const char turn = name[i++];
		if (turn != 'N' && turn != 'P') return false;
		if (!consume(name, i, "_k")) return false;

		int k = 0, pb = 0, pr = 0, pp = 0;
		if (!parse_uint(name, i, k)) return false;
		if (!consume(name, i, "_pb")) return false;
		if (!parse_uint(name, i, pb)) return false;
		if (!consume(name, i, "_pr")) return false;
		if (!parse_uint(name, i, pr)) return false;
		if (!consume(name, i, "_pp")) return false;
		if (!parse_uint(name, i, pp)) return false;
		if (!consume(name, i, ".bin")) return false;
		if (i != name.size()) return false;

		PurpleMaterialKey key{
			static_cast<std::uint8_t>(k),
			static_cast<std::uint8_t>(pb),
			static_cast<std::uint8_t>(pr),
			static_cast<std::uint8_t>(pp)
		};
		if (!purple_runtime_key_in_domain(key)) return false;

		out_turn = turn;
		out_key = key;
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

	struct purple_entry {
		storage_kind kind = storage_kind::none;

		// raw .bin
		tbio::mmap::mapped_file mf;

		// seekable zstd (.bin.zst only for purple runtime)
		tbio::seekable_zstd::mapped_seekable_file zsf;

		PurpleMaterialKey key{};
		bool present = false;
	};

	inline std::array<perfect_entry, kPerfectDomainTotal> g_perfect{};
	inline std::array<purple_entry, kPurpleDomainTotal> g_purple{};

	inline std::once_flag g_load_once{};
	inline std::atomic<bool> g_loaded{ false };
	inline std::atomic<bool> g_load_failed{ false };
	inline std::atomic<bool> g_background_started{ false };

	[[nodiscard]] inline std::size_t purple_index(std::uint8_t k, std::uint8_t pb, std::uint8_t pr, std::uint8_t pp) noexcept {
		return (((static_cast<std::size_t>(k) * 5ULL + pb) * 5ULL + pr) * 9ULL + pp);
	}

	[[nodiscard]] inline int candidate_score(bool obsblk, bool seekable) noexcept {
		// obsblk has larger impact than compression preference.
		return (obsblk ? 2 : 0) + (seekable ? 1 : 0);
	}

	[[nodiscard]] inline int candidate_score(bool seekable) noexcept {
		return seekable ? 1 : 0;
	}

	[[nodiscard]] inline int entry_score(const perfect_entry& e) noexcept {
		return candidate_score(e.obsblk, e.kind == storage_kind::seekable_zstd);
	}

	[[nodiscard]] inline int entry_score(const purple_entry& e) noexcept {
		return candidate_score(e.kind == storage_kind::seekable_zstd);
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

	[[nodiscard]] inline std::uint64_t mapped_bytes(const purple_entry& e) noexcept {
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
		std::uint64_t loaded_perfect_files = 0;
		std::uint64_t loaded_perfect_bytes = 0;
		std::uint64_t loaded_purple_files = 0;
		std::uint64_t loaded_purple_bytes = 0;

		const std::filesystem::path dir = std::filesystem::current_path();

		for (const auto& ent : std::filesystem::directory_iterator(dir)) {
			std::error_code ec;
			if (!ent.is_regular_file(ec)) continue;

			const std::filesystem::path p = ent.path();

			bool is_seekable = false;
			bool is_purple_seekable_allowed = false;
			std::string parse_name;

			if (p.extension() == ".bin") {
				parse_name = p.filename().string();
				is_seekable = false;
				is_purple_seekable_allowed = false;
			}
			else if (p.extension() == ".zst" && p.stem().extension() == ".bin") {
				// example: id000_..._obsblk.bin.zst  -> parse "id000_..._obsblk.bin"
				parse_name = p.stem().filename().string();
				is_seekable = true;
				is_purple_seekable_allowed = true;
			}
			else if (p.extension() == ".zstd" && p.stem().extension() == ".bin") {
				// Perfect-information runtime still accepts .bin.zstd.
				parse_name = p.stem().filename().string();
				is_seekable = true;
				is_purple_seekable_allowed = false;
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
								loaded_perfect_bytes -= mapped_bytes(slot);
							}
							else {
								++loaded_perfect_files;
							}

							slot.zsf = std::move(zsf);
							slot.mf = {}; // release raw mapping if any
							slot.kind = storage_kind::seekable_zstd;
							slot.counts = c;
							slot.obsblk = obsblk;
							slot.present = true;

							loaded_perfect_bytes += mapped_bytes(slot);
						}
						else {
							auto mf = tbio::mmap::open_tablebase_bin_readonly(p, entries);
							mf.advise(tbio::mmap::advice::random);

							// Replace / install.
							if (slot.present) {
								loaded_perfect_bytes -= mapped_bytes(slot);
							}
							else {
								++loaded_perfect_files;
							}

							slot.mf = std::move(mf);
							slot.zsf = {}; // release seekable mapping if any
							slot.kind = storage_kind::raw_bin;
							slot.counts = c;
							slot.obsblk = obsblk;
							slot.present = true;

							loaded_perfect_bytes += mapped_bytes(slot);
						}
					}
					catch (const std::exception& e) {
						std::cerr << "[WARN] failed to load perfect table: " << p.filename().string()
							<< " (" << e.what() << ")" << std::endl;
					}
					continue;
				}
			}

			// Purple N-side single-file runtime tables.
			{
				char turn = 0;
				PurpleMaterialKey key{};
				if (parse_purple_bin_filename(parse_name, turn, key)) {
					if (turn != 'N') {
						continue; // runtime probes only Normal-to-move purple tables
					}
					if (is_seekable && !is_purple_seekable_allowed) {
						continue; // purple runtime intentionally ignores .bin.zstd
					}

					const std::uint64_t entries = states_for_counts(key.pb, key.pr, key.pp);
					auto& slot = g_purple[purple_index(key.k, key.pb, key.pr, key.pp)];
					const int new_score = candidate_score(is_seekable);
					const int old_score = slot.present ? entry_score(slot) : -1;

					if (slot.present && new_score <= old_score) {
						if (new_score == old_score) {
							std::cerr << "[WARN] duplicate purple table ignored: " << p.filename().string() << std::endl;
						}
						continue;
					}

					try {
						if (is_seekable) {
							auto zsf = tbio::seekable_zstd::open_tablebase_seekable_zstd_readonly(p, entries);
							zsf.advise(tbio::mmap::advice::random);

							if (slot.present) {
								loaded_purple_bytes -= mapped_bytes(slot);
							}
							else {
								++loaded_purple_files;
							}

							slot.zsf = std::move(zsf);
							slot.mf = {};
							slot.kind = storage_kind::seekable_zstd;
							slot.key = key;
							slot.present = true;

							loaded_purple_bytes += mapped_bytes(slot);
						}
						else {
							auto mf = tbio::mmap::open_tablebase_bin_readonly(p, entries);
							mf.advise(tbio::mmap::advice::random);

							if (slot.present) {
								loaded_purple_bytes -= mapped_bytes(slot);
							}
							else {
								++loaded_purple_files;
							}

							slot.mf = std::move(mf);
							slot.zsf = {};
							slot.kind = storage_kind::raw_bin;
							slot.key = key;
							slot.present = true;

							loaded_purple_bytes += mapped_bytes(slot);
						}
					}
					catch (const std::exception& e) {
						std::cerr << "[WARN] failed to load purple table: " << p.filename().string()
							<< " (" << e.what() << ")" << std::endl;
					}
					continue;
				}
			}

			// Other files are ignored here (belief TB, partitioned intermediates, etc.).
		}

		g_loaded.store(true, std::memory_order_release);

		std::cerr << "[TB] loaded perfect-information tables: " << loaded_perfect_files
			<< ", total mapped bytes: " << loaded_perfect_bytes
			<< " (cwd=" << dir.string() << ")" << std::endl;
		std::cerr << "[TB] loaded purple N-side tables: " << loaded_purple_files
			<< ", total mapped bytes: " << loaded_purple_bytes << std::endl;

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

	// ---------------------------------------------------------------------
	// Purple terminal detection (Normal-to-move convention only)
	// ---------------------------------------------------------------------

	[[nodiscard]] inline std::uint8_t purple_normal_immediate_value(const perfect_information_geister& pos, std::uint8_t k) noexcept {
		if (pos.bb_player.bb_blue & kExitSquares) return 1;
		if (pos.bb_player.bb_red == 0) return 1;
		if (pos.bb_opponent.bb_piece == 0) return 1;
		if (pos.bb_player.bb_blue == 0) return 2;
		if (k >= 4) return 2;
		return 0;
	}

	[[nodiscard]] inline std::optional<std::uint8_t> read_entry(const perfect_entry& ent, std::uint64_t idx) noexcept {
		switch (ent.kind) {
		case storage_kind::raw_bin: {
			const auto span = ent.mf.u8span();
			if (idx >= span.size()) return std::nullopt;
			return span[static_cast<std::size_t>(idx)];
		}
		case storage_kind::seekable_zstd: {
			std::uint8_t v = 0;
			if (!ent.zsf.read_u8(idx, v)) return std::nullopt;
			return v;
		}
		default:
			return std::nullopt;
		}
	}

	[[nodiscard]] inline std::optional<std::uint8_t> read_entry(const purple_entry& ent, std::uint64_t idx) noexcept {
		switch (ent.kind) {
		case storage_kind::raw_bin: {
			const auto span = ent.mf.u8span();
			if (idx >= span.size()) return std::nullopt;
			return span[static_cast<std::size_t>(idx)];
		}
		case storage_kind::seekable_zstd: {
			std::uint8_t v = 0;
			if (!ent.zsf.read_u8(idx, v)) return std::nullopt;
			return v;
		}
		default:
			return std::nullopt;
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

		return detail::read_entry(ent, idx);
	}

	[[nodiscard]] std::optional<std::uint8_t> probe_purple(
		const purple_position& req) noexcept
	{
		if (!detail::g_loaded.load(std::memory_order_acquire)) return std::nullopt;

		if (const std::uint8_t imm = detail::purple_normal_immediate_value(req.pos, req.k); imm != 0) {
			return imm;
		}

		const int pb = std::popcount(req.pos.bb_player.bb_blue);
		const int pr = std::popcount(req.pos.bb_player.bb_red);
		const int pp = std::popcount(req.pos.bb_opponent.bb_piece);
		if (pb < 1 || pb > 4 || pr < 1 || pr > 4 || pp < 2 || pp > 8 || req.k > 3) {
			return std::nullopt;
		}

		const detail::PurpleMaterialKey key{
			req.k,
			static_cast<std::uint8_t>(pb),
			static_cast<std::uint8_t>(pr),
			static_cast<std::uint8_t>(pp)
		};
		if (!detail::purple_runtime_key_in_domain(key)) {
			return std::nullopt;
		}

		const auto& ent = detail::g_purple[
			detail::purple_index(key.k, key.pb, key.pr, key.pp)];
		if (!ent.present) return std::nullopt;

		const std::uint64_t idx = rank_triplet_canon(
			req.pos.bb_player.bb_blue,
			req.pos.bb_player.bb_red,
			req.pos.bb_opponent.bb_piece);
		return detail::read_entry(ent, idx);
	}

} // namespace geister_tb
