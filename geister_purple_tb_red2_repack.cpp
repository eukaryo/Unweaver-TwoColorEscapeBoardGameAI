// geister_purple_tb_red2_repack.cpp
//
// Repack the partitioned red2 purple tablebases produced by
//   geister_purple_tb_red2.cpp
// into legacy single-file raw .bin tables:
//
//   tb_purple_N_k3_pb4_pr1_pp4.bin
//   tb_purple_P_k3_pb4_pr1_pp4.bin
//   tb_purple_N_k3_pb3_pr1_pp5.bin
//   tb_purple_P_k3_pb3_pr1_pp5.bin
//   tb_purple_N_k3_pb4_pr1_pp5.bin
//   tb_purple_P_k3_pb4_pr1_pp5.bin
//
// In the current public runtime integration, typically only the N-side files are
// consumed (`--turn N`), but the repacker can still emit both turns when needed.
//
// The output layout matches the legacy purple builder / handler convention:
//   - Normal-to-move: rank_triplet_canon(normal_blue, normal_red, purple_piece)
//   - Purple-to-move: rank_triplet_canon(purple_piece, normal_blue, normal_red)
//
// Important implementation choice:
//   The red2 builder stores final iterations as 18 LR-canonical "red-left"
//   partitions compressed with regular zstd (.bin.zst). Random access into the
//   source therefore is not directly possible. This repacker first inflates each
//   final partition to a raw staging .bin, mmaps those raws read-only, and then
//   emits the legacy single-file order chunk-by-chunk.
//
// Practical note:
//   For p10, one turn is ~160.14 GiB raw. During repack, the working disk usage
//   for one turn is roughly:
//     staged raw partitions + output file + small buffers
//   i.e. about 320+ GiB for p10. The code processes one turn at a time so the
//   peak stays at "one turn" rather than both turns at once.
//
// Build notes:
//   - Requires BMI2: compile with -mbmi2 -mbmi
//   - Requires zstd: link with -lzstd
//   - OpenMP is optional but recommended for the permutation step
//
// Example:
//   ./geister_purple_tb_red2_repack --in tb_purple_red2 --iter 210 --out . \
//       --stage .tb_purple_red2_stage --chunk-mib 256 --threads 32

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <immintrin.h>
#include <zstd.h>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#if defined(_OPENMP) && __has_include(<omp.h>)
#include <omp.h>
#define GEISTER_HAVE_OMP_HEADER 1
#else
#define GEISTER_HAVE_OMP_HEADER 0
#endif

import geister_core;
import geister_rank_triplet;

namespace fs = std::filesystem;

namespace {

// ============================================================
// Constants / geometry
// ============================================================

static constexpr std::uint64_t kBoardMask64 = bb_board;

static consteval std::uint64_t make_mask_cols(int c_lo, int c_hi_inclusive) {
	std::uint64_t m = 0;
	for (int r = 1; r <= 6; ++r) {
		for (int c = c_lo; c <= c_hi_inclusive; ++c) {
			const int sq = 8 * r + c;
			m |= (1ULL << sq);
		}
	}
	return m;
}

static constexpr std::uint64_t kLeftMask64 = make_mask_cols(1, 3);
static constexpr std::uint64_t kBoardMask36 = (1ULL << 36) - 1ULL;
static constexpr int kNumPartitions = 18;
static constexpr std::uint8_t kDefaultK = 3;
static constexpr std::uint8_t kDefaultPr = 1;

enum class TurnKind : std::uint8_t {
	NormalToMove = 0,
	PurpleToMove = 1,
};

struct PurpleMaterialKey {
	std::uint8_t k = 0;
	std::uint8_t pb = 0;
	std::uint8_t pr = 0;
	std::uint8_t pp = 0;
};

struct MaterialSpec {
	const char* tag = "";
	PurpleMaterialKey key{};
	std::uint64_t entries_per_part = 0;
	std::uint64_t total_entries = 0;
	std::uint64_t factor_normal_second = 0; // C(35-pb, pp)
	std::uint64_t factor_purple_second = 0; // C(35-pp, pb)
};

// ============================================================
// Logging helpers
// ============================================================

namespace tbutil {

using Clock = std::chrono::steady_clock;
static const Clock::time_point g_program_start = Clock::now();

[[nodiscard]] inline std::string format_hms(double sec) {
	if (sec < 0) sec = 0;
	std::uint64_t s = static_cast<std::uint64_t>(sec);
	const std::uint64_t h = s / 3600; s %= 3600;
	const std::uint64_t m = s / 60; s %= 60;
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
	std::cout << "[T+" << format_hms(sec) << "] ";
	(std::cout << ... << xs);
	std::cout << std::endl;
}

[[nodiscard]] inline bool parse_int(std::string_view s, int& out) {
	out = 0;
	const char* b = s.data();
	const char* e = s.data() + s.size();
	const auto [ptr, ec] = std::from_chars(b, e, out, 10);
	return ec == std::errc() && ptr == e;
}

[[nodiscard]] inline bool parse_u64(std::string_view s, std::uint64_t& out) {
	out = 0;
	const char* b = s.data();
	const char* e = s.data() + s.size();
	const auto [ptr, ec] = std::from_chars(b, e, out, 10);
	return ec == std::errc() && ptr == e;
}

[[nodiscard]] inline double to_gib(std::uint64_t bytes) noexcept {
	return static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
}

} // namespace tbutil

// ============================================================
// Filesystem helpers
// ============================================================

static void atomic_rename_best_effort(const fs::path& tmp, const fs::path& dst) {
	std::error_code ec;
	fs::rename(tmp, dst, ec);
	if (!ec) return;

	std::error_code ec2;
	fs::remove(dst, ec2);
	fs::rename(tmp, dst, ec);
	if (ec) {
		std::error_code ec3;
		fs::remove(tmp, ec3);
		throw std::runtime_error("rename failed: " + ec.message());
	}
}

[[nodiscard]] static inline fs::path tmp_path_for(const fs::path& filename) {
	fs::path tmp = filename;
	tmp += ".tmp";
	return tmp;
}

// ============================================================
// Regular-zstd streaming decompression
// ============================================================

static void decompress_zstd_to_raw_checked(
	const fs::path& input_zst,
	const fs::path& output_raw,
	std::uint64_t expected_size)
{
	const fs::path tmp = tmp_path_for(output_raw);
	std::error_code ec;
	fs::create_directories(output_raw.parent_path(), ec);

	std::ifstream ifs(input_zst, std::ios::binary);
	if (!ifs) {
		throw std::runtime_error("failed to open zstd input: " + input_zst.string());
	}
	std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
	if (!ofs) {
		throw std::runtime_error("failed to open raw output tmp: " + tmp.string());
	}

	ZSTD_DCtx* dctx = ZSTD_createDCtx();
	if (!dctx) {
		throw std::runtime_error("ZSTD_createDCtx failed");
	}

	std::vector<char> in_buf(ZSTD_DStreamInSize());
	std::vector<char> out_buf(ZSTD_DStreamOutSize());
	ZSTD_inBuffer input{ in_buf.data(), 0, 0 };

	std::uint64_t written = 0;
	bool done = false;
	try {
		while (!done) {
			if (input.pos == input.size) {
				ifs.read(in_buf.data(), static_cast<std::streamsize>(in_buf.size()));
				const std::streamsize got = ifs.gcount();
				if (got <= 0) input = { in_buf.data(), 0, 0 };
				else          input = { in_buf.data(), static_cast<size_t>(got), 0 };
			}

			ZSTD_outBuffer output{ out_buf.data(), out_buf.size(), 0 };
			const size_t ret = ZSTD_decompressStream(dctx, &output, &input);
			if (ZSTD_isError(ret)) {
				throw std::runtime_error(std::string("ZSTD_decompressStream: ") + ZSTD_getErrorName(ret));
			}

			if (output.pos != 0) {
				written += static_cast<std::uint64_t>(output.pos);
				if (written > expected_size) {
					throw std::runtime_error("decompressed output exceeds expected size");
				}
				ofs.write(out_buf.data(), static_cast<std::streamsize>(output.pos));
				if (!ofs) throw std::runtime_error("raw output write failed");
			}

			if (ret == 0) {
				done = true;
			}

			if (input.pos == input.size && ifs.eof() && !done) {
				throw std::runtime_error("truncated zstd stream (unexpected EOF)");
			}
		}

		if (written != expected_size) {
			std::ostringstream oss;
			oss << "decompressed size mismatch: wrote=" << written << " expected=" << expected_size;
			throw std::runtime_error(oss.str());
		}

		ofs.flush();
		ofs.close();
		ZSTD_freeDCtx(dctx);
		atomic_rename_best_effort(tmp, output_raw);
	}
	catch (...) {
		ZSTD_freeDCtx(dctx);
		std::error_code ec_rm;
		fs::remove(tmp, ec_rm);
		throw;
	}
}

// ============================================================
// Read-only raw mmap
// ============================================================

class mapped_raw_file {
public:
	mapped_raw_file() noexcept = default;
	mapped_raw_file(mapped_raw_file&& other) noexcept { move_from_(std::move(other)); }
	mapped_raw_file& operator=(mapped_raw_file&& other) noexcept {
		if (this != &other) {
			release_();
			move_from_(std::move(other));
		}
		return *this;
	}
	mapped_raw_file(const mapped_raw_file&) = delete;
	mapped_raw_file& operator=(const mapped_raw_file&) = delete;
	~mapped_raw_file() { release_(); }

	static mapped_raw_file open_readonly(const fs::path& path, std::uint64_t expected_size) {
#if defined(__unix__) || defined(__APPLE__)
		mapped_raw_file mf;
		mf.fd_ = ::open(path.c_str(), O_RDONLY);
		if (mf.fd_ < 0) {
			throw std::runtime_error("open failed: " + path.string());
		}

		struct stat st{};
		if (::fstat(mf.fd_, &st) != 0) {
			const int e = errno;
			::close(mf.fd_);
			mf.fd_ = -1;
			throw std::runtime_error("fstat failed: " + path.string() + " errno=" + std::to_string(e));
		}

		const std::uint64_t sz = static_cast<std::uint64_t>(st.st_size);
		if (sz != expected_size) {
			::close(mf.fd_);
			mf.fd_ = -1;
			std::ostringstream oss;
			oss << "raw stage size mismatch: file=" << sz << " expected=" << expected_size
				<< " (" << path.string() << ")";
			throw std::runtime_error(oss.str());
		}

		if (sz != 0) {
			mf.addr_ = ::mmap(nullptr, static_cast<size_t>(sz), PROT_READ, MAP_PRIVATE, mf.fd_, 0);
			if (mf.addr_ == MAP_FAILED) {
				const int e = errno;
				::close(mf.fd_);
				mf.fd_ = -1;
				mf.addr_ = nullptr;
				throw std::runtime_error("mmap failed: " + path.string() + " errno=" + std::to_string(e));
			}
			(void)::madvise(mf.addr_, static_cast<size_t>(sz), MADV_RANDOM);
		}

		mf.size_ = static_cast<size_t>(sz);
		return mf;
#else
		(void)path;
		(void)expected_size;
		throw std::runtime_error("mapped_raw_file is supported only on Unix-like platforms in this tool");
#endif
	}

	[[nodiscard]] inline const std::uint8_t* data() const noexcept {
		return reinterpret_cast<const std::uint8_t*>(addr_);
	}

	[[nodiscard]] inline std::size_t size() const noexcept { return size_; }

private:
	void release_() noexcept {
#if defined(__unix__) || defined(__APPLE__)
		if (addr_) {
			::munmap(addr_, size_);
			addr_ = nullptr;
		}
		if (fd_ >= 0) {
			::close(fd_);
			fd_ = -1;
		}
#endif
		size_ = 0;
	}

	void move_from_(mapped_raw_file&& other) noexcept {
		addr_ = other.addr_;
		size_ = other.size_;
#if defined(__unix__) || defined(__APPLE__)
		fd_ = other.fd_;
		other.fd_ = -1;
#endif
		other.addr_ = nullptr;
		other.size_ = 0;
	}

	void* addr_ = nullptr;
	std::size_t size_ = 0;
#if defined(__unix__) || defined(__APPLE__)
	int fd_ = -1;
#endif
};

// ============================================================
// Combinatorics / mirror / partition rank-unrank
// ============================================================

namespace comb {

consteval auto make_comb36() {
	std::array<std::array<std::uint64_t, 37>, 37> C{};
	for (int n = 0; n <= 36; ++n) {
		C[n][0] = 1;
		for (int k = 1; k <= n; ++k) {
			if (k == n) C[n][k] = 1;
			else C[n][k] = C[n - 1][k - 1] + C[n - 1][k];
		}
	}
	return C;
}

inline constexpr auto C = make_comb36();

[[nodiscard]] inline std::uint64_t rank_patterns_colex(std::uint64_t x, int P, int S) noexcept {
	assert(P >= 0 && P <= 36);
	assert(S >= 0 && S <= P);
	assert(std::popcount(x) == S);
	if (S == 0) return 0;

	std::uint64_t r = 0;
	int i = 1;
	while (x != 0) {
		const int p = std::countr_zero(x);
		r += C[p][i];
		++i;
		x &= (x - 1);
	}
	return r;
}

[[nodiscard]] inline std::uint64_t unrank_patterns_colex(std::uint64_t r, int P, int S) noexcept {
	assert(P >= 0 && P <= 36);
	assert(S >= 0 && S <= P);
	assert(r < C[P][S]);
	if (S == 0) return 0;

	std::uint64_t x = 0;
	int p = P;
	for (int i = S; i >= 1; --i) {
		int lo = i - 1;
		int hi = p;
		while (lo + 1 < hi) {
			const int mid = (lo + hi) >> 1;
			if (C[mid][i] <= r) lo = mid; else hi = mid;
		}
		p = lo;
		x |= (1ULL << p);
		r -= C[p][i];
	}
	return x;
}

} // namespace comb

namespace mirror {

consteval auto make_rev8_table() {
	std::array<std::uint8_t, 256> t{};
	for (int x = 0; x < 256; ++x) {
		std::uint8_t v = static_cast<std::uint8_t>(x);
		std::uint8_t r = 0;
		for (int i = 0; i < 8; ++i) {
			r = static_cast<std::uint8_t>((r << 1) | (v & 1));
			v >>= 1;
		}
		t[static_cast<size_t>(x)] = r;
	}
	return t;
}

inline constexpr auto REV8 = make_rev8_table();

[[nodiscard]] inline std::uint64_t mirror_lr_u64(std::uint64_t x) noexcept {
	std::uint64_t y = 0;
	for (int b = 0; b < 8; ++b) {
		const std::uint8_t byte = static_cast<std::uint8_t>((x >> (b * 8)) & 0xFF);
		y |= static_cast<std::uint64_t>(REV8[byte]) << (b * 8);
	}
	return y;
}

} // namespace mirror

[[nodiscard]] static inline std::uint64_t pext36(std::uint64_t bb64) noexcept {
	return _pext_u64(bb64, kBoardMask64);
}

[[nodiscard]] static inline std::uint64_t pdep36(std::uint64_t pat36) noexcept {
	return _pdep_u64(pat36, kBoardMask64);
}

[[nodiscard]] static inline int full_index_of_single_bb(std::uint64_t bb_single) noexcept {
	const std::uint64_t pat = pext36(bb_single);
	if (pat == 0) return -1;
	return std::countr_zero(pat);
}

struct PartitionIndex18 {
	std::array<int8_t, 36> full_to_part{};
	std::array<std::uint8_t, 18> part_to_full{};
	std::array<std::uint64_t, 18> part_red_bb64{};
	std::array<std::uint64_t, 18> part_red_pat36{};
};

static PartitionIndex18 build_partition_index18() {
	PartitionIndex18 pi{};
	pi.full_to_part.fill(-1);

	int part = 0;
	for (int full = 0; full < 36; ++full) {
		const std::uint64_t pat = 1ULL << full;
		const std::uint64_t bb = pdep36(pat);
		if ((bb & kLeftMask64) != 0) {
			if (part >= 18) throw std::runtime_error("left-half squares > 18? unexpected");
			pi.full_to_part[static_cast<size_t>(full)] = static_cast<int8_t>(part);
			pi.part_to_full[static_cast<size_t>(part)] = static_cast<std::uint8_t>(full);
			pi.part_red_bb64[static_cast<size_t>(part)] = bb;
			pi.part_red_pat36[static_cast<size_t>(part)] = pat;
			++part;
		}
	}
	if (part != 18) throw std::runtime_error("left-half squares != 18");
	return pi;
}

[[nodiscard]] static inline bool is_left_red(std::uint64_t red_single) noexcept {
	return (red_single & kLeftMask64) != 0;
}

static inline void canonicalize_red_left_normal(std::uint64_t& normal_blue, std::uint64_t& normal_red, std::uint64_t& purple_piece) noexcept {
	if (is_left_red(normal_red)) return;
	normal_blue = mirror::mirror_lr_u64(normal_blue);
	normal_red = mirror::mirror_lr_u64(normal_red);
	purple_piece = mirror::mirror_lr_u64(purple_piece);
}

static inline void canonicalize_red_left_purple(std::uint64_t& purple_piece, std::uint64_t& normal_blue, std::uint64_t& normal_red) noexcept {
	if (is_left_red(normal_red)) return;
	purple_piece = mirror::mirror_lr_u64(purple_piece);
	normal_blue = mirror::mirror_lr_u64(normal_blue);
	normal_red = mirror::mirror_lr_u64(normal_red);
}

struct RankResult {
	std::uint16_t part = 0;
	std::uint64_t idx = 0;
};

[[nodiscard]] static RankResult rank_partitioned_normal(
	const PartitionIndex18& pi,
	const MaterialSpec& mat,
	std::uint64_t normal_blue,
	std::uint64_t normal_red,
	std::uint64_t purple_piece) noexcept
{
	canonicalize_red_left_normal(normal_blue, normal_red, purple_piece);

	const int full_r = full_index_of_single_bb(normal_red);
	assert(full_r >= 0);
	const int part = pi.full_to_part[static_cast<size_t>(full_r)];
	assert(part >= 0);

	const std::uint64_t red_pat36 = 1ULL << static_cast<std::uint64_t>(full_r);
	const std::uint64_t rem_mask36 = kBoardMask36 ^ red_pat36;

	const std::uint64_t blue_pat36 = pext36(normal_blue);
	const std::uint64_t purple_pat36 = pext36(purple_piece);

	const std::uint64_t blue_pat35 = _pext_u64(blue_pat36, rem_mask36);
	const std::uint64_t r_blue = comb::rank_patterns_colex(blue_pat35, 35, mat.key.pb);

	const std::uint64_t rem2_mask36 = rem_mask36 ^ blue_pat36;
	const int N2 = 35 - static_cast<int>(mat.key.pb);
	const std::uint64_t purple_patN2 = _pext_u64(purple_pat36, rem2_mask36);
	const std::uint64_t r_purple = comb::rank_patterns_colex(purple_patN2, N2, mat.key.pp);

	const std::uint64_t idx = r_blue * mat.factor_normal_second + r_purple;
	assert(idx < mat.entries_per_part);
	return RankResult{ static_cast<std::uint16_t>(part), idx };
}

[[nodiscard]] static RankResult rank_partitioned_purple(
	const PartitionIndex18& pi,
	const MaterialSpec& mat,
	std::uint64_t purple_piece,
	std::uint64_t normal_blue,
	std::uint64_t normal_red) noexcept
{
	canonicalize_red_left_purple(purple_piece, normal_blue, normal_red);

	const int full_r = full_index_of_single_bb(normal_red);
	assert(full_r >= 0);
	const int part = pi.full_to_part[static_cast<size_t>(full_r)];
	assert(part >= 0);

	const std::uint64_t red_pat36 = 1ULL << static_cast<std::uint64_t>(full_r);
	const std::uint64_t rem_mask36 = kBoardMask36 ^ red_pat36;

	const std::uint64_t purple_pat36 = pext36(purple_piece);
	const std::uint64_t blue_pat36 = pext36(normal_blue);

	const std::uint64_t purple_pat35 = _pext_u64(purple_pat36, rem_mask36);
	const std::uint64_t r_purple = comb::rank_patterns_colex(purple_pat35, 35, mat.key.pp);

	const std::uint64_t rem2_mask36 = rem_mask36 ^ purple_pat36;
	const int N2 = 35 - static_cast<int>(mat.key.pp);
	const std::uint64_t blue_patN2 = _pext_u64(blue_pat36, rem2_mask36);
	const std::uint64_t r_blue = comb::rank_patterns_colex(blue_patN2, N2, mat.key.pb);

	const std::uint64_t idx = r_purple * mat.factor_purple_second + r_blue;
	assert(idx < mat.entries_per_part);
	return RankResult{ static_cast<std::uint16_t>(part), idx };
}

static inline perfect_information_geister unrank_partitioned_normal(
	const PartitionIndex18& pi,
	const MaterialSpec& mat,
	std::uint16_t part,
	std::uint64_t idx)
{
	assert(part < 18);
	assert(idx < mat.entries_per_part);

	const std::uint64_t red_pat36 = pi.part_red_pat36[static_cast<size_t>(part)];
	const std::uint64_t rem_mask36 = kBoardMask36 ^ red_pat36;

	const std::uint64_t r_blue = idx / mat.factor_normal_second;
	const std::uint64_t r_purple = idx % mat.factor_normal_second;

	const std::uint64_t blue_pat35 = comb::unrank_patterns_colex(r_blue, 35, mat.key.pb);
	const std::uint64_t blue_pat36 = _pdep_u64(blue_pat35, rem_mask36);

	const std::uint64_t rem2_mask36 = rem_mask36 ^ blue_pat36;
	const int N2 = 35 - static_cast<int>(mat.key.pb);
	const std::uint64_t purple_patN2 = comb::unrank_patterns_colex(r_purple, N2, mat.key.pp);
	const std::uint64_t purple_pat36 = _pdep_u64(purple_patN2, rem2_mask36);

	const std::uint64_t normal_red = pdep36(red_pat36);
	const std::uint64_t normal_blue = pdep36(blue_pat36);
	const std::uint64_t purple_piece = pdep36(purple_pat36);

	player_board player{ normal_red, normal_blue };
	player_board opponent{ 0ULL, purple_piece };
	return perfect_information_geister{ player, opponent };
}

static inline perfect_information_geister unrank_partitioned_purple(
	const PartitionIndex18& pi,
	const MaterialSpec& mat,
	std::uint16_t part,
	std::uint64_t idx)
{
	assert(part < 18);
	assert(idx < mat.entries_per_part);

	const std::uint64_t red_pat36 = pi.part_red_pat36[static_cast<size_t>(part)];
	const std::uint64_t rem_mask36 = kBoardMask36 ^ red_pat36;

	const std::uint64_t r_purple = idx / mat.factor_purple_second;
	const std::uint64_t r_blue = idx % mat.factor_purple_second;

	const std::uint64_t purple_pat35 = comb::unrank_patterns_colex(r_purple, 35, mat.key.pp);
	const std::uint64_t purple_pat36 = _pdep_u64(purple_pat35, rem_mask36);

	const std::uint64_t rem2_mask36 = rem_mask36 ^ purple_pat36;
	const int N2 = 35 - static_cast<int>(mat.key.pp);
	const std::uint64_t blue_patN2 = comb::unrank_patterns_colex(r_blue, N2, mat.key.pb);
	const std::uint64_t blue_pat36 = _pdep_u64(blue_patN2, rem2_mask36);

	const std::uint64_t normal_red = pdep36(red_pat36);
	const std::uint64_t normal_blue = pdep36(blue_pat36);
	const std::uint64_t purple_piece = pdep36(purple_pat36);

	player_board player{ 0ULL, purple_piece };
	player_board opponent{ normal_red, normal_blue };
	return perfect_information_geister{ player, opponent };
}

[[nodiscard]] static inline std::uint64_t rank_legacy_normal_turn(const perfect_information_geister& pos) noexcept {
	return rank_triplet_canon(pos.bb_player.bb_blue, pos.bb_player.bb_red, pos.bb_opponent.bb_blue);
}

[[nodiscard]] static inline std::uint64_t rank_legacy_purple_turn(const perfect_information_geister& pos) noexcept {
	return rank_triplet_canon(pos.bb_player.bb_blue, pos.bb_opponent.bb_blue, pos.bb_opponent.bb_red);
}

[[nodiscard]] static MaterialSpec make_material(const char* tag, int pb, int pp) {
	MaterialSpec m{};
	m.tag = tag;
	m.key = PurpleMaterialKey{ kDefaultK, static_cast<std::uint8_t>(pb), kDefaultPr, static_cast<std::uint8_t>(pp) };
	m.factor_normal_second = comb::C[35 - pb][pp];
	m.factor_purple_second = comb::C[35 - pp][pb];
	m.entries_per_part = comb::C[35][pb] * m.factor_normal_second;
	m.total_entries = states_for_counts(static_cast<std::uint8_t>(pb), kDefaultPr, static_cast<std::uint8_t>(pp));

	const std::uint64_t expect = m.entries_per_part * 18ULL;
	if (m.total_entries != expect) {
		std::ostringstream oss;
		oss << "unexpected total_entries for " << tag << ": states_for_counts=" << m.total_entries
			<< " but part_total=" << expect;
		throw std::runtime_error(oss.str());
	}
	return m;
}

// ============================================================
// Names / paths
// ============================================================

[[nodiscard]] static inline std::string iter_dir_name(int iter) {
	std::ostringstream oss;
	oss << "iter_" << std::setw(3) << std::setfill('0') << iter;
	return oss.str();
}

[[nodiscard]] static inline fs::path material_root_path(const fs::path& in_root, const MaterialSpec& mat) {
	std::ostringstream oss;
	oss << "tb_purple_red2_k" << int(mat.key.k)
		<< "_pb" << int(mat.key.pb)
		<< "_pr" << int(mat.key.pr)
		<< "_pp" << int(mat.key.pp);
	return in_root / oss.str();
}

[[nodiscard]] static inline std::string part_file_base_name(
	const MaterialSpec& mat,
	TurnKind turn,
	std::uint16_t part)
{
	std::ostringstream oss;
	oss << "tb_purple_" << (turn == TurnKind::NormalToMove ? 'N' : 'P')
		<< "_k" << int(mat.key.k)
		<< "_pb" << int(mat.key.pb)
		<< "_pr" << int(mat.key.pr)
		<< "_pp" << int(mat.key.pp)
		<< "_part" << std::setw(2) << std::setfill('0') << int(part)
		<< ".bin";
	return oss.str();
}

[[nodiscard]] static inline std::string legacy_full_bin_name(const PurpleMaterialKey& key, TurnKind turn) {
	std::ostringstream oss;
	oss << "tb_purple_" << (turn == TurnKind::NormalToMove ? 'N' : 'P')
		<< "_k" << int(key.k)
		<< "_pb" << int(key.pb)
		<< "_pr" << int(key.pr)
		<< "_pp" << int(key.pp)
		<< ".bin";
	return oss.str();
}

[[nodiscard]] static fs::path find_existing_part_input(
	const fs::path& material_root,
	const MaterialSpec& mat,
	TurnKind turn,
	int iter,
	std::uint16_t part)
{
	const fs::path iter_dir = material_root / iter_dir_name(iter);
	const std::string base = part_file_base_name(mat, turn, part);

	const fs::path raw = iter_dir / base;
	if (fs::exists(raw)) return raw;

	const fs::path zst = iter_dir / (base + ".zst");
	if (fs::exists(zst)) return zst;

	const fs::path zstd = iter_dir / (base + ".zstd");
	if (fs::exists(zstd)) return zstd;

	throw std::runtime_error("missing input partition file under " + iter_dir.string() + ": " + base + "{,.zst,.zstd}");
}

[[nodiscard]] static inline fs::path stage_turn_dir(
	const fs::path& stage_root,
	const MaterialSpec& mat,
	TurnKind turn,
	int iter)
{
	std::ostringstream oss;
	oss << "k" << int(mat.key.k)
		<< "_pb" << int(mat.key.pb)
		<< "_pr" << int(mat.key.pr)
		<< "_pp" << int(mat.key.pp)
		<< "_" << (turn == TurnKind::NormalToMove ? 'N' : 'P')
		<< "_i" << std::setw(3) << std::setfill('0') << iter;
	return stage_root / oss.str();
}

// ============================================================
// CLI
// ============================================================

struct Args {
	fs::path in_root = "tb_purple_red2";
	fs::path out_dir = ".";
	fs::path stage_root = ".tb_purple_red2_stage";
	int iter = 210;
	enum class OnlyMat : std::uint8_t { All, P9A, P9B, P10 } only_mat = OnlyMat::All;
	enum class TurnSel : std::uint8_t { Both, N, P } turn_sel = TurnSel::Both;
	std::uint64_t chunk_mib = 256;
	int threads = 0;
	bool keep_stage = false;
	bool skip_existing = false;
	bool self_test = false;
	bool self_test_only = false;
	std::uint64_t self_test_samples = 10000;
};

static void print_usage(const char* argv0) {
	std::cerr
		<< "Usage: " << argv0 << " [options]\n"
		<< "  --in DIR              base directory that contains the material roots produced by geister_purple_tb_red2.cpp\n"
		<< "                        (default: tb_purple_red2)\n"
		<< "  --out DIR             output directory for legacy single-file *.bin tables (default: current dir)\n"
		<< "  --stage DIR           raw staging directory for inflated partitions (default: .tb_purple_red2_stage)\n"
		<< "  --iter N              completed iteration depth to repack (default: 210)\n"
		<< "  --only TAG            one of: p9a | p9b | p10 (default: all)\n"
		<< "  --turn SEL            one of: both | N | P (default: both)\n"
		<< "  --chunk-mib M         output chunk size in MiB (default: 256)\n"
		<< "  --threads T           OpenMP threads for the permutation step (default: use OMP_NUM_THREADS)\n"
		<< "  --keep-stage          keep inflated raw partitions after repack\n"
		<< "  --skip-existing       skip an output if a same-sized file already exists\n"
		<< "  --self-test           run random mapping checks before repacking\n"
		<< "  --self-test-only      run the mapping checks and exit without touching table files\n"
		<< "  --self-test-samples N number of random mapping checks per material/turn for --self-test (default: 10000)\n"
		<< "  --help, -h            show this help\n";
}

static Args parse_args(int argc, char** argv) {
	Args a{};
	for (int i = 1; i < argc; ++i) {
		const std::string_view s(argv[i]);
		auto need_value = [&](std::string_view opt) -> std::string_view {
			if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + std::string(opt));
			return std::string_view(argv[++i]);
		};

		if (s == "--in") {
			a.in_root = fs::path(std::string(need_value(s)));
		}
		else if (s == "--out") {
			a.out_dir = fs::path(std::string(need_value(s)));
		}
		else if (s == "--stage") {
			a.stage_root = fs::path(std::string(need_value(s)));
		}
		else if (s == "--iter") {
			int v = 0;
			if (!tbutil::parse_int(need_value(s), v)) throw std::runtime_error("bad --iter");
			a.iter = v;
		}
		else if (s == "--only") {
			const auto v = need_value(s);
			if (v == "p9a") a.only_mat = Args::OnlyMat::P9A;
			else if (v == "p9b") a.only_mat = Args::OnlyMat::P9B;
			else if (v == "p10") a.only_mat = Args::OnlyMat::P10;
			else throw std::runtime_error("--only must be one of: p9a, p9b, p10");
		}
		else if (s == "--turn") {
			const auto v = need_value(s);
			if (v == "both") a.turn_sel = Args::TurnSel::Both;
			else if (v == "N" || v == "n") a.turn_sel = Args::TurnSel::N;
			else if (v == "P" || v == "p") a.turn_sel = Args::TurnSel::P;
			else throw std::runtime_error("--turn must be one of: both, N, P");
		}
		else if (s == "--chunk-mib") {
			std::uint64_t v = 0;
			if (!tbutil::parse_u64(need_value(s), v)) throw std::runtime_error("bad --chunk-mib");
			a.chunk_mib = v;
		}
		else if (s == "--threads") {
			int v = 0;
			if (!tbutil::parse_int(need_value(s), v)) throw std::runtime_error("bad --threads");
			a.threads = v;
		}
		else if (s == "--keep-stage") {
			a.keep_stage = true;
		}
		else if (s == "--skip-existing") {
			a.skip_existing = true;
		}
		else if (s == "--self-test") {
			a.self_test = true;
		}
		else if (s == "--self-test-only") {
			a.self_test = true;
			a.self_test_only = true;
		}
		else if (s == "--self-test-samples") {
			std::uint64_t v = 0;
			if (!tbutil::parse_u64(need_value(s), v)) throw std::runtime_error("bad --self-test-samples");
			a.self_test_samples = v;
		}
		else if (s == "--help" || s == "-h") {
			print_usage(argv[0]);
			std::exit(0);
		}
		else {
			throw std::runtime_error(std::string("unknown option: ") + std::string(s));
		}
	}

	if (a.iter <= 0) throw std::runtime_error("--iter must be > 0");
	if (a.chunk_mib == 0) throw std::runtime_error("--chunk-mib must be > 0");
	if (a.threads < 0) throw std::runtime_error("--threads must be >= 0");
	if (a.self_test_samples == 0) throw std::runtime_error("--self-test-samples must be > 0");
	return a;
}

// ============================================================
// Self-test
// ============================================================

static void run_mapping_self_test_one(
	const PartitionIndex18& pi,
	const MaterialSpec& mat,
	TurnKind turn,
	std::uint64_t samples)
{
	tbutil::log_line("[self-test] ", mat.tag, " turn=", (turn == TurnKind::NormalToMove ? 'N' : 'P'),
					" samples=", samples);

	std::mt19937_64 rng(0x9e3779b97f4a7c15ULL
		^ (static_cast<std::uint64_t>(mat.key.pb) << 8)
		^ (static_cast<std::uint64_t>(mat.key.pp) << 16)
		^ (turn == TurnKind::NormalToMove ? 0x1234ULL : 0x4321ULL));
	std::uniform_int_distribution<int> part_dist(0, 17);
	std::uniform_int_distribution<std::uint64_t> idx_dist(0, mat.entries_per_part - 1);

	for (std::uint64_t t = 0; t < samples; ++t) {
		const std::uint16_t part = static_cast<std::uint16_t>(part_dist(rng));
		const std::uint64_t idx = idx_dist(rng);

		if (turn == TurnKind::NormalToMove) {
			const auto pos = unrank_partitioned_normal(pi, mat, part, idx);
			const std::uint64_t legacy = rank_legacy_normal_turn(pos);
			std::uint64_t b = 0, r = 0, p = 0;
			unrank_triplet_canon(legacy, mat.key.pb, mat.key.pr, mat.key.pp, b, r, p);
			const RankResult rr = rank_partitioned_normal(pi, mat, b, r, p);
			if (!(rr.part == part && rr.idx == idx)) {
				throw std::runtime_error("self-test failed (normal turn mapping mismatch)");
			}
		}
		else {
			const auto pos = unrank_partitioned_purple(pi, mat, part, idx);
			const std::uint64_t legacy = rank_legacy_purple_turn(pos);
			std::uint64_t p = 0, b = 0, r = 0;
			unrank_triplet_canon(legacy, mat.key.pp, mat.key.pb, mat.key.pr, p, b, r);
			const RankResult rr = rank_partitioned_purple(pi, mat, p, b, r);
			if (!(rr.part == part && rr.idx == idx)) {
				throw std::runtime_error("self-test failed (purple turn mapping mismatch)");
			}
		}
	}
}

// ============================================================
// Staging / input preparation
// ============================================================

struct PreparedRawInputs {
	std::array<fs::path, 18> raw_path{};
	bool used_stage = false;
	fs::path stage_dir{};
};

static PreparedRawInputs prepare_raw_inputs_for_turn(
	const Args& args,
	const MaterialSpec& mat,
	TurnKind turn)
{
	PreparedRawInputs prep{};
	prep.stage_dir = stage_turn_dir(args.stage_root, mat, turn, args.iter);

	const fs::path material_root = material_root_path(args.in_root, mat);
	const std::uint64_t expected_part_bytes = mat.entries_per_part;

	for (int pid = 0; pid < 18; ++pid) {
		const fs::path src = find_existing_part_input(material_root, mat, turn, args.iter, static_cast<std::uint16_t>(pid));
		if (src.extension() == ".bin") {
			const auto sz = fs::file_size(src);
			if (sz != expected_part_bytes) {
				std::ostringstream oss;
				oss << "raw partition size mismatch: file=" << sz << " expected=" << expected_part_bytes
					<< " (" << src.string() << ")";
				throw std::runtime_error(oss.str());
			}
			prep.raw_path[static_cast<size_t>(pid)] = src;
			continue;
		}

		prep.used_stage = true;
		std::error_code ec;
		fs::create_directories(prep.stage_dir, ec);
		const fs::path dst = prep.stage_dir / part_file_base_name(mat, turn, static_cast<std::uint16_t>(pid));
		if (fs::exists(dst)) {
			const auto sz = fs::file_size(dst);
			if (sz == expected_part_bytes) {
				prep.raw_path[static_cast<size_t>(pid)] = dst;
				continue;
			}
			std::error_code ec_rm;
			fs::remove(dst, ec_rm);
		}

		tbutil::log_line("[stage] ", mat.tag, " turn=", (turn == TurnKind::NormalToMove ? 'N' : 'P'),
					" part=", pid, " <- ", src.string(), " -> ", dst.string(),
					" (", std::fixed, std::setprecision(3), tbutil::to_gib(expected_part_bytes), " GiB raw)");
		decompress_zstd_to_raw_checked(src, dst, expected_part_bytes);
		prep.raw_path[static_cast<size_t>(pid)] = dst;
	}

	return prep;
}

// ============================================================
// Repack core
// ============================================================

static void repack_one_turn(
	const Args& args,
	const PartitionIndex18& pi,
	const MaterialSpec& mat,
	TurnKind turn)
{
	const fs::path out_path = args.out_dir / legacy_full_bin_name(mat.key, turn);
	const std::uint64_t total_entries = mat.total_entries;

	if (args.skip_existing && fs::exists(out_path) && fs::file_size(out_path) == total_entries) {
		tbutil::log_line("[skip] ", out_path.string(), " already exists with expected size");
		return;
	}

	const PreparedRawInputs prep = prepare_raw_inputs_for_turn(args, mat, turn);

	std::array<mapped_raw_file, 18> parts{};
	for (int pid = 0; pid < 18; ++pid) {
		parts[static_cast<size_t>(pid)] = mapped_raw_file::open_readonly(prep.raw_path[static_cast<size_t>(pid)], mat.entries_per_part);
	}

	std::error_code ec;
	fs::create_directories(args.out_dir, ec);
	const fs::path out_tmp = tmp_path_for(out_path);

	std::ofstream ofs(out_tmp, std::ios::binary | std::ios::trunc);
	if (!ofs) {
		throw std::runtime_error("failed to open output tmp: " + out_tmp.string());
	}

	const std::uint64_t chunk_bytes = args.chunk_mib * (1ULL << 20);
	std::vector<std::uint8_t> out_buf(static_cast<size_t>(chunk_bytes));

	tbutil::log_line("[repack] begin ", mat.tag,
				" turn=", (turn == TurnKind::NormalToMove ? 'N' : 'P'),
				" total_entries=", total_entries,
				" (", std::fixed, std::setprecision(3), tbutil::to_gib(total_entries), " GiB)",
				" chunk=", args.chunk_mib, " MiB");

	const auto t0 = tbutil::Clock::now();
	std::uint64_t base = 0;
	std::uint64_t next_log_at = 0;
	const std::uint64_t log_step = std::max<std::uint64_t>(total_entries / 64ULL, chunk_bytes); // ~64 logs max

	while (base < total_entries) {
		const std::uint64_t take = std::min<std::uint64_t>(chunk_bytes, total_entries - base);

		if (turn == TurnKind::NormalToMove) {
#ifdef _OPENMP
#pragma omp parallel for schedule(static)
#endif
			for (std::int64_t i = 0; i < static_cast<std::int64_t>(take); ++i) {
				const std::uint64_t idx = base + static_cast<std::uint64_t>(i);
				std::uint64_t normal_blue = 0, normal_red = 0, purple_piece = 0;
				unrank_triplet_canon(idx, mat.key.pb, mat.key.pr, mat.key.pp, normal_blue, normal_red, purple_piece);
				const RankResult rr = rank_partitioned_normal(pi, mat, normal_blue, normal_red, purple_piece);
				out_buf[static_cast<size_t>(i)] = parts[static_cast<size_t>(rr.part)].data()[static_cast<size_t>(rr.idx)];
			}
		}
		else {
#ifdef _OPENMP
#pragma omp parallel for schedule(static)
#endif
			for (std::int64_t i = 0; i < static_cast<std::int64_t>(take); ++i) {
				const std::uint64_t idx = base + static_cast<std::uint64_t>(i);
				std::uint64_t purple_piece = 0, normal_blue = 0, normal_red = 0;
				unrank_triplet_canon(idx, mat.key.pp, mat.key.pb, mat.key.pr, purple_piece, normal_blue, normal_red);
				const RankResult rr = rank_partitioned_purple(pi, mat, purple_piece, normal_blue, normal_red);
				out_buf[static_cast<size_t>(i)] = parts[static_cast<size_t>(rr.part)].data()[static_cast<size_t>(rr.idx)];
			}
		}

		ofs.write(reinterpret_cast<const char*>(out_buf.data()), static_cast<std::streamsize>(take));
		if (!ofs) throw std::runtime_error("output write failed: " + out_tmp.string());
		base += take;

		if (base >= next_log_at || base == total_entries) {
			const double elapsed = std::chrono::duration<double>(tbutil::Clock::now() - t0).count();
			const double frac = (total_entries == 0) ? 1.0 : (static_cast<double>(base) / static_cast<double>(total_entries));
			const double rate_gib_s = (elapsed > 0.0) ? (tbutil::to_gib(base) / elapsed) : 0.0;
			std::ostringstream pct;
			pct << std::fixed << std::setprecision(2) << (frac * 100.0);
			tbutil::log_line("[repack] ", mat.tag,
						" turn=", (turn == TurnKind::NormalToMove ? 'N' : 'P'),
						" progress=", pct.str(), "%",
						" bytes=", base, "/", total_entries,
						" (", std::fixed, std::setprecision(3), tbutil::to_gib(base), "/", tbutil::to_gib(total_entries), " GiB)",
						" rate=", std::fixed, std::setprecision(3), rate_gib_s, " GiB/s");
			next_log_at = base + log_step;
		}
	}

	ofs.flush();
	ofs.close();
	atomic_rename_best_effort(out_tmp, out_path);

	const double sec = std::chrono::duration<double>(tbutil::Clock::now() - t0).count();
	tbutil::log_line("[repack] done ", out_path.string(),
				" (", std::fixed, std::setprecision(3), sec, "s)");

	if (prep.used_stage && !args.keep_stage) {
		std::error_code ec_rm;
		fs::remove_all(prep.stage_dir, ec_rm);
		if (ec_rm) {
			tbutil::log_line("[WARN] failed to remove stage dir: ", prep.stage_dir.string(), " : ", ec_rm.message());
		}
	}
}

} // namespace

int main(int argc, char** argv) {
	try {
		const Args args = parse_args(argc, argv);

#if defined(_OPENMP) && GEISTER_HAVE_OMP_HEADER
		if (args.threads > 0) {
			omp_set_num_threads(args.threads);
		}
#endif

		const PartitionIndex18 pi = build_partition_index18();
		const MaterialSpec p9a = make_material("p9a", 4, 4);
		const MaterialSpec p9b = make_material("p9b", 3, 5);
		const MaterialSpec p10 = make_material("p10", 4, 5);

		std::vector<MaterialSpec> mats;
		switch (args.only_mat) {
		case Args::OnlyMat::All:
			mats = { p9a, p9b, p10 };
			break;
		case Args::OnlyMat::P9A:
			mats = { p9a };
			break;
		case Args::OnlyMat::P9B:
			mats = { p9b };
			break;
		case Args::OnlyMat::P10:
			mats = { p10 };
			break;
		}

		if (args.self_test) {
			for (const auto& mat : mats) {
				if (args.turn_sel == Args::TurnSel::Both || args.turn_sel == Args::TurnSel::N) {
					run_mapping_self_test_one(pi, mat, TurnKind::NormalToMove, args.self_test_samples);
				}
				if (args.turn_sel == Args::TurnSel::Both || args.turn_sel == Args::TurnSel::P) {
					run_mapping_self_test_one(pi, mat, TurnKind::PurpleToMove, args.self_test_samples);
				}
			}
			tbutil::log_line("[self-test] all checks passed");
			if (args.self_test_only) return 0;
		}

		for (const auto& mat : mats) {
			if (args.turn_sel == Args::TurnSel::Both || args.turn_sel == Args::TurnSel::N) {
				repack_one_turn(args, pi, mat, TurnKind::NormalToMove);
			}
			if (args.turn_sel == Args::TurnSel::Both || args.turn_sel == Args::TurnSel::P) {
				repack_one_turn(args, pi, mat, TurnKind::PurpleToMove);
			}
		}

		return 0;
	}
	catch (const std::exception& e) {
		std::cerr << "ERROR: " << e.what() << "\n";
		print_usage(argv[0]);
		return 1;
	}
}
