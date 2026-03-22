// geister_purple_tb_red2.cpp
//   Partitioned DTW builder for the purple tablebase restricted to
//   "on-board red count == 2", i.e. (k=3, pr=1).
//
// Built materials:
//   p9a : (k,pb,pr,pp) = (3,4,1,4)
//   p9b : (k,pb,pr,pp) = (3,3,1,5)
//   p10 : (k,pb,pr,pp) = (3,4,1,5)
//
// Storage / update strategy:
//   - Canonical domain is split into 18 partitions by the unique Normal-red square
//     after LR canonicalization (red must be on the left half).
//   - Each iteration is stored as regular zstd-compressed raw byte tables
//     (1 byte / entry), one file per (turn, part).
//   - Current-part input is streamed chunk-by-chunk from zstd.
//   - Same-material previous-iteration children are queried through on-demand
//     partition bitset caches: {known, odd, exact(prev_iter)}.
//   - Lower dependencies are queried from existing <=8 full purple TBs (legacy
//     single-file format) or, for p10, from the completed p9b partitioned build.
//
// Optional raw partition export (debug / inspection only):
//   --export-runtime-normal-parts
//     Decompresses the completed final Normal-to-move partitions into flat raw
//     .bin files named
//       tb_purple_N_k3_pb{pb}_pr1_pp{pp}_partXX.bin
//     The current public runtime does not consume these partition files directly;
//     use geister_purple_tb_red2_repack.cpp to obtain runtime-ready single-file
//     tb_purple_N_*.bin outputs.

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <immintrin.h>
#include <zstd.h>

#ifdef _OPENMP
extern "C" int omp_get_max_threads(void);
extern "C" int omp_get_num_procs(void);
#endif

import tablebase_io;
import geister_core;
import geister_rank_triplet;

namespace fs = std::filesystem;

namespace {

// ============================================================
// Constants / geometry
// ============================================================

static constexpr std::uint64_t kBoardMask64 = bb_board;
static constexpr std::uint64_t kExitMask = (1ULL << POSITIONS::A1) | (1ULL << POSITIONS::F1);

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
static constexpr std::uint8_t kTBMaxDepth = 210;

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

[[nodiscard]] static inline int material_total(const PurpleMaterialKey& key) noexcept {
    return static_cast<int>(key.pb) + static_cast<int>(key.pr) + static_cast<int>(key.pp);
}

[[nodiscard]] static inline bool key_in_red2_domain(const PurpleMaterialKey& key) noexcept {
    return key.k == 3 && key.pr == 1 &&
        ((key.pb == 4 && key.pp == 4) || (key.pb == 3 && key.pp == 5) || (key.pb == 4 && key.pp == 5));
}

[[nodiscard]] static inline std::string material_tag_of(const PurpleMaterialKey& key) {
    if (key.pb == 4 && key.pr == 1 && key.pp == 4 && key.k == 3) return "p9a";
    if (key.pb == 3 && key.pr == 1 && key.pp == 5 && key.k == 3) return "p9b";
    if (key.pb == 4 && key.pr == 1 && key.pp == 5 && key.k == 3) return "p10";
    std::ostringstream oss;
    oss << "k" << int(key.k) << "_pb" << int(key.pb) << "_pr" << int(key.pr) << "_pp" << int(key.pp);
    return oss.str();
}

// ============================================================
// Logging helpers
// ============================================================

namespace tbutil {

using Clock = std::chrono::steady_clock;
static const Clock::time_point g_program_start = Clock::now();
static std::mutex g_log_mutex;

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
    std::lock_guard<std::mutex> lock(g_log_mutex);
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

} // namespace tbutil

// ============================================================
// Zstd streaming write/read
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

class ZstdOStreambuf final : public std::streambuf {
public:
    ZstdOStreambuf(std::ostream& sink, int level, int nbWorkers)
        : sink_(sink),
          cctx_(ZSTD_createCCtx()),
          in_buffer_(ZSTD_CStreamInSize()),
          out_buffer_(ZSTD_CStreamOutSize())
    {
        if (!cctx_) throw std::runtime_error("ZSTD_createCCtx failed");

        check_(ZSTD_CCtx_setParameter(cctx_, ZSTD_c_compressionLevel, level), "ZSTD_c_compressionLevel");
        check_(ZSTD_CCtx_setParameter(cctx_, ZSTD_c_checksumFlag, 1), "ZSTD_c_checksumFlag");
        if (nbWorkers > 0) {
            const size_t code = ZSTD_CCtx_setParameter(cctx_, ZSTD_c_nbWorkers, nbWorkers);
            if (ZSTD_isError(code)) {
                const std::string_view err = ZSTD_getErrorName(code);
                if (err.find("Unsupported parameter") == std::string_view::npos) {
                    check_(code, "ZSTD_c_nbWorkers");
                }
            }
        }
    }

    ZstdOStreambuf(const ZstdOStreambuf&) = delete;
    ZstdOStreambuf& operator=(const ZstdOStreambuf&) = delete;

    ~ZstdOStreambuf() override {
        try {
            if (!finished_) finish();
        }
        catch (...) {
        }
        ZSTD_freeCCtx(cctx_);
    }

    void finish() {
        if (finished_) return;
        flush_impl_(ZSTD_e_end);
        finished_ = true;
    }

protected:
    int_type overflow(int_type ch) override {
        if (finished_) return traits_type::eof();
        if (ch == traits_type::eof()) return traits_type::not_eof(ch);
        const char c = static_cast<char>(ch);
        const std::streamsize n = xsputn(&c, 1);
        return (n == 1) ? ch : traits_type::eof();
    }

    std::streamsize xsputn(const char* s, std::streamsize n) override {
        if (finished_ || n <= 0) return 0;

        std::streamsize total = 0;
        while (n > 0) {
            const size_t space = in_buffer_.size() - in_pos_;
            const size_t to_copy = static_cast<size_t>(std::min<std::streamsize>(n, static_cast<std::streamsize>(space)));

            std::memcpy(in_buffer_.data() + in_pos_, s, to_copy);
            in_pos_ += to_copy;
            s += to_copy;
            n -= static_cast<std::streamsize>(to_copy);
            total += static_cast<std::streamsize>(to_copy);

            if (in_pos_ == in_buffer_.size()) {
                consume_input_continue_();
            }
        }
        return total;
    }

    int sync() override {
        if (finished_) return 0;
        flush_impl_(ZSTD_e_flush);
        return 0;
    }

private:
    std::ostream& sink_;
    ZSTD_CCtx* cctx_ = nullptr;
    std::vector<char> in_buffer_;
    std::vector<char> out_buffer_;
    size_t in_pos_ = 0;
    bool finished_ = false;

    static void check_(size_t code, const char* what) {
        if (ZSTD_isError(code)) {
            throw std::runtime_error(std::string(what) + ": " + ZSTD_getErrorName(code));
        }
    }

    void write_out_(const void* data, size_t size) {
        sink_.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
        if (!sink_) throw std::runtime_error("ostream write failed");
    }

    void consume_input_continue_() {
        ZSTD_inBuffer input{ in_buffer_.data(), in_pos_, 0 };
        while (input.pos < input.size) {
            ZSTD_outBuffer output{ out_buffer_.data(), out_buffer_.size(), 0 };
            const size_t remaining = ZSTD_compressStream2(cctx_, &output, &input, ZSTD_e_continue);
            check_(remaining, "ZSTD_compressStream2(ZSTD_e_continue)");
            if (output.pos) write_out_(out_buffer_.data(), output.pos);
        }
        in_pos_ = 0;
    }

    void flush_impl_(ZSTD_EndDirective mode) {
        if (in_pos_ > 0) consume_input_continue_();
        ZSTD_inBuffer empty_input{ nullptr, 0, 0 };
        size_t remaining = 0;
        do {
            ZSTD_outBuffer output{ out_buffer_.data(), out_buffer_.size(), 0 };
            remaining = ZSTD_compressStream2(cctx_, &output, &empty_input, mode);
            check_(remaining, (mode == ZSTD_e_flush) ? "ZSTD_compressStream2(ZSTD_e_flush)" : "ZSTD_compressStream2(ZSTD_e_end)");
            if (output.pos) write_out_(out_buffer_.data(), output.pos);
        } while (remaining != 0);
    }
};

struct ZstdWriter {
    fs::path final_path;
    fs::path tmp_path;
    std::ofstream ofs;
    std::unique_ptr<ZstdOStreambuf> zbuf;
    std::unique_ptr<std::ostream> os;

    ZstdWriter(const fs::path& final_path_, int level, int workers)
        : final_path(final_path_), tmp_path(final_path_)
    {
        tmp_path += ".tmp";
        std::error_code ec;
        fs::create_directories(final_path.parent_path(), ec);
        ofs.open(tmp_path, std::ios::binary | std::ios::trunc);
        if (!ofs) throw std::runtime_error("failed to open for write: " + tmp_path.string());
        ofs.exceptions(std::ios::badbit);
        zbuf = std::make_unique<ZstdOStreambuf>(ofs, level, workers);
        os = std::make_unique<std::ostream>(zbuf.get());
        os->exceptions(std::ios::badbit);
    }

    void write_bytes(const std::uint8_t* data, size_t n) {
        os->write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(n));
        if (!(*os)) throw std::runtime_error("zstd ostream write failed");
    }

    void finish() {
        zbuf->finish();
        ofs.flush();
        ofs.close();
        atomic_rename_best_effort(tmp_path, final_path);
    }
};

struct ZstdReader {
    std::ifstream ifs;
    ZSTD_DCtx* dctx = nullptr;
    std::vector<char> in_buf;
    std::vector<char> out_buf;
    ZSTD_inBuffer input{ nullptr, 0, 0 };
    size_t out_pos = 0;
    size_t out_size = 0;
    bool done = false;

    explicit ZstdReader(const fs::path& src)
        : ifs(src, std::ios::binary),
          dctx(ZSTD_createDCtx()),
          in_buf(ZSTD_DStreamInSize()),
          out_buf(ZSTD_DStreamOutSize())
    {
        if (!ifs) throw std::runtime_error("failed to open for read: " + src.string());
        if (!dctx) throw std::runtime_error("ZSTD_createDCtx failed");
        input = { in_buf.data(), 0, 0 };
    }

    ~ZstdReader() {
        ZSTD_freeDCtx(dctx);
    }

    void refill_out_() {
        if (done) { out_pos = out_size = 0; return; }

        out_pos = 0;
        out_size = 0;
        while (out_size == 0) {
            if (input.pos == input.size) {
                ifs.read(in_buf.data(), static_cast<std::streamsize>(in_buf.size()));
                const std::streamsize got = ifs.gcount();
                if (got <= 0) input = { in_buf.data(), 0, 0 };
                else          input = { in_buf.data(), static_cast<size_t>(got), 0 };
            }

            ZSTD_outBuffer output{ out_buf.data(), out_buf.size(), 0 };
            const size_t ret = ZSTD_decompressStream(dctx, &output, &input);
            if (ZSTD_isError(ret)) throw std::runtime_error(std::string("ZSTD_decompressStream: ") + ZSTD_getErrorName(ret));

            out_size = output.pos;
            if (ret == 0) done = true;
            if (out_size == 0 && done) return;
        }
    }

    void read_exact(std::uint8_t* dst, size_t n) {
        size_t written = 0;
        while (written < n) {
            if (out_pos == out_size) {
                refill_out_();
                if (out_size == 0) throw std::runtime_error("zstd truncated (unexpected EOF)");
            }
            const size_t avail = out_size - out_pos;
            const size_t take = std::min(avail, n - written);
            std::memcpy(dst + written, out_buf.data() + out_pos, take);
            out_pos += take;
            written += take;
        }
    }
};

static inline void read_exact_zero(std::uint8_t* dst, size_t n) {
    std::memset(dst, 0, n);
}

[[nodiscard]] static std::vector<std::uint8_t> load_zstd_file_exact(const fs::path& fn, std::uint64_t expected_size) {
    std::vector<std::uint8_t> out(static_cast<size_t>(expected_size));
    ZstdReader zr(fn);
    zr.read_exact(out.data(), out.size());
    return out;
}

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
    std::array<std::uint64_t, 18> part_rem_mask36{};
};

static PartitionIndex18 build_partition_index18() {
    PartitionIndex18 pi{};
    pi.full_to_part.fill(-1);

    int part = 0;
    for (int full = 0; full < 36; ++full) {
        const std::uint64_t pat = 1ULL << full;
        const std::uint64_t bb = pdep36(pat);
        if ((bb & kLeftMask64) != 0) {
            assert(part < 18);
            pi.full_to_part[static_cast<size_t>(full)] = static_cast<int8_t>(part);
            pi.part_to_full[static_cast<size_t>(part)] = static_cast<std::uint8_t>(full);
            pi.part_red_bb64[static_cast<size_t>(part)] = bb;
            pi.part_red_pat36[static_cast<size_t>(part)] = pat;
            pi.part_rem_mask36[static_cast<size_t>(part)] = kBoardMask36 ^ pat;
            ++part;
        }
    }
    if (part != 18) throw std::runtime_error("left-half squares != 18");
    return pi;
}

struct MaterialSpec {
    const char* tag = "";
    PurpleMaterialKey key{};
    std::uint64_t entries_per_part = 0;
    std::uint64_t factor_normal_second = 0; // C(35-pb, pp)
    std::uint64_t factor_purple_second = 0; // C(35-pp, pb)
};

static MaterialSpec make_material(const char* tag, int pb, int pp) {
    MaterialSpec m{};
    m.tag = tag;
    m.key = PurpleMaterialKey{ kDefaultK, static_cast<std::uint8_t>(pb), kDefaultPr, static_cast<std::uint8_t>(pp) };
    m.factor_normal_second = comb::C[35 - pb][pp];
    m.factor_purple_second = comb::C[35 - pp][pb];
    m.entries_per_part = comb::C[35][pb] * m.factor_normal_second;
    assert(m.entries_per_part == comb::C[35][pp] * m.factor_purple_second);
    return m;
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

// ============================================================
// Immediate values (must match geister_purple_tb.cpp semantics)
// ============================================================

[[nodiscard]] static inline std::uint8_t normal_immediate_value(const perfect_information_geister& pos, std::uint8_t k) noexcept {
    if (pos.bb_player.bb_blue & kExitMask) return 1;
    if (pos.bb_player.bb_red == 0) return 1;
    if (pos.bb_opponent.bb_blue == 0) return 1;
    if (pos.bb_player.bb_blue == 0) return 2;
    if (k >= 4) return 2;
    return 0;
}

[[nodiscard]] static inline std::uint8_t purple_immediate_value(const perfect_information_geister& pos, std::uint8_t k) noexcept {
    if (pos.bb_player.bb_blue & kExitMask) return 1;
    if (pos.bb_opponent.bb_blue == 0) return 1;
    if (pos.bb_opponent.bb_red == 0) return 2;
    if (k >= 4) return 0; // would be win, not loss
    if (pos.bb_player.bb_blue == 0) return 2;
    return 0;
}

// ============================================================
// Partition child-part metadata (for cache retention)
// ============================================================

struct PartMeta {
    std::array<std::uint16_t, 5> normal_to_purple_child_parts{};
    std::uint8_t normal_to_purple_n = 0;
    std::uint16_t purple_to_normal_child_part = 0;
};

[[nodiscard]] static inline std::uint16_t canonical_part_after_swap_red_sq(
    const PartitionIndex18& pi,
    std::uint64_t red_sq_bb_current) noexcept
{
    std::uint64_t child_red = bit_reverse64(red_sq_bb_current);
    if ((child_red & kLeftMask64) == 0) child_red = mirror::mirror_lr_u64(child_red);
    const int full = full_index_of_single_bb(child_red);
    assert(full >= 0);
    const int part = pi.full_to_part[static_cast<size_t>(full)];
    assert(part >= 0);
    return static_cast<std::uint16_t>(part);
}

[[nodiscard]] static std::array<PartMeta, 18> build_part_meta(const PartitionIndex18& pi) {
    std::array<PartMeta, 18> meta{};
    for (int pid = 0; pid < 18; ++pid) {
        PartMeta pm{};
        const std::uint64_t red = pi.part_red_bb64[static_cast<size_t>(pid)];

        auto add_part = [&](std::uint16_t p) {
            for (std::uint8_t i = 0; i < pm.normal_to_purple_n; ++i) {
                if (pm.normal_to_purple_child_parts[static_cast<size_t>(i)] == p) return;
            }
            assert(pm.normal_to_purple_n < pm.normal_to_purple_child_parts.size());
            pm.normal_to_purple_child_parts[static_cast<size_t>(pm.normal_to_purple_n++)] = p;
        };

        const std::uint16_t unchanged = canonical_part_after_swap_red_sq(pi, red);
        pm.purple_to_normal_child_part = unchanged;
        add_part(unchanged);

        const int sq = std::countr_zero(red);
        for (int dir = 0; dir < 4; ++dir) {
            const int to = sq + DIFF_SQUARE[dir];
            if (to < 0 || to >= 64) continue;
            const std::uint64_t to_bb = 1ULL << to;
            if ((to_bb & kBoardMask64) == 0) continue;
            add_part(canonical_part_after_swap_red_sq(pi, to_bb));
        }

        meta[static_cast<size_t>(pid)] = pm;
    }
    return meta;
}

// ============================================================
// Filenames / iter dirs
// ============================================================

[[nodiscard]] static inline std::string iter_dir_name(int iter) {
    std::ostringstream oss;
    oss << "iter_" << std::setw(3) << std::setfill('0') << iter;
    return oss.str();
}

[[nodiscard]] static inline fs::path material_root_path(const fs::path& out_dir, const MaterialSpec& mat) {
    std::ostringstream oss;
    oss << "tb_purple_red2_k" << int(mat.key.k)
        << "_pb" << int(mat.key.pb)
        << "_pr" << int(mat.key.pr)
        << "_pp" << int(mat.key.pp);
    return out_dir / oss.str();
}

[[nodiscard]] static inline std::string runtime_part_bin_name(const PurpleMaterialKey& key, int part) {
    std::ostringstream oss;
    oss << "tb_purple_N_k" << int(key.k)
        << "_pb" << int(key.pb)
        << "_pr" << int(key.pr)
        << "_pp" << int(key.pp)
        << "_part" << std::setw(2) << std::setfill('0') << part
        << ".bin";
    return oss.str();
}

[[nodiscard]] static inline fs::path part_file_path(
    const fs::path& root,
    const MaterialSpec& mat,
    TurnKind turn,
    int iter,
    std::uint16_t part)
{
    std::ostringstream oss;
    oss << "tb_purple_" << (turn == TurnKind::NormalToMove ? 'N' : 'P')
        << "_k" << int(mat.key.k)
        << "_pb" << int(mat.key.pb)
        << "_pr" << int(mat.key.pr)
        << "_pp" << int(mat.key.pp)
        << "_part" << std::setw(2) << std::setfill('0') << int(part)
        << ".bin.zst";
    return root / iter_dir_name(iter) / oss.str();
}

[[nodiscard]] static inline fs::path complete_marker_path(const fs::path& iter_dir) {
    return iter_dir / ".complete";
}

static void mark_iter_complete(const fs::path& iter_dir) {
    std::ofstream ofs(complete_marker_path(iter_dir), std::ios::binary | std::ios::trunc);
    if (!ofs) throw std::runtime_error("failed to create complete marker: " + iter_dir.string());
}

[[nodiscard]] static int find_last_complete_iter(const fs::path& root) {
    int best = 0;
    if (!fs::exists(root)) return 0;
    for (const auto& ent : fs::directory_iterator(root)) {
        std::error_code ec;
        if (!ent.is_directory(ec)) continue;
        const std::string name = ent.path().filename().string();
        if (name.size() != 8 || name.rfind("iter_", 0) != 0) continue;
        int iter = 0;
        if (!tbutil::parse_int(std::string_view(name).substr(5), iter)) continue;
        if (!fs::exists(complete_marker_path(ent.path()))) continue;
        if (iter > best) best = iter;
    }
    return best;
}

static void cleanup_incomplete_iters(const fs::path& root, int last_complete) {
    if (!fs::exists(root)) return;
    for (const auto& ent : fs::directory_iterator(root)) {
        std::error_code ec;
        if (!ent.is_directory(ec)) continue;
        const std::string name = ent.path().filename().string();
        if (name.size() != 8 || name.rfind("iter_", 0) != 0) continue;
        int iter = 0;
        if (!tbutil::parse_int(std::string_view(name).substr(5), iter)) continue;
        if (iter <= last_complete && fs::exists(complete_marker_path(ent.path()))) continue;
        tbutil::log_line("[cleanup] remove stale iter dir: ", ent.path().string());
        std::error_code ec_rm;
        fs::remove_all(ent.path(), ec_rm);
        if (ec_rm) throw std::runtime_error("failed to remove_all: " + ent.path().string() + " : " + ec_rm.message());
    }
}

static void delete_iter_dir_if_exists(const fs::path& root, int iter) {
    if (iter <= 0) return;
    const fs::path dir = root / iter_dir_name(iter);
    if (!fs::exists(dir)) return;
    std::error_code ec;
    fs::remove_all(dir, ec);
    if (ec) throw std::runtime_error("failed to remove_all: " + dir.string() + " : " + ec.message());
}

// ============================================================
// Config / CLI
// ============================================================

struct Config {
    fs::path out_dir = "tb_purple_red2";
    fs::path dep_dir = ".";
    fs::path runtime_out_dir = ".";
    int max_depth = kTBMaxDepth;
    int zstd_level = 3;
    int zstd_workers = 8;
    std::uint64_t chunk_bytes = 1ULL << 20;
    int omp_chunk = 1024;
    bool keep_all_iters = false;
    bool export_runtime_normal_parts = false;
    bool self_test = false;
    std::optional<std::string> only_material{};
    int dep_p9b_iter = kTBMaxDepth;
};

static Config g_cfg{};

static void print_usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " [options]\n\n"
        << "Builds purple DTW tables for the red2 restricted domain:\n"
        << "  p9a = (k,pb,pr,pp) = (3,4,1,4)\n"
        << "  p9b = (k,pb,pr,pp) = (3,3,1,5)\n"
        << "  p10 = (k,pb,pr,pp) = (3,4,1,5)\n\n"
        << "Options:\n"
        << "  --out-dir DIR                    Iteration output root (default: tb_purple_red2)\n"
        << "  --dep-dir DIR                    Legacy <=8 purple TB directory (default: .)\n"
        << "  --runtime-out-dir DIR            Output dir for --export-runtime-normal-parts debug files (default: .)\n"
        << "  --max-depth N                    Max DTW depth to build (default: 210)\n"
        << "  --zstd-level N                   zstd compression level (default: 3)\n"
        << "  --zstd-workers N                 zstd nbWorkers (default: 8)\n"
        << "  --chunk-bytes N                  Streaming chunk size in bytes (default: 1048576)\n"
        << "  --omp-chunk N                    OpenMP schedule chunk (default: 1024)\n"
        << "  --only p9a|p9b|p10               Build only one material\n"
        << "  --dep-p9b-iter N                 Completed p9b iter used by p10 (default: max-depth)\n"
        << "  --export-runtime-normal-parts    Export final N-turn raw partition bins (debug only; public runtime uses repacked single-file bins)\n"
        << "  --keep-all-iters                 Keep all completed iter dirs (default: keep only prev/current)\n"
        << "  --self-test                      Run partition rank/unrank self-tests and exit\n"
        << "  --help, -h                       Show this help\n";
}

static void parse_args_or_throw(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        const std::string_view a(argv[i]);
        if (a == "--out-dir" && i + 1 < argc) {
            g_cfg.out_dir = argv[++i];
        } else if (a == "--dep-dir" && i + 1 < argc) {
            g_cfg.dep_dir = argv[++i];
        } else if (a == "--runtime-out-dir" && i + 1 < argc) {
            g_cfg.runtime_out_dir = argv[++i];
        } else if (a == "--max-depth" && i + 1 < argc) {
            if (!tbutil::parse_int(argv[++i], g_cfg.max_depth)) throw std::runtime_error("bad --max-depth");
        } else if (a == "--zstd-level" && i + 1 < argc) {
            if (!tbutil::parse_int(argv[++i], g_cfg.zstd_level)) throw std::runtime_error("bad --zstd-level");
        } else if (a == "--zstd-workers" && i + 1 < argc) {
            if (!tbutil::parse_int(argv[++i], g_cfg.zstd_workers)) throw std::runtime_error("bad --zstd-workers");
        } else if (a == "--chunk-bytes" && i + 1 < argc) {
            if (!tbutil::parse_u64(argv[++i], g_cfg.chunk_bytes)) throw std::runtime_error("bad --chunk-bytes");
        } else if (a == "--omp-chunk" && i + 1 < argc) {
            if (!tbutil::parse_int(argv[++i], g_cfg.omp_chunk)) throw std::runtime_error("bad --omp-chunk");
        } else if (a == "--only" && i + 1 < argc) {
            g_cfg.only_material = std::string(argv[++i]);
            if (*g_cfg.only_material != "p9a" && *g_cfg.only_material != "p9b" && *g_cfg.only_material != "p10") {
                throw std::runtime_error("--only must be one of: p9a, p9b, p10");
            }
        } else if (a == "--dep-p9b-iter" && i + 1 < argc) {
            if (!tbutil::parse_int(argv[++i], g_cfg.dep_p9b_iter)) throw std::runtime_error("bad --dep-p9b-iter");
        } else if (a == "--export-runtime-normal-parts") {
            g_cfg.export_runtime_normal_parts = true;
        } else if (a == "--keep-all-iters") {
            g_cfg.keep_all_iters = true;
        } else if (a == "--self-test") {
            g_cfg.self_test = true;
        } else if (a == "--help" || a == "-h") {
            print_usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error(std::string("unknown argument: ") + std::string(a));
        }
    }

    if (g_cfg.max_depth < 1 || g_cfg.max_depth > 254) throw std::runtime_error("--max-depth out of range (1..254)");
    if (g_cfg.dep_p9b_iter < 1 || g_cfg.dep_p9b_iter > 254) throw std::runtime_error("--dep-p9b-iter out of range (1..254)");
    if (g_cfg.chunk_bytes == 0) throw std::runtime_error("--chunk-bytes must be > 0");
    if (g_cfg.omp_chunk <= 0) g_cfg.omp_chunk = 1;
}

// ============================================================
// Bitsets / caches
// ============================================================

struct DenseBitset {
    std::uint64_t nbits = 0;
    std::uint64_t nwords = 0;
    std::unique_ptr<std::uint64_t[]> words{};

    DenseBitset() = default;
    explicit DenseBitset(std::uint64_t nbits_) { init(nbits_); }

    void init(std::uint64_t nbits_) {
        nbits = nbits_;
        nwords = (nbits + 63) / 64;
        words = std::make_unique<std::uint64_t[]>(static_cast<size_t>(nwords));
        std::memset(words.get(), 0, static_cast<size_t>(nwords) * sizeof(std::uint64_t));
    }

    [[nodiscard]] inline bool test(std::uint64_t idx) const noexcept {
        const std::uint64_t word = idx >> 6;
        const std::uint64_t bit = idx & 63;
        return (words[static_cast<size_t>(word)] >> bit) & 1ULL;
    }

    inline void set(std::uint64_t idx) noexcept {
        const std::uint64_t word = idx >> 6;
        const std::uint64_t bit = idx & 63;
        words[static_cast<size_t>(word)] |= (1ULL << bit);
    }
};

struct PartBits {
    DenseBitset known;
    DenseBitset odd;
    DenseBitset layer_prev;

    explicit PartBits(std::uint64_t entries)
        : known(entries), odd(entries), layer_prev(entries) {}
};

static std::shared_ptr<const PartBits> load_part_bits_from_zstd(
    const fs::path& fn,
    std::uint64_t entries,
    int prev_iter)
{
    auto bits = std::make_shared<PartBits>(entries);
    std::vector<std::uint8_t> buf(static_cast<size_t>(g_cfg.chunk_bytes));
    ZstdReader zr(fn);

    std::uint64_t remaining = entries;
    std::uint64_t base = 0;
    while (remaining != 0) {
        const std::uint64_t take = std::min<std::uint64_t>(remaining, g_cfg.chunk_bytes);
        zr.read_exact(buf.data(), static_cast<size_t>(take));
        for (std::uint64_t i = 0; i < take; ++i) {
            const std::uint8_t v = buf[static_cast<size_t>(i)];
            if (v != 0) {
                bits->known.set(base + i);
                if (v & 1U) bits->odd.set(base + i);
            }
            if (v == static_cast<std::uint8_t>(prev_iter)) {
                bits->layer_prev.set(base + i);
            }
        }
        base += take;
        remaining -= take;
    }
    return bits;
}

class SameIterPartBitsCache {
public:
    SameIterPartBitsCache() = default;
    SameIterPartBitsCache(fs::path root, MaterialSpec mat, TurnKind turn)
        : root_(std::move(root)), mat_(std::move(mat)), turn_(turn) {}

    void reset(fs::path root, MaterialSpec mat, TurnKind turn) {
        root_ = std::move(root);
        mat_ = std::move(mat);
        turn_ = turn;
        set_iter(-1);
    }

    void set_iter(int it) {
        if (iter_ == it) return;
        iter_ = it;
        tables_.clear();
        bytes_loaded_ = 0;
    }

    [[nodiscard]] int iter() const noexcept { return iter_; }
    [[nodiscard]] std::uint64_t bytes_loaded() const noexcept { return bytes_loaded_; }
    [[nodiscard]] std::size_t cached_parts() const noexcept { return tables_.size(); }

    [[nodiscard]] std::shared_ptr<const PartBits> get(std::uint16_t part) {
        if (iter_ <= 0) return {};
        auto it = tables_.find(part);
        if (it != tables_.end()) return it->second;

        const fs::path fn = part_file_path(root_, mat_, turn_, iter_, part);
        if (!fs::exists(fn)) throw std::runtime_error("missing iter partition: " + fn.string());
        auto sp = load_part_bits_from_zstd(fn, mat_.entries_per_part, iter_);
        const std::uint64_t approx = ((mat_.entries_per_part + 63) / 64) * sizeof(std::uint64_t) * 3ULL;
        bytes_loaded_ += approx;
        tables_.emplace(part, sp);
        return sp;
    }

    void retain_only(const std::uint16_t* keep_parts, int keep_n) {
        if (iter_ <= 0) return;
        for (auto it = tables_.begin(); it != tables_.end();) {
            bool keep = false;
            for (int i = 0; i < keep_n; ++i) {
                if (it->first == keep_parts[i]) { keep = true; break; }
            }
            if (!keep) {
                const std::uint64_t approx = ((mat_.entries_per_part + 63) / 64) * sizeof(std::uint64_t) * 3ULL;
                bytes_loaded_ -= approx;
                it = tables_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    fs::path root_{};
    MaterialSpec mat_{};
    TurnKind turn_ = TurnKind::NormalToMove;
    int iter_ = -1;
    std::unordered_map<std::uint16_t, std::shared_ptr<const PartBits>> tables_{};
    std::uint64_t bytes_loaded_ = 0;
};

class CompletedPartByteCache {
public:
    CompletedPartByteCache() = default;
    CompletedPartByteCache(fs::path root, MaterialSpec mat, TurnKind turn, int iter)
        : root_(std::move(root)), mat_(std::move(mat)), turn_(turn), iter_(iter) {}

    [[nodiscard]] std::shared_ptr<const std::vector<std::uint8_t>> get(std::uint16_t part) {
        auto it = tables_.find(part);
        if (it != tables_.end()) return it->second;

        const fs::path fn = part_file_path(root_, mat_, turn_, iter_, part);
        if (!fs::exists(fn)) throw std::runtime_error("missing completed dependency partition: " + fn.string());
        auto vec = load_zstd_file_exact(fn, mat_.entries_per_part);
        auto sp = std::make_shared<std::vector<std::uint8_t>>(std::move(vec));
        bytes_loaded_ += static_cast<std::uint64_t>(sp->size());
        tables_.emplace(part, sp);
        return sp;
    }

    void retain_only(const std::uint16_t* keep_parts, int keep_n) {
        for (auto it = tables_.begin(); it != tables_.end();) {
            bool keep = false;
            for (int i = 0; i < keep_n; ++i) {
                if (it->first == keep_parts[i]) { keep = true; break; }
            }
            if (!keep) {
                bytes_loaded_ -= static_cast<std::uint64_t>(it->second->size());
                it = tables_.erase(it);
            } else {
                ++it;
            }
        }
    }

    [[nodiscard]] std::uint64_t bytes_loaded() const noexcept { return bytes_loaded_; }
    [[nodiscard]] std::size_t cached_parts() const noexcept { return tables_.size(); }

private:
    fs::path root_{};
    MaterialSpec mat_{};
    TurnKind turn_ = TurnKind::NormalToMove;
    int iter_ = 0;
    std::unordered_map<std::uint16_t, std::shared_ptr<const std::vector<std::uint8_t>>> tables_{};
    std::uint64_t bytes_loaded_ = 0;
};

// ============================================================
// Legacy <=8 full purple dependencies (Normal-to-move only)
// ============================================================

enum class LegacyDepKind : std::uint8_t {
    none = 0,
    raw_mmap = 1,
    in_memory = 2,
};

struct LegacyPurpleNormalDep {
    PurpleMaterialKey key{};
    std::uint64_t entries = 0;
    LegacyDepKind kind = LegacyDepKind::none;
    tbio::mmap::mapped_file mf{};
    std::vector<std::uint8_t> vec{};

    [[nodiscard]] inline std::uint8_t get(std::uint64_t idx) const noexcept {
        switch (kind) {
        case LegacyDepKind::raw_mmap:
            return mf.u8span()[static_cast<size_t>(idx)];
        case LegacyDepKind::in_memory:
            return vec[static_cast<size_t>(idx)];
        default:
            return 0;
        }
    }
};

[[nodiscard]] static inline std::string make_legacy_purple_txt_name(const PurpleMaterialKey& key, TurnKind turn) {
    std::ostringstream oss;
    oss << "tb_purple_" << (turn == TurnKind::NormalToMove ? 'N' : 'P')
        << "_k" << int(key.k)
        << "_pb" << int(key.pb)
        << "_pr" << int(key.pr)
        << "_pp" << int(key.pp)
        << ".txt";
    return oss.str();
}

[[nodiscard]] static inline std::string make_legacy_purple_bin_name(const PurpleMaterialKey& key, TurnKind turn) {
    fs::path p = make_legacy_purple_txt_name(key, turn);
    p.replace_extension(".bin");
    return p.string();
}

[[nodiscard]] static LegacyPurpleNormalDep load_legacy_normal_dep_or_throw(const fs::path& dir, const PurpleMaterialKey& key) {
    LegacyPurpleNormalDep dep{};
    dep.key = key;
    dep.entries = states_for_counts(key.pb, key.pr, key.pp);

    const fs::path fbin = dir / make_legacy_purple_bin_name(key, TurnKind::NormalToMove);
    if (tbio::tablebase_bin_looks_valid(fbin, dep.entries)) {
        dep.mf = tbio::mmap::open_tablebase_bin_readonly(fbin, dep.entries);
        dep.mf.advise(tbio::mmap::advice::random);
        dep.kind = LegacyDepKind::raw_mmap;
        tbutil::log_line("[dep] mmap legacy purple N-turn bin: ", fbin.string(), " entries=", dep.entries);
        return dep;
    }

    const fs::path ftxt = dir / make_legacy_purple_txt_name(key, TurnKind::NormalToMove);
    if (tbio::tablebase_file_looks_valid(ftxt, dep.entries)) {
        dep.vec = tbio::load_tablebase_hex_lines_streaming(ftxt, dep.entries, kTBMaxDepth);
        dep.kind = LegacyDepKind::in_memory;
        tbutil::log_line("[dep] load legacy purple N-turn txt: ", ftxt.string(), " entries=", dep.entries);
        return dep;
    }

    throw std::runtime_error("missing legacy <=8 purple dependency for key: " + make_legacy_purple_bin_name(key, TurnKind::NormalToMove));
}

// ============================================================
// Small child-value helpers
// ============================================================

[[nodiscard]] static inline bool is_win_value(std::uint8_t v) noexcept { return v != 0 && (v & 1U); }
[[nodiscard]] static inline bool is_loss_value(std::uint8_t v) noexcept { return v != 0 && ((v & 1U) == 0); }

struct PrevBitsViews {
    std::array<const PartBits*, 18> part{};
    PrevBitsViews() { part.fill(nullptr); }
};

[[nodiscard]] static PrevBitsViews make_prev_bits_views(
    SameIterPartBitsCache& cache,
    const std::uint16_t* parts,
    int n)
{
    PrevBitsViews out{};
    for (int i = 0; i < n; ++i) {
        const std::uint16_t part = parts[i];
        out.part[static_cast<size_t>(part)] = cache.get(part).get();
    }
    return out;
}

// ============================================================
// Streaming partition builders
// ============================================================

static void build_next_partition_normal_stream(
    const PartitionIndex18& pi,
    const MaterialSpec& mat,
    int depth,
    std::uint16_t part,
    int prev_iter,
    const PrevBitsViews& prev_p_views,
    const fs::path& root)
{
    const fs::path out_fn = part_file_path(root, mat, TurnKind::NormalToMove, depth, part);
    ZstdWriter zw(out_fn, g_cfg.zstd_level, g_cfg.zstd_workers);

    std::unique_ptr<ZstdReader> zr;
    if (prev_iter > 0) {
        const fs::path in_fn = part_file_path(root, mat, TurnKind::NormalToMove, prev_iter, part);
        zr = std::make_unique<ZstdReader>(in_fn);
    }

    std::vector<std::uint8_t> cur_buf(static_cast<size_t>(g_cfg.chunk_bytes));
    std::vector<std::uint8_t> next_buf(static_cast<size_t>(g_cfg.chunk_bytes));

    const bool odd_depth = (depth & 1) != 0;

    std::uint64_t base = 0;
    while (base < mat.entries_per_part) {
        const std::uint64_t take = std::min<std::uint64_t>(mat.entries_per_part - base, g_cfg.chunk_bytes);
        if (zr) zr->read_exact(cur_buf.data(), static_cast<size_t>(take));
        else    read_exact_zero(cur_buf.data(), static_cast<size_t>(take));

#ifdef _OPENMP
        #pragma omp parallel for schedule(dynamic, 1024)
#endif
        for (std::int64_t i = 0; i < static_cast<std::int64_t>(take); ++i) {
            const std::uint8_t cur_v = cur_buf[static_cast<size_t>(i)];
            if (cur_v != 0) {
                next_buf[static_cast<size_t>(i)] = cur_v;
                continue;
            }

            const std::uint64_t idx = base + static_cast<std::uint64_t>(i);
            perfect_information_geister pos = unrank_partitioned_normal(pi, mat, part, idx);

            const std::uint8_t imm = normal_immediate_value(pos, mat.key.k);
            if (imm != 0) {
                next_buf[static_cast<size_t>(i)] = imm;
                continue;
            }

            std::array<move, 32> moves{};
            const int n = pos.bb_player.gen_moves(0ULL, pos.bb_opponent.bb_blue, moves);
            assert(n > 0);

            if (odd_depth) {
                bool found = false;
                for (int mi = 0; mi < n; ++mi) {
                    const move m = moves[mi];
                    if (m.if_capture_blue()) {
                        // k=3 -> capturing any purple piece means k=4: immediate loss for current player.
                        continue;
                    }

                    perfect_information_geister nxt = pos;
                    perfect_information_geister::do_move(m, nxt);
                    const RankResult rr = rank_partitioned_purple(pi, mat, nxt.bb_player.bb_blue, nxt.bb_opponent.bb_blue, nxt.bb_opponent.bb_red);
                    const PartBits* const bits = prev_p_views.part[static_cast<size_t>(rr.part)];
                    if (bits && bits->layer_prev.test(rr.idx)) {
                        // prev_iter = depth-1 is even here, so exact child is a loss for the child side.
                        found = true;
                        break;
                    }
                }
                next_buf[static_cast<size_t>(i)] = found ? static_cast<std::uint8_t>(depth) : 0;
            }
            else {
                bool forced = true;
                bool has_prev = false;
                bool exact_odd_gt_prev = false;
                for (int mi = 0; mi < n; ++mi) {
                    const move m = moves[mi];
                    if (m.if_capture_blue()) {
                        // child exact value = 1 (opponent immediate win). depth==2 can use it as exact(prev).
                        if (depth == 2) has_prev = true;
                        continue;
                    }

                    perfect_information_geister nxt = pos;
                    perfect_information_geister::do_move(m, nxt);
                    const RankResult rr = rank_partitioned_purple(pi, mat, nxt.bb_player.bb_blue, nxt.bb_opponent.bb_blue, nxt.bb_opponent.bb_red);
                    const PartBits* const bits = prev_p_views.part[static_cast<size_t>(rr.part)];
                    if (!bits || !bits->known.test(rr.idx)) {
                        forced = false;
                        break;
                    }
                    if (!bits->odd.test(rr.idx)) {
                        forced = false;
                        break;
                    }
                    if (bits->layer_prev.test(rr.idx)) has_prev = true;
                }
                next_buf[static_cast<size_t>(i)] = (forced && has_prev && !exact_odd_gt_prev) ? static_cast<std::uint8_t>(depth) : 0;
            }
        }

        zw.write_bytes(next_buf.data(), static_cast<size_t>(take));
        base += take;
    }

    zw.finish();
}

static void build_next_partition_p9_stream(
    const PartitionIndex18& pi,
    const MaterialSpec& mat,
    int depth,
    std::uint16_t part,
    int prev_iter,
    const PrevBitsViews& prev_n_views,
    const LegacyPurpleNormalDep& lower_dep,
    const fs::path& root)
{
    const fs::path out_fn = part_file_path(root, mat, TurnKind::PurpleToMove, depth, part);
    ZstdWriter zw(out_fn, g_cfg.zstd_level, g_cfg.zstd_workers);

    std::unique_ptr<ZstdReader> zr;
    if (prev_iter > 0) {
        const fs::path in_fn = part_file_path(root, mat, TurnKind::PurpleToMove, prev_iter, part);
        zr = std::make_unique<ZstdReader>(in_fn);
    }

    std::vector<std::uint8_t> cur_buf(static_cast<size_t>(g_cfg.chunk_bytes));
    std::vector<std::uint8_t> next_buf(static_cast<size_t>(g_cfg.chunk_bytes));

    const bool odd_depth = (depth & 1) != 0;

    std::uint64_t base = 0;
    while (base < mat.entries_per_part) {
        const std::uint64_t take = std::min<std::uint64_t>(mat.entries_per_part - base, g_cfg.chunk_bytes);
        if (zr) zr->read_exact(cur_buf.data(), static_cast<size_t>(take));
        else    read_exact_zero(cur_buf.data(), static_cast<size_t>(take));

#ifdef _OPENMP
        #pragma omp parallel for schedule(dynamic, 1024)
#endif
        for (std::int64_t i = 0; i < static_cast<std::int64_t>(take); ++i) {
            const std::uint8_t cur_v = cur_buf[static_cast<size_t>(i)];
            if (cur_v != 0) {
                next_buf[static_cast<size_t>(i)] = cur_v;
                continue;
            }

            const std::uint64_t idx = base + static_cast<std::uint64_t>(i);
            perfect_information_geister pos = unrank_partitioned_purple(pi, mat, part, idx);

            const std::uint8_t imm = purple_immediate_value(pos, mat.key.k);
            if (imm != 0) {
                next_buf[static_cast<size_t>(i)] = imm;
                continue;
            }

            std::array<move, 32> moves{};
            const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
            assert(n > 0);

            if (odd_depth) {
                bool found = false;
                for (int mi = 0; mi < n; ++mi) {
                    const move m = moves[mi];
                    if (m.if_capture_red()) {
                        // pr=1 -> child exact value is 1 for Normal.
                        continue;
                    }
                    if (m.if_capture_blue()) {
                        perfect_information_geister nxt = pos;
                        perfect_information_geister::do_move(m, nxt);
                        const std::uint64_t cidx = rank_legacy_normal_turn(nxt);
                        const std::uint8_t childv = lower_dep.get(cidx);
                        if (childv == static_cast<std::uint8_t>(depth - 1) && is_loss_value(childv)) {
                            found = true;
                            break;
                        }
                        continue;
                    }

                    perfect_information_geister nxt = pos;
                    perfect_information_geister::do_move(m, nxt);
                    const RankResult rr = rank_partitioned_normal(pi, mat, nxt.bb_player.bb_blue, nxt.bb_player.bb_red, nxt.bb_opponent.bb_blue);
                    const PartBits* const bits = prev_n_views.part[static_cast<size_t>(rr.part)];
                    if (bits && bits->layer_prev.test(rr.idx)) {
                        found = true;
                        break;
                    }
                }
                next_buf[static_cast<size_t>(i)] = found ? static_cast<std::uint8_t>(depth) : 0;
            }
            else {
                bool forced = true;
                bool has_prev = false;
                bool exact_odd_gt_prev = false;
                for (int mi = 0; mi < n; ++mi) {
                    const move m = moves[mi];
                    if (m.if_capture_red()) {
                        // child exact value = 1 (Normal immediate win)
                        if (depth == 2) has_prev = true;
                        continue;
                    }
                    if (m.if_capture_blue()) {
                        perfect_information_geister nxt = pos;
                        perfect_information_geister::do_move(m, nxt);
                        const std::uint64_t cidx = rank_legacy_normal_turn(nxt);
                        const std::uint8_t childv = lower_dep.get(cidx);
                        if (childv == 0 || !is_win_value(childv)) {
                            forced = false;
                            break;
                        }
                        if (childv == static_cast<std::uint8_t>(depth - 1)) has_prev = true;
                        else if (childv > static_cast<std::uint8_t>(depth - 1)) exact_odd_gt_prev = true;
                        continue;
                    }

                    perfect_information_geister nxt = pos;
                    perfect_information_geister::do_move(m, nxt);
                    const RankResult rr = rank_partitioned_normal(pi, mat, nxt.bb_player.bb_blue, nxt.bb_player.bb_red, nxt.bb_opponent.bb_blue);
                    const PartBits* const bits = prev_n_views.part[static_cast<size_t>(rr.part)];
                    if (!bits || !bits->known.test(rr.idx)) {
                        forced = false;
                        break;
                    }
                    if (!bits->odd.test(rr.idx)) {
                        forced = false;
                        break;
                    }
                    if (bits->layer_prev.test(rr.idx)) has_prev = true;
                }
                next_buf[static_cast<size_t>(i)] = (forced && has_prev && !exact_odd_gt_prev) ? static_cast<std::uint8_t>(depth) : 0;
            }
        }

        zw.write_bytes(next_buf.data(), static_cast<size_t>(take));
        base += take;
    }

    zw.finish();
}

static void build_next_partition_p10_stream(
    const PartitionIndex18& pi,
    const MaterialSpec& mat10,
    const MaterialSpec& dep9b_mat,
    int depth,
    std::uint16_t part,
    int prev_iter,
    const PrevBitsViews& prev_n_views,
    const std::vector<std::uint8_t>* dep9b_part,
    std::uint16_t dep9b_part_id,
    const fs::path& root)
{
    const fs::path out_fn = part_file_path(root, mat10, TurnKind::PurpleToMove, depth, part);
    ZstdWriter zw(out_fn, g_cfg.zstd_level, g_cfg.zstd_workers);

    std::unique_ptr<ZstdReader> zr;
    if (prev_iter > 0) {
        const fs::path in_fn = part_file_path(root, mat10, TurnKind::PurpleToMove, prev_iter, part);
        zr = std::make_unique<ZstdReader>(in_fn);
    }

    std::vector<std::uint8_t> cur_buf(static_cast<size_t>(g_cfg.chunk_bytes));
    std::vector<std::uint8_t> next_buf(static_cast<size_t>(g_cfg.chunk_bytes));

    const bool odd_depth = (depth & 1) != 0;

    std::uint64_t base = 0;
    while (base < mat10.entries_per_part) {
        const std::uint64_t take = std::min<std::uint64_t>(mat10.entries_per_part - base, g_cfg.chunk_bytes);
        if (zr) zr->read_exact(cur_buf.data(), static_cast<size_t>(take));
        else    read_exact_zero(cur_buf.data(), static_cast<size_t>(take));

#ifdef _OPENMP
        #pragma omp parallel for schedule(dynamic, 1024)
#endif
        for (std::int64_t i = 0; i < static_cast<std::int64_t>(take); ++i) {
            const std::uint8_t cur_v = cur_buf[static_cast<size_t>(i)];
            if (cur_v != 0) {
                next_buf[static_cast<size_t>(i)] = cur_v;
                continue;
            }

            const std::uint64_t idx = base + static_cast<std::uint64_t>(i);
            perfect_information_geister pos = unrank_partitioned_purple(pi, mat10, part, idx);

            const std::uint8_t imm = purple_immediate_value(pos, mat10.key.k);
            if (imm != 0) {
                next_buf[static_cast<size_t>(i)] = imm;
                continue;
            }

            std::array<move, 32> moves{};
            const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
            assert(n > 0);

            if (odd_depth) {
                bool found = false;
                for (int mi = 0; mi < n; ++mi) {
                    const move m = moves[mi];
                    if (m.if_capture_red()) {
                        continue;
                    }
                    if (m.if_capture_blue()) {
                        assert(dep9b_part != nullptr);
                        perfect_information_geister nxt = pos;
                        perfect_information_geister::do_move(m, nxt);
                        const RankResult rr = rank_partitioned_normal(pi, dep9b_mat, nxt.bb_player.bb_blue, nxt.bb_player.bb_red, nxt.bb_opponent.bb_blue);
                        assert(rr.part == dep9b_part_id);
                        const std::uint8_t childv = (*dep9b_part)[static_cast<size_t>(rr.idx)];
                        if (childv == static_cast<std::uint8_t>(depth - 1) && is_loss_value(childv)) {
                            found = true;
                            break;
                        }
                        continue;
                    }

                    perfect_information_geister nxt = pos;
                    perfect_information_geister::do_move(m, nxt);
                    const RankResult rr = rank_partitioned_normal(pi, mat10, nxt.bb_player.bb_blue, nxt.bb_player.bb_red, nxt.bb_opponent.bb_blue);
                    const PartBits* const bits = prev_n_views.part[static_cast<size_t>(rr.part)];
                    if (bits && bits->layer_prev.test(rr.idx)) {
                        found = true;
                        break;
                    }
                }
                next_buf[static_cast<size_t>(i)] = found ? static_cast<std::uint8_t>(depth) : 0;
            }
            else {
                bool forced = true;
                bool has_prev = false;
                bool exact_odd_gt_prev = false;
                for (int mi = 0; mi < n; ++mi) {
                    const move m = moves[mi];
                    if (m.if_capture_red()) {
                        if (depth == 2) has_prev = true;
                        continue;
                    }
                    if (m.if_capture_blue()) {
                        assert(dep9b_part != nullptr);
                        perfect_information_geister nxt = pos;
                        perfect_information_geister::do_move(m, nxt);
                        const RankResult rr = rank_partitioned_normal(pi, dep9b_mat, nxt.bb_player.bb_blue, nxt.bb_player.bb_red, nxt.bb_opponent.bb_blue);
                        assert(rr.part == dep9b_part_id);
                        const std::uint8_t childv = (*dep9b_part)[static_cast<size_t>(rr.idx)];
                        if (childv == 0 || !is_win_value(childv)) {
                            forced = false;
                            break;
                        }
                        if (childv == static_cast<std::uint8_t>(depth - 1)) has_prev = true;
                        else if (childv > static_cast<std::uint8_t>(depth - 1)) exact_odd_gt_prev = true;
                        continue;
                    }

                    perfect_information_geister nxt = pos;
                    perfect_information_geister::do_move(m, nxt);
                    const RankResult rr = rank_partitioned_normal(pi, mat10, nxt.bb_player.bb_blue, nxt.bb_player.bb_red, nxt.bb_opponent.bb_blue);
                    const PartBits* const bits = prev_n_views.part[static_cast<size_t>(rr.part)];
                    if (!bits || !bits->known.test(rr.idx)) {
                        forced = false;
                        break;
                    }
                    if (!bits->odd.test(rr.idx)) {
                        forced = false;
                        break;
                    }
                    if (bits->layer_prev.test(rr.idx)) has_prev = true;
                }
                next_buf[static_cast<size_t>(i)] = (forced && has_prev && !exact_odd_gt_prev) ? static_cast<std::uint8_t>(depth) : 0;
            }
        }

        zw.write_bytes(next_buf.data(), static_cast<size_t>(take));
        base += take;
    }

    zw.finish();
}

// ============================================================
// Material build loops
// ============================================================

static void export_final_runtime_normal_parts(const fs::path& root, const MaterialSpec& mat, int final_iter) {
    fs::create_directories(g_cfg.runtime_out_dir);
    tbutil::log_line("[export] runtime normal partitions: ", mat.tag, " iter=", final_iter,
                     " -> ", g_cfg.runtime_out_dir.string());

    std::vector<std::uint8_t> buf(static_cast<size_t>(g_cfg.chunk_bytes));
    for (int pid = 0; pid < 18; ++pid) {
        const fs::path in_fn = part_file_path(root, mat, TurnKind::NormalToMove, final_iter, static_cast<std::uint16_t>(pid));
        if (!fs::exists(in_fn)) throw std::runtime_error("missing final normal partition: " + in_fn.string());

        const fs::path out_fn = g_cfg.runtime_out_dir / runtime_part_bin_name(mat.key, pid);
        const fs::path tmp_fn = out_fn.string() + ".tmp";

        ZstdReader zr(in_fn);
        std::ofstream ofs(tmp_fn, std::ios::binary | std::ios::trunc);
        if (!ofs) throw std::runtime_error("failed to open runtime export tmp: " + tmp_fn.string());

        std::uint64_t remaining = mat.entries_per_part;
        while (remaining != 0) {
            const std::uint64_t take = std::min<std::uint64_t>(remaining, g_cfg.chunk_bytes);
            zr.read_exact(buf.data(), static_cast<size_t>(take));
            ofs.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(take));
            if (!ofs) throw std::runtime_error("runtime export write failed: " + tmp_fn.string());
            remaining -= take;
        }
        ofs.flush();
        ofs.close();
        atomic_rename_best_effort(tmp_fn, out_fn);
    }
}

static void build_material_p9(
    const PartitionIndex18& pi,
    const std::array<PartMeta, 18>& pm,
    const MaterialSpec& mat,
    const LegacyPurpleNormalDep& lower_dep)
{
    const fs::path root = material_root_path(g_cfg.out_dir, mat);
    fs::create_directories(root);

    const int last_complete = find_last_complete_iter(root);
    cleanup_incomplete_iters(root, last_complete);

    int start_iter = last_complete + 1;
    if (start_iter < 1) start_iter = 1;
    if (start_iter > g_cfg.max_depth) {
        tbutil::log_line("[", mat.tag, "] already complete up to max_depth=", g_cfg.max_depth);
        if (g_cfg.export_runtime_normal_parts) export_final_runtime_normal_parts(root, mat, g_cfg.max_depth);
        return;
    }

    tbutil::log_line("[", mat.tag, "] resume last_complete=", last_complete,
                     " start_iter=", start_iter,
                     " root=", root.string(),
                     " entries/part=", mat.entries_per_part,
                     " total_states=", mat.entries_per_part * 18ULL);

    SameIterPartBitsCache prev_n_cache(root, mat, TurnKind::NormalToMove);
    SameIterPartBitsCache prev_p_cache(root, mat, TurnKind::PurpleToMove);

    for (int depth = start_iter; depth <= g_cfg.max_depth; ++depth) {
        const auto t_iter0 = tbutil::Clock::now();
        const int prev = depth - 1;
        prev_n_cache.set_iter(prev);
        prev_p_cache.set_iter(prev);

        tbutil::log_line("[", mat.tag, "] begin depth=", depth);
        const fs::path out_iter_dir = root / iter_dir_name(depth);
        if (fs::exists(out_iter_dir)) {
            std::error_code ec;
            fs::remove_all(out_iter_dir, ec);
            if (ec) throw std::runtime_error("failed to remove partial iter dir: " + out_iter_dir.string());
        }
        fs::create_directories(out_iter_dir);

        for (int pid = 0; pid < 18; ++pid) {
            const auto t_part0 = tbutil::Clock::now();
            const auto& meta = pm[static_cast<size_t>(pid)];
            const std::uint16_t part = static_cast<std::uint16_t>(pid);

            if (pid % 3 == 0) {
                tbutil::log_line("[", mat.tag, "] depth=", depth, " part=", pid,
                                 " prevN(parts=", prev_n_cache.cached_parts(), ",bits~", prev_n_cache.bytes_loaded(), ")",
                                 " prevP(parts=", prev_p_cache.cached_parts(), ",bits~", prev_p_cache.bytes_loaded(), ")");
            }

            const PrevBitsViews prev_p_views = make_prev_bits_views(prev_p_cache, meta.normal_to_purple_child_parts.data(), meta.normal_to_purple_n);
            build_next_partition_normal_stream(pi, mat, depth, part, prev, prev_p_views, root);

            const PrevBitsViews prev_n_views = make_prev_bits_views(prev_n_cache, &meta.purple_to_normal_child_part, 1);
            build_next_partition_p9_stream(pi, mat, depth, part, prev, prev_n_views, lower_dep, root);

            prev_p_cache.retain_only(meta.normal_to_purple_child_parts.data(), meta.normal_to_purple_n);
            prev_n_cache.retain_only(&meta.purple_to_normal_child_part, 1);

            const double sec = std::chrono::duration<double>(tbutil::Clock::now() - t_part0).count();
            tbutil::log_line("[", mat.tag, "] depth=", depth, " part=", pid,
                             " done (", std::fixed, std::setprecision(3), sec, "s)");
        }

        mark_iter_complete(out_iter_dir);
        if (!g_cfg.keep_all_iters && depth >= 3) delete_iter_dir_if_exists(root, depth - 2);

        const double sec = std::chrono::duration<double>(tbutil::Clock::now() - t_iter0).count();
        tbutil::log_line("[", mat.tag, "] done depth=", depth,
                         " (", std::fixed, std::setprecision(3), sec, "s)");
    }

    if (g_cfg.export_runtime_normal_parts) {
        export_final_runtime_normal_parts(root, mat, g_cfg.max_depth);
    }
}

static void build_material_p10(
    const PartitionIndex18& pi,
    const std::array<PartMeta, 18>& pm,
    const MaterialSpec& mat10,
    const MaterialSpec& dep9b_mat)
{
    const fs::path root10 = material_root_path(g_cfg.out_dir, mat10);
    fs::create_directories(root10);

    const fs::path root9b = material_root_path(g_cfg.out_dir, dep9b_mat);
    const fs::path dep_check = part_file_path(root9b, dep9b_mat, TurnKind::NormalToMove, g_cfg.dep_p9b_iter, 0);
    if (!fs::exists(dep_check)) {
        throw std::runtime_error("p10 dependency missing: completed p9b iter not found: " + dep_check.string());
    }

    const int last_complete = find_last_complete_iter(root10);
    cleanup_incomplete_iters(root10, last_complete);

    int start_iter = last_complete + 1;
    if (start_iter < 1) start_iter = 1;
    if (start_iter > g_cfg.max_depth) {
        tbutil::log_line("[", mat10.tag, "] already complete up to max_depth=", g_cfg.max_depth);
        if (g_cfg.export_runtime_normal_parts) export_final_runtime_normal_parts(root10, mat10, g_cfg.max_depth);
        return;
    }

    tbutil::log_line("[", mat10.tag, "] resume last_complete=", last_complete,
                     " start_iter=", start_iter,
                     " root=", root10.string(),
                     " entries/part=", mat10.entries_per_part,
                     " total_states=", mat10.entries_per_part * 18ULL,
                     " dep9b_iter=", g_cfg.dep_p9b_iter);

    SameIterPartBitsCache prev_n_cache(root10, mat10, TurnKind::NormalToMove);
    SameIterPartBitsCache prev_p_cache(root10, mat10, TurnKind::PurpleToMove);
    CompletedPartByteCache dep9b_cache(root9b, dep9b_mat, TurnKind::NormalToMove, g_cfg.dep_p9b_iter);

    for (int depth = start_iter; depth <= g_cfg.max_depth; ++depth) {
        const auto t_iter0 = tbutil::Clock::now();
        const int prev = depth - 1;
        prev_n_cache.set_iter(prev);
        prev_p_cache.set_iter(prev);

        tbutil::log_line("[", mat10.tag, "] begin depth=", depth);
        const fs::path out_iter_dir = root10 / iter_dir_name(depth);
        if (fs::exists(out_iter_dir)) {
            std::error_code ec;
            fs::remove_all(out_iter_dir, ec);
            if (ec) throw std::runtime_error("failed to remove partial iter dir: " + out_iter_dir.string());
        }
        fs::create_directories(out_iter_dir);

        for (int pid = 0; pid < 18; ++pid) {
            const auto t_part0 = tbutil::Clock::now();
            const auto& meta = pm[static_cast<size_t>(pid)];
            const std::uint16_t part = static_cast<std::uint16_t>(pid);

            if (pid % 3 == 0) {
                tbutil::log_line("[", mat10.tag, "] depth=", depth, " part=", pid,
                                 " prevN(parts=", prev_n_cache.cached_parts(), ",bits~", prev_n_cache.bytes_loaded(), ")",
                                 " prevP(parts=", prev_p_cache.cached_parts(), ",bits~", prev_p_cache.bytes_loaded(), ")",
                                 " dep9b(parts=", dep9b_cache.cached_parts(), ",bytes=", dep9b_cache.bytes_loaded(), ")");
            }

            const PrevBitsViews prev_p_views = make_prev_bits_views(prev_p_cache, meta.normal_to_purple_child_parts.data(), meta.normal_to_purple_n);
            build_next_partition_normal_stream(pi, mat10, depth, part, prev, prev_p_views, root10);

            const PrevBitsViews prev_n_views = make_prev_bits_views(prev_n_cache, &meta.purple_to_normal_child_part, 1);
            const auto dep9b_sp = dep9b_cache.get(meta.purple_to_normal_child_part);
            build_next_partition_p10_stream(pi, mat10, dep9b_mat, depth, part, prev, prev_n_views,
                                            dep9b_sp.get(), meta.purple_to_normal_child_part, root10);

            prev_p_cache.retain_only(meta.normal_to_purple_child_parts.data(), meta.normal_to_purple_n);
            prev_n_cache.retain_only(&meta.purple_to_normal_child_part, 1);
            dep9b_cache.retain_only(&meta.purple_to_normal_child_part, 1);

            const double sec = std::chrono::duration<double>(tbutil::Clock::now() - t_part0).count();
            tbutil::log_line("[", mat10.tag, "] depth=", depth, " part=", pid,
                             " done (", std::fixed, std::setprecision(3), sec, "s)");
        }

        mark_iter_complete(out_iter_dir);
        if (!g_cfg.keep_all_iters && depth >= 3) delete_iter_dir_if_exists(root10, depth - 2);

        const double sec = std::chrono::duration<double>(tbutil::Clock::now() - t_iter0).count();
        tbutil::log_line("[", mat10.tag, "] done depth=", depth,
                         " (", std::fixed, std::setprecision(3), sec, "s)");
    }

    if (g_cfg.export_runtime_normal_parts) {
        export_final_runtime_normal_parts(root10, mat10, g_cfg.max_depth);
    }
}

// ============================================================
// Self-test
// ============================================================

static void run_self_test() {
    tbutil::log_line("[self-test] start");
    const PartitionIndex18 pi = build_partition_index18();
    const auto mats = std::array<MaterialSpec, 3>{
        make_material("p9a", 4, 4),
        make_material("p9b", 3, 5),
        make_material("p10", 4, 5),
    };

    std::mt19937_64 rng(0xD1357A5EULL);
    for (const auto& mat : mats) {
        const std::uint64_t legacy_total = states_for_counts(mat.key.pb, mat.key.pr, mat.key.pp);
        if (legacy_total != mat.entries_per_part * 18ULL) {
            throw std::runtime_error(std::string("entries_per_part*18 mismatch for ") + mat.tag);
        }

        for (int t = 0; t < 2; ++t) {
            const TurnKind turn = (t == 0 ? TurnKind::NormalToMove : TurnKind::PurpleToMove);
            for (int rep = 0; rep < 2000; ++rep) {
                const std::uint16_t part = static_cast<std::uint16_t>(rng() % 18ULL);
                const std::uint64_t idx = rng() % mat.entries_per_part;
                if (turn == TurnKind::NormalToMove) {
                    const auto pos = unrank_partitioned_normal(pi, mat, part, idx);
                    const auto rr = rank_partitioned_normal(pi, mat, pos.bb_player.bb_blue, pos.bb_player.bb_red, pos.bb_opponent.bb_blue);
                    if (rr.part != part || rr.idx != idx) {
                        throw std::runtime_error(std::string("rank/unrank mismatch (normal): ") + mat.tag);
                    }
                } else {
                    const auto pos = unrank_partitioned_purple(pi, mat, part, idx);
                    const auto rr = rank_partitioned_purple(pi, mat, pos.bb_player.bb_blue, pos.bb_opponent.bb_blue, pos.bb_opponent.bb_red);
                    if (rr.part != part || rr.idx != idx) {
                        throw std::runtime_error(std::string("rank/unrank mismatch (purple): ") + mat.tag);
                    }
                }
            }
            tbutil::log_line("[self-test] ok ", mat.tag, " turn=", (turn == TurnKind::NormalToMove ? 'N' : 'P'));
        }
    }

    tbutil::log_line("[self-test] passed");
}

} // namespace

int main(int argc, char** argv) {
    try {
        parse_args_or_throw(argc, argv);

#ifdef _OPENMP
        tbutil::log_line("[env] omp max_threads=", omp_get_max_threads(), " num_procs=", omp_get_num_procs());
#endif
        tbutil::log_line("[cfg] out_dir=", g_cfg.out_dir.string(),
                         " dep_dir=", g_cfg.dep_dir.string(),
                         " max_depth=", g_cfg.max_depth,
                         " zstd(level=", g_cfg.zstd_level, ",workers=", g_cfg.zstd_workers, ")",
                         " chunk_bytes=", g_cfg.chunk_bytes,
                         " omp_chunk=", g_cfg.omp_chunk);

        const PartitionIndex18 pi = build_partition_index18();
        const auto pm = build_part_meta(pi);

        if (g_cfg.self_test) {
            run_self_test();
            return 0;
        }

        const MaterialSpec mat9a = make_material("p9a", 4, 4);
        const MaterialSpec mat9b = make_material("p9b", 3, 5);
        const MaterialSpec mat10 = make_material("p10", 4, 5);

        const bool do_p9a = !g_cfg.only_material || *g_cfg.only_material == "p9a";
        const bool do_p9b = !g_cfg.only_material || *g_cfg.only_material == "p9b";
        const bool do_p10 = !g_cfg.only_material || *g_cfg.only_material == "p10";

        if (do_p9a) {
            const LegacyPurpleNormalDep dep = load_legacy_normal_dep_or_throw(g_cfg.dep_dir, PurpleMaterialKey{3, 3, 1, 4});
            build_material_p9(pi, pm, mat9a, dep);
        }

        if (do_p9b) {
            const LegacyPurpleNormalDep dep = load_legacy_normal_dep_or_throw(g_cfg.dep_dir, PurpleMaterialKey{3, 2, 1, 5});
            build_material_p9(pi, pm, mat9b, dep);
        }

        if (do_p10) {
            build_material_p10(pi, pm, mat10, mat9b);
        }

        tbutil::log_line("[done]");
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << std::endl;
        return 1;
    }
}
