// geister_perfect_information_tb_9_10.cpp
//   Perfect-information Geister DTW tablebase generator for 9/10 pieces, restricted to
//   "each side has exactly 1 red".
//
// Scope:
//   - 9-piece: (pb,pr,ob,or) = (4,1,3,1) and its swap (3,1,4,1)
//   - 10-piece: (4,1,4,1)
//
// Key idea:
//   - Quotient by LR mirror using a simple canonical representative:
//       "player red must be on the left half (columns A-C)."
//     (No fixed points because red counts are odd.)
//   - Split the canonical domain into 18*35 = 630 partitions by (player_red, opponent_red).
//   - Run synchronous iterative retrograde up to max_depth (default 210), storing each
//     iteration as a directory (iter_XXX). Only iter_{n-1} and iter_{n} are kept.
//   - Partition files are zstd-compressed (multithreaded) raw 1 byte/entry.
//
// Notes:
//   - 8-piece-and-below tablebases are expected to exist as *_obsblk.bin files generated
//     by geister_perfect_information_tb.cpp (legacy .bin is also accepted as fallback).
//   - This file intentionally does not share the same rank/unrank domain with the existing
//     generator for >=9 pieces. It is a dedicated builder/format for the restricted case.

#include <algorithm>
#include <array>
#include <atomic>
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
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
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
import geister_rank;
import geister_rank_obsblk;
import geister_core;

namespace fs = std::filesystem;

// ============================================================
// Constants / knobs
// ============================================================

static constexpr int kDefaultMaxDepth = 210;
static constexpr int kNumPartitions = 18 * 35;
static constexpr const char* kMagic = "GSTB";

// ============================================================
// Small helpers (logging)
// ============================================================

namespace tbutil {

using Clock = std::chrono::steady_clock;
static const Clock::time_point g_program_start = Clock::now();
static std::mutex g_log_mutex;

[[nodiscard]] inline std::string format_hms(double sec) {
    if (sec < 0) sec = 0;
    std::uint64_t s = (std::uint64_t)sec;
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
    auto [ptr, ec] = std::from_chars(b, e, out, 10);
    return (ec == std::errc() && ptr == e);
}

[[nodiscard]] inline bool parse_u64(std::string_view s, std::uint64_t& out) {
    out = 0;
    const char* b = s.data();
    const char* e = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(b, e, out, 10);
    return (ec == std::errc() && ptr == e);
}

} // namespace tbutil

// ============================================================
// Zstd compressed streambuf (write)
//   (Based on the user's sketch, with extra sink error checks.)
// ============================================================

class ZstdOStreambuf final : public std::streambuf {
public:
    ZstdOStreambuf(std::ostream& sink, const int compression_level, const int nbWorkers)
        : sink_(sink),
        cctx_(ZSTD_createCCtx()),
        in_buffer_(ZSTD_CStreamInSize()),
        out_buffer_(ZSTD_CStreamOutSize())
    {
        if (cctx_ == nullptr) {
            throw std::runtime_error("ZSTD_createCCtx() failed");
        }

        check_(ZSTD_CCtx_setParameter(cctx_, ZSTD_c_compressionLevel, compression_level), "ZSTD_c_compressionLevel");

        // Enable checksum for better corruption detection (small overhead).
        check_(ZSTD_CCtx_setParameter(cctx_, ZSTD_c_checksumFlag, 1), "ZSTD_c_checksumFlag");

        // Multi-threaded compression: nbWorkers >= 1 enables asynchronous compression mode.
        // Total threads ~= (caller thread + nbWorkers).
        if (nbWorkers > 0) {
            check_(ZSTD_CCtx_setParameter(cctx_, ZSTD_c_nbWorkers, nbWorkers), "ZSTD_c_nbWorkers");
        }
    }

    ZstdOStreambuf(const ZstdOStreambuf&) = delete;
    ZstdOStreambuf& operator=(const ZstdOStreambuf&) = delete;

    ~ZstdOStreambuf() override {
        try {
            if (!finished_) {
                finish();
            }
        }
        catch (...) {
            // Destructors must not throw.
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
        if (finished_) {
            return traits_type::eof();
        }
        if (ch == traits_type::eof()) {
            return traits_type::not_eof(ch);
        }
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

    static void check_(const size_t code, const char* what) {
        if (ZSTD_isError(code)) {
            throw std::runtime_error(std::string(what) + ": " + ZSTD_getErrorName(code));
        }
    }

    void write_out_(const void* data, const size_t size) {
        sink_.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
        if (!sink_) {
            throw std::runtime_error("ostream write failed");
        }
    }

    void consume_input_continue_() {
        ZSTD_inBuffer input = { in_buffer_.data(), in_pos_, 0 };

        while (input.pos < input.size) {
            ZSTD_outBuffer output = { out_buffer_.data(), out_buffer_.size(), 0 };
            const size_t remaining = ZSTD_compressStream2(cctx_, &output, &input, ZSTD_e_continue);
            check_(remaining, "ZSTD_compressStream2(ZSTD_e_continue)");
            if (output.pos) {
                write_out_(out_buffer_.data(), output.pos);
            }
        }

        in_pos_ = 0;
    }

    void flush_impl_(const ZSTD_EndDirective mode) {
        if (in_pos_ > 0) {
            consume_input_continue_();
        }

        ZSTD_inBuffer empty_input = { nullptr, 0, 0 };
        size_t remaining = 0;

        do {
            ZSTD_outBuffer output = { out_buffer_.data(), out_buffer_.size(), 0 };
            remaining = ZSTD_compressStream2(cctx_, &output, &empty_input, mode);
            check_(remaining, (mode == ZSTD_e_flush) ? "ZSTD_compressStream2(ZSTD_e_flush)" : "ZSTD_compressStream2(ZSTD_e_end)");
            if (output.pos) {
                write_out_(out_buffer_.data(), output.pos);
            }
        } while (remaining != 0);

        sink_.flush();
        if (!sink_) {
            throw std::runtime_error("ostream flush failed");
        }
    }
};

// ============================================================
// Zstd streaming read helpers
// ============================================================

static std::vector<std::uint8_t> load_zstd_file_exact(
    const fs::path& filename,
    const std::uint64_t expected_size)
{
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs) {
        throw std::runtime_error("failed to open for read: " + filename.string());
    }

    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    if (!dctx) {
        throw std::runtime_error("ZSTD_createDCtx failed");
    }

    const size_t in_size = ZSTD_DStreamInSize();
    const size_t out_size = ZSTD_DStreamOutSize();

    std::vector<char> in_buf(in_size);
    std::vector<char> out_buf(out_size);

    std::vector<std::uint8_t> out;
    out.resize((size_t)expected_size);

    std::uint64_t written = 0;

    ZSTD_inBuffer input{ in_buf.data(), 0, 0 };
    bool done = false;

    while (!done) {
        if (input.pos == input.size) {
            ifs.read(in_buf.data(), (std::streamsize)in_buf.size());
            const std::streamsize got = ifs.gcount();
            if (got <= 0) {
                // EOF
                input = { in_buf.data(), 0, 0 };
            }
            else {
                input = { in_buf.data(), (size_t)got, 0 };
            }
        }

        ZSTD_outBuffer output{ out_buf.data(), out_buf.size(), 0 };
        const size_t ret = ZSTD_decompressStream(dctx, &output, &input);
        if (ZSTD_isError(ret)) {
            const std::string err = ZSTD_getErrorName(ret);
            ZSTD_freeDCtx(dctx);
            throw std::runtime_error("ZSTD_decompressStream failed: " + err);
        }

        if (output.pos) {
            if (written + output.pos > expected_size) {
                ZSTD_freeDCtx(dctx);
                throw std::runtime_error("decompressed data exceeds expected_size");
            }
            std::memcpy(out.data() + (size_t)written, out_buf.data(), output.pos);
            written += (std::uint64_t)output.pos;
        }

        if (ret == 0) {
            done = true;
        }
        else {
            // Need more input; continue.
            if (ifs.eof() && input.pos == input.size) {
                // No more bytes available but not finished => truncated.
                break;
            }
        }
    }

    ZSTD_freeDCtx(dctx);

    if (written != expected_size) {
        std::ostringstream oss;
        oss << "decompressed size mismatch: got=" << written << " expected=" << expected_size
            << " file=" << filename.string();
        throw std::runtime_error(oss.str());
    }

    return out;
}

static void atomic_rename_best_effort(const fs::path& tmp, const fs::path& dst) {
    std::error_code ec;
    fs::rename(tmp, dst, ec);
    if (!ec) return;

    // Fallback: remove then rename.
    std::error_code ec2;
    fs::remove(dst, ec2);
    fs::rename(tmp, dst, ec);
    if (ec) {
        // Cleanup tmp if possible.
        std::error_code ec3;
        fs::remove(tmp, ec3);
        throw std::runtime_error("rename failed: " + ec.message());
    }
}

static void write_zstd_file_atomic(
    const fs::path& filename,
    const std::uint8_t* data,
    const std::uint64_t size,
    const int compression_level,
    const int nbWorkers)
{
    fs::create_directories(filename.parent_path());

    const fs::path tmp = fs::path(filename.string() + ".tmp");

    {
        std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
        if (!ofs) {
            throw std::runtime_error("failed to open for write: " + tmp.string());
        }

        // Make iostream throw on hard failures.
        ofs.exceptions(std::ios::badbit);

        ZstdOStreambuf zbuf(ofs, compression_level, nbWorkers);
        std::ostream os(&zbuf);
        os.exceptions(std::ios::badbit);

        os.write(reinterpret_cast<const char*>(data), (std::streamsize)size);
        if (!os) {
            throw std::runtime_error("ostream write failed (zstd)");
        }

        zbuf.finish();

        ofs.flush();
        if (!ofs) {
            throw std::runtime_error("ofstream flush failed");
        }
    }

    atomic_rename_best_effort(tmp, filename);
}

// ============================================================
// Combinatorics helpers (nCk up to 36)
// ============================================================

namespace comb {

static constexpr int kMaxN = 36;

consteval auto make_comb_table() {
    std::array<std::array<std::uint64_t, kMaxN + 1>, kMaxN + 1> C{};
    for (int n = 0; n <= kMaxN; ++n) {
        C[n][0] = 1;
        C[n][n] = 1;
        for (int k = 1; k < n; ++k) {
            C[n][k] = C[n - 1][k - 1] + C[n - 1][k];
        }
    }
    return C;
}

inline constexpr auto C = make_comb_table();

// Rank/unrank among N-bit patterns (colex), 0-based.
// Assumes S <= 4.
[[nodiscard]] inline std::uint64_t rank_patterns_colex(std::uint64_t x, const int N, const int S) noexcept {
    (void)N;
    assert(0 <= S && S <= 4);
    assert(std::popcount(x) == S);

    if (S == 0) return 0;
    std::uint64_t rank = 0;
    for (int i = 1; x; x &= x - 1) {
        const int p = std::countr_zero(x);
        rank += C[p][i++];
    }
    return rank;
}

[[nodiscard]] inline std::uint64_t unrank_patterns_colex(std::uint64_t r, int N, int S) noexcept {
    assert(0 <= S && S <= 4);
    assert(S <= N && N <= kMaxN);
    assert(r < C[N][S]);

    if (S == 0) return 0;
    std::uint64_t x = 0;
    int hi = N - 1;

    for (int i = S; i >= 2; --i) {
        int p = hi;
        while (C[p][i] > r) --p;
        x |= 1ULL << p;
        r -= C[p][i];
        hi = p - 1;
    }
    x |= 1ULL << static_cast<int>(r);
    return x;
}

} // namespace comb

// ============================================================
// LR mirror helper (same as geister_rank's private impl)
// ============================================================

namespace mirror {

consteval std::array<std::uint8_t, 256> make_rev8_table() {
    std::array<std::uint8_t, 256> t{};
    for (int x = 0; x < 256; ++x) {
        std::uint8_t v = static_cast<std::uint8_t>(x);
        std::uint8_t r = 0;
        for (int i = 0; i < 8; ++i) {
            r = static_cast<std::uint8_t>((r << 1) | (v & 1));
            v >>= 1;
        }
        t[x] = r;
    }
    return t;
}

inline constexpr auto REV8 = make_rev8_table();

[[nodiscard]] inline std::uint64_t mirror_lr_u64(std::uint64_t x) noexcept {
    std::uint64_t y = 0;
    for (int b = 0; b < 8; ++b) {
        const std::uint8_t byte = static_cast<std::uint8_t>((x >> (b * 8)) & 0xFF);
        y |= (std::uint64_t)REV8[byte] << (b * 8);
    }
    return y;
}

} // namespace mirror

// ============================================================
// Partition indexing (630 partitions):
//   player red: left half (18 squares)
//   opponent red: any other square (35 choices)
// ============================================================

struct PartitionMeta {
    // full-index (0..35 in pext order) for player red
    std::uint8_t rp_full = 0;
    // full-index (0..35 in pext order) for opponent red
    std::uint8_t ro_full = 0;

    // Up to 5 child partitions reachable via (non-capture) turn swap,
    // distinguished by where the *moving side's red* ends up before the swap.
    // We include "red unchanged" (blue moves) + up to 4 neighbors.
    std::array<std::uint16_t, 5> child_parts{};
    std::uint8_t child_parts_n = 0;
};

struct PartitionIndex {
    std::array<std::uint8_t, 18> left_to_full{};          // rpL -> full
    std::array<std::int8_t, 36> full_to_left{};           // full -> rpL (or -1)
    std::array<std::array<std::int8_t, 36>, 18> ro_to_ord{}; // rpL -> (ro_full -> roOrd or -1)
    std::array<std::array<std::uint8_t, 35>, 18> ord_to_ro{}; // rpL -> roOrd -> ro_full

    std::array<std::uint64_t, 36> full_idx_to_bb64{};     // full -> 64-bit square bit

    std::array<PartitionMeta, kNumPartitions> meta{};
};

// ============================================================
// Material specs
// ============================================================

enum class MatKind : std::uint8_t {
    M9A = 0, // (4,1,3,1)
    M9B = 1, // (3,1,4,1)
    M10 = 2, // (4,1,4,1)
};

struct MaterialSpec {
    MatKind kind;
    int pb = 0;
    int pr = 1;
    int ob = 0;
    int or_ = 1;

    std::string tag;
    std::uint64_t entries_per_partition = 0;
    std::uint64_t factor_ob = 0; // C(34-pb, ob)

    [[nodiscard]] Count4 as_count4() const {
        return Count4{ (std::uint8_t)pb, (std::uint8_t)pr, (std::uint8_t)ob, (std::uint8_t)or_ };
    }
};

static MaterialSpec make_material(MatKind k) {
    MaterialSpec m{};
    m.kind = k;
    m.pr = 1;
    m.or_ = 1;

    switch (k) {
    case MatKind::M9A:
        m.pb = 4; m.ob = 3;
        m.tag = "m9_pb4ob3";
        break;
    case MatKind::M9B:
        m.pb = 3; m.ob = 4;
        m.tag = "m9_pb3ob4";
        break;
    case MatKind::M10:
        m.pb = 4; m.ob = 4;
        m.tag = "m10_pb4ob4";
        break;
    default:
        throw std::runtime_error("unknown MatKind");
    }

    // entries_per_partition = C(34,pb) * C(34-pb, ob)
    m.factor_ob = comb::C[34 - m.pb][m.ob];
    m.entries_per_partition = comb::C[34][m.pb] * m.factor_ob;

    return m;
}

[[maybe_unused]] [[nodiscard]] static MatKind swap_kind(MatKind k) {
    switch (k) {
    case MatKind::M9A: return MatKind::M9B;
    case MatKind::M9B: return MatKind::M9A;
    case MatKind::M10: return MatKind::M10;
    default: return MatKind::M10;
    }
}

// ============================================================
// Runtime config
// ============================================================

struct RuntimeConfig {
    fs::path out_dir = "tb_9_10";
    fs::path dep_dir = "."; // where <=8 *_obsblk.bin deps live (legacy .bin also accepted)

    int max_depth = kDefaultMaxDepth;

    // zstd
    int zstd_level = 19;
    int zstd_workers = 16;

    // OpenMP scheduling
    int omp_chunk = 1 << 20;

    // For debugging / partial runs
    int start_partition = 0;
    int end_partition = kNumPartitions; // exclusive

    bool build_9 = true;
    bool build_10 = true;

    bool verbose = true;
};

static RuntimeConfig g_cfg;

static void print_usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " [options]\n\n"
        << "Options:\n"
        << "  --out DIR              Output root directory (default: tb_9_10)\n"
        << "                       Output layout:\n"
        << "                         DIR/tb9/iter_###/g9a_... .zst + g9b_... .zst\n"
        << "                         DIR/tb10/iter_###/g10_... .zst\n"
        << "                       If --start-part/--end-part is not the full range,\n"
        << "                       outputs go under DIR/tb9/parts_P0_P1/ and DIR/tb10/parts_P0_P1/.\n"
        << "  --dep DIR              Directory of <=8piece *_obsblk.bin deps (default: .)\n"
        << "                       Legacy headerless .bin is also accepted as a fallback.\n"
        << "  --max-depth N          Max DTW depth (default: 210)\n"
        << "  --zstd-level L         zstd compression level (default: 19)\n"
        << "  --zstd-workers W       zstd worker threads (default: 16)\n"
        << "  --omp-chunk N          OpenMP schedule chunk (default: 1048576)\n"
        << "  --start-part N         Start partition id (default: 0)\n"
        << "  --end-part N           End partition id, exclusive (default: 630)\n"
        << "  --only-9               Build only 9-piece tables\n"
        << "  --only-10              Build only 10-piece table (requires completed DIR/tb9/)\n"
        << "  -h, --help             Show this help\n";
}

// ============================================================
// Parallel failure flag (do not throw inside OpenMP loops)
// ============================================================

enum class TbFailKind : std::uint32_t {
    None = 0,
    NoLegalMoves = 1,
    UnexpectedException = 2,
    BadRank = 3,
};

static inline const char* tb_fail_kind_to_cstr(TbFailKind k) noexcept {
    switch (k) {
    case TbFailKind::None: return "None";
    case TbFailKind::NoLegalMoves: return "NoLegalMoves";
    case TbFailKind::UnexpectedException: return "UnexpectedException";
    case TbFailKind::BadRank: return "BadRank";
    default: return "Unknown";
    }
}

struct ParallelFailFlag {
    std::atomic<std::uint64_t> first_bad_idx{ UINT64_MAX };
    std::atomic<std::uint32_t> kind{ (std::uint32_t)TbFailKind::None };

    inline bool has_failed() const noexcept {
        return first_bad_idx.load(std::memory_order_relaxed) != UINT64_MAX;
    }

    inline void fail(std::uint64_t idx, TbFailKind k) noexcept {
        std::uint64_t expected = UINT64_MAX;
        if (first_bad_idx.compare_exchange_strong(expected, idx, std::memory_order_relaxed)) {
            kind.store((std::uint32_t)k, std::memory_order_relaxed);
        }
    }

    inline std::uint64_t bad_idx() const noexcept {
        return first_bad_idx.load(std::memory_order_relaxed);
    }
    inline TbFailKind bad_kind() const noexcept {
        return (TbFailKind)kind.load(std::memory_order_relaxed);
    }
};

// ============================================================
// Domain encoding / decoding
//   - compress board to 36-bit "full index" using pext/pdep with bb_board mask.
//   - canonicalize by requiring player red in left half.
// ============================================================

static constexpr std::uint64_t kBoardMask64 = bb_board;

// Left half (columns A-C) on the 6x6 board embedded in 8x8.
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

[[nodiscard]] inline std::uint64_t pext36(std::uint64_t bb) noexcept {
    return _pext_u64(bb, kBoardMask64);
}

[[nodiscard]] inline std::uint64_t pdep36(std::uint64_t pat36) noexcept {
    return _pdep_u64(pat36, kBoardMask64);
}

[[nodiscard]] inline int full_index_of_single_bb(std::uint64_t bb_single) noexcept {
    const std::uint64_t pat = pext36(bb_single);
    return (pat == 0) ? -1 : std::countr_zero(pat);
}

struct RankResult {
    std::uint16_t part = 0;
    std::uint64_t idx = 0;
};

// Canonicalize a position under LR mirror so that player-red is on the left half.
static inline void canonicalize_by_player_red_left(perfect_information_geister& pos) noexcept {
    if ((pos.bb_player.bb_red & kLeftMask64) != 0) return;

    pos.bb_player.bb_piece = mirror::mirror_lr_u64(pos.bb_player.bb_piece);
    pos.bb_player.bb_blue  = mirror::mirror_lr_u64(pos.bb_player.bb_blue);
    pos.bb_player.bb_red   = mirror::mirror_lr_u64(pos.bb_player.bb_red);
    pos.bb_opponent.bb_piece = mirror::mirror_lr_u64(pos.bb_opponent.bb_piece);
    pos.bb_opponent.bb_blue  = mirror::mirror_lr_u64(pos.bb_opponent.bb_blue);
    pos.bb_opponent.bb_red   = mirror::mirror_lr_u64(pos.bb_opponent.bb_red);
}

// ============================================================
// Partition index initialization
// ============================================================

static PartitionIndex build_partition_index() {
    PartitionIndex pi{};
    pi.full_to_left.fill(-1);
    for (auto& row : pi.ro_to_ord) row.fill(-1);

    // Precompute full_idx_to_bb64.
    for (int full = 0; full < 36; ++full) {
        pi.full_idx_to_bb64[(size_t)full] = pdep36(1ULL << full);
    }

    // Enumerate left-half squares in full-index order.
    int left_count = 0;
    for (int full = 0; full < 36; ++full) {
        const std::uint64_t bb = pi.full_idx_to_bb64[(size_t)full];
        if ((bb & kLeftMask64) != 0) {
            if (left_count >= 18) throw std::runtime_error("left_count overflow");
            pi.left_to_full[(size_t)left_count] = (std::uint8_t)full;
            pi.full_to_left[(size_t)full] = (std::int8_t)left_count;
            ++left_count;
        }
    }
    if (left_count != 18) {
        throw std::runtime_error("expected 18 left-half squares");
    }

    // For each left square, enumerate opponent-red squares (35 choices).
    for (int rpL = 0; rpL < 18; ++rpL) {
        const int rp_full = pi.left_to_full[(size_t)rpL];
        int ord = 0;
        for (int ro_full = 0; ro_full < 36; ++ro_full) {
            if (ro_full == rp_full) continue;
            if (ord >= 35) throw std::runtime_error("ro ord overflow");
            pi.ord_to_ro[(size_t)rpL][(size_t)ord] = (std::uint8_t)ro_full;
            pi.ro_to_ord[(size_t)rpL][(size_t)ro_full] = (std::int8_t)ord;
            ++ord;
        }
        if (ord != 35) throw std::runtime_error("expected 35 ro squares");
    }

    // Fill PartitionMeta (rp_full, ro_full) and its child partition set.
    for (int pid = 0; pid < kNumPartitions; ++pid) {
        const int rpL = pid / 35;
        const int roOrd = pid % 35;

        const int rp_full = pi.left_to_full[(size_t)rpL];
        const int ro_full = pi.ord_to_ro[(size_t)rpL][(size_t)roOrd];

        PartitionMeta pm{};
        pm.rp_full = (std::uint8_t)rp_full;
        pm.ro_full = (std::uint8_t)ro_full;

        const std::uint64_t bb_rp = pi.full_idx_to_bb64[(size_t)rp_full];
        const std::uint64_t bb_ro = pi.full_idx_to_bb64[(size_t)ro_full];

        // Candidate destinations for the moving side's red before the swap:
        //  - unchanged (blue move)
        //  - 4-neighborhood
        std::array<std::uint64_t, 5> dst_bb{};
        int dst_n = 0;
        dst_bb[(size_t)dst_n++] = bb_rp;

        const std::array<std::uint64_t, 4> shifts = {
            (bb_rp >> 8), (bb_rp << 8), (bb_rp >> 1), (bb_rp << 1)
        };
        for (std::uint64_t nb : shifts) {
            if ((nb & kBoardMask64) == 0) continue;
            // avoid duplicates (can happen only if board weird; still)
            bool dup = false;
            for (int k = 0; k < dst_n; ++k) {
                if (dst_bb[(size_t)k] == nb) { dup = true; break; }
            }
            if (!dup) {
                dst_bb[(size_t)dst_n++] = nb;
                if (dst_n == 5) break;
            }
        }

        // Map to child partitions.
        std::unordered_set<std::uint16_t> uniq;
        for (int k = 0; k < dst_n; ++k) {
            const std::uint64_t bb_rp_after = dst_bb[(size_t)k];

            // If the moving side's red steps onto the opponent red, that would mean capturing
            // the opponent's last red, which leaves the (or_=1) domain. Such a transition must
            // NOT be included in child partition sets.
            if (bb_rp_after == bb_ro) continue;

            // After move, do_move() would swap+rotate (bit_reverse64) and swap boards.
            // New player's red is old opponent red.
            std::uint64_t child_pr = bit_reverse64(bb_ro);
            std::uint64_t child_or = bit_reverse64(bb_rp_after);

            // Canonicalize by LR mirror (player red must be left).
            if ((child_pr & kLeftMask64) == 0) {
                child_pr = mirror::mirror_lr_u64(child_pr);
                child_or = mirror::mirror_lr_u64(child_or);
            }

            const int child_pr_full = full_index_of_single_bb(child_pr);
            const int child_or_full = full_index_of_single_bb(child_or);
            if (child_pr_full < 0 || child_or_full < 0) {
                throw std::runtime_error("child red mapping failed");
            }
            const int child_rpL = pi.full_to_left[(size_t)child_pr_full];
            if (child_rpL < 0) {
                throw std::runtime_error("child player red not in left after canonicalization");
            }
            const int child_roOrd = pi.ro_to_ord[(size_t)child_rpL][(size_t)child_or_full];
            if (child_roOrd < 0) {
                throw std::runtime_error("child opponent red ord mapping failed");
            }
            const std::uint16_t child_pid = (std::uint16_t)(child_rpL * 35 + child_roOrd);
            uniq.insert(child_pid);
        }

        pm.child_parts_n = 0;
        for (std::uint16_t v : uniq) {
            if (pm.child_parts_n >= pm.child_parts.size()) break;
            pm.child_parts[(size_t)pm.child_parts_n++] = v;
        }

        // Ensure deterministic order for reproducibility.
        std::sort(pm.child_parts.begin(), pm.child_parts.begin() + pm.child_parts_n);

        pi.meta[(size_t)pid] = pm;
    }

    return pi;
}

// ============================================================
// Rank/unrank inside a partition (custom domain)
// ============================================================

struct UnrankedPos {
    std::uint64_t bb_player_blue = 0;
    std::uint64_t bb_player_red = 0;
    std::uint64_t bb_opponent_blue = 0;
    std::uint64_t bb_opponent_red = 0;
};

static inline UnrankedPos unrank_in_partition(
    const PartitionIndex& pi,
    const MaterialSpec& mat,
    const std::uint16_t part,
    const std::uint64_t idx)
{
    const PartitionMeta& pm = pi.meta[(size_t)part];
    const std::uint8_t rp_full = pm.rp_full;
    const std::uint8_t ro_full = pm.ro_full;

    const std::uint64_t rp_pat36 = 1ULL << rp_full;
    const std::uint64_t ro_pat36 = 1ULL << ro_full;
    const std::uint64_t all36 = (1ULL << 36) - 1ULL;
    const std::uint64_t rem_mask36 = all36 ^ rp_pat36 ^ ro_pat36; // 34 bits set

    const std::uint64_t factor_ob = mat.factor_ob; // C(34-pb, ob)
    const std::uint64_t r_pb = idx / factor_ob;
    const std::uint64_t r_ob = idx % factor_ob;

    const std::uint64_t pb_pat34 = comb::unrank_patterns_colex(r_pb, 34, mat.pb);
    const std::uint64_t pb_pat36 = _pdep_u64(pb_pat34, rem_mask36);

    const std::uint64_t rem2_mask36 = rem_mask36 ^ pb_pat36; // 34-pb set
    const int N2 = 34 - mat.pb;
    const std::uint64_t ob_patN = comb::unrank_patterns_colex(r_ob, N2, mat.ob);
    const std::uint64_t ob_pat36 = _pdep_u64(ob_patN, rem2_mask36);

    UnrankedPos out{};
    out.bb_player_red = pdep36(rp_pat36);
    out.bb_opponent_red = pdep36(ro_pat36);
    out.bb_player_blue = pdep36(pb_pat36);
    out.bb_opponent_blue = pdep36(ob_pat36);

    return out;
}

static inline RankResult rank_partitioned(
    const PartitionIndex& pi,
    const MaterialSpec& mat,
    perfect_information_geister pos)
{
    canonicalize_by_player_red_left(pos);

    const std::uint64_t rp_pat36 = pext36(pos.bb_player.bb_red);
    const std::uint64_t ro_pat36 = pext36(pos.bb_opponent.bb_red);
    if (rp_pat36 == 0 || ro_pat36 == 0) {
        throw std::runtime_error("rank_partitioned: missing red bit (outside domain)");
    }
    const int rp_full = std::countr_zero(rp_pat36);
    const int ro_full = std::countr_zero(ro_pat36);

    const int rpL = pi.full_to_left[(size_t)rp_full];
    if (rpL < 0) {
        throw std::runtime_error("rank_partitioned: player red not in left half after canonicalization");
    }
    const int roOrd = pi.ro_to_ord[(size_t)rpL][(size_t)ro_full];
    if (roOrd < 0) {
        throw std::runtime_error("rank_partitioned: opponent red mapping failed");
    }
    const std::uint16_t part = (std::uint16_t)(rpL * 35 + roOrd);

    const std::uint64_t pb_pat36 = pext36(pos.bb_player.bb_blue);
    const std::uint64_t ob_pat36 = pext36(pos.bb_opponent.bb_blue);

    const std::uint64_t all36 = (1ULL << 36) - 1ULL;
    const std::uint64_t rem_mask36 = all36 ^ rp_pat36 ^ ro_pat36;

    const std::uint64_t pb_pat34 = _pext_u64(pb_pat36, rem_mask36);
    const std::uint64_t r_pb = comb::rank_patterns_colex(pb_pat34, 34, mat.pb);

    const std::uint64_t rem2_mask36 = rem_mask36 ^ pb_pat36;
    const std::uint64_t ob_patN = _pext_u64(ob_pat36, rem2_mask36);
    const int N2 = 34 - mat.pb;
    const std::uint64_t r_ob = comb::rank_patterns_colex(ob_patN, N2, mat.ob);

    const std::uint64_t idx = r_pb * mat.factor_ob + r_ob;
    // Sanity: should always hold.
    if (idx >= mat.entries_per_partition) {
        throw std::runtime_error("rank_partitioned: idx out of range");
    }
    return RankResult{ part, idx };
}


// ============================================================
// File naming
// ============================================================

static inline std::string iter_dir_name(int iter) {
    std::ostringstream oss;
    oss << "iter_" << std::setw(3) << std::setfill('0') << iter;
    return oss.str();
}

static inline std::string part_file_name(
    const MaterialSpec& mat,
    const int iter,
    const std::uint16_t part)
{
    std::ostringstream oss;
    oss << mat.tag
        << "_i" << std::setw(3) << std::setfill('0') << (int)part
        << "_n" << std::setw(3) << std::setfill('0') << iter
        << "_" << kMagic
        << "_" << mat.entries_per_partition
        << ".zst";
    return oss.str();
}

static inline fs::path part_file_path(
    const fs::path& root,
    const MaterialSpec& mat,
    const int iter,
    const std::uint16_t part)
{
    fs::path p = root / iter_dir_name(iter) / part_file_name(mat, iter, part);
    return p;
}

// Existing <=8 file naming (compatible with geister_perfect_information_tb.cpp)
static inline std::string make_tablebase_filename_obsblk_bin(std::uint16_t id, const Count4& c) {
    std::ostringstream oss;
    oss << "id" << std::setw(3) << std::setfill('0') << (int)id
        << "_pb" << (int)c.pop_pb
        << "pr" << (int)c.pop_pr
        << "ob" << (int)c.pop_ob
        << "or" << (int)c.pop_or
        << "_obsblk.bin";
    return oss.str();
}

static inline std::string make_tablebase_filename_legacy_bin(std::uint16_t id, const Count4& c) {
    std::ostringstream oss;
    oss << "id" << std::setw(3) << std::setfill('0') << (int)id
        << "_pb" << (int)c.pop_pb
        << "pr" << (int)c.pop_pr
        << "ob" << (int)c.pop_ob
        << "or" << (int)c.pop_or
        << ".bin";
    return oss.str();
}

static std::vector<std::uint8_t> load_obsblk_bin_as_legacy(
    const fs::path& fn,
    const Count4& c,
    const std::uint64_t N)
{
    auto obsblk = tbio::load_tablebase_bin_streaming(fn, N, (std::uint8_t)g_cfg.max_depth);
    std::vector<std::uint8_t> legacy((size_t)N, 0);
    for (std::uint64_t legacy_idx = 0; legacy_idx < N; ++legacy_idx) {
        std::uint64_t pb = 0, pr = 0, ob = 0, oc = 0;
        unrank_geister_perfect_information(legacy_idx, c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or, pb, pr, ob, oc);
        const std::uint64_t obsblk_idx = rank_geister_perfect_information_obsblk(pb, pr, ob, oc);
        legacy[(size_t)legacy_idx] = obsblk[(size_t)obsblk_idx];
    }
    return legacy;
}

// ============================================================
// Load deps (<=8 piece) from existing bins
// ============================================================

struct LowerDep {
    Count4 c{};
    std::vector<std::uint8_t> tb;

    [[nodiscard]] inline std::uint8_t get(std::uint64_t idx) const noexcept {
        return tb[(size_t)idx];
    }
};

static LowerDep load_lower_dep_bin(const fs::path& dep_dir, const Count4& c) {
    const std::uint16_t id = material_id_of(c);
    const std::uint64_t N = states_of(c);
    const fs::path fn_obsblk = dep_dir / make_tablebase_filename_obsblk_bin(id, c);
    const fs::path fn_legacy = dep_dir / make_tablebase_filename_legacy_bin(id, c);

    LowerDep d{};
    d.c = c;

    if (tbio::tablebase_bin_looks_valid(fn_obsblk, N)) {
        tbutil::log_line("[DEP] load obsblk ", fn_obsblk.string());
        d.tb = load_obsblk_bin_as_legacy(fn_obsblk, c, N);
        tbutil::log_line("[DEP] loaded obsblk ", fn_obsblk.string(), " bytes=", d.tb.size());
        return d;
    }

    if (tbio::tablebase_bin_looks_valid(fn_legacy, N)) {
        tbutil::log_line("[DEP] load legacy ", fn_legacy.string());
        d.tb = tbio::load_tablebase_bin_streaming(fn_legacy, N, (std::uint8_t)g_cfg.max_depth);
        tbutil::log_line("[DEP] loaded legacy ", fn_legacy.string(), " bytes=", d.tb.size());
        return d;
    }

    std::ostringstream oss;
    oss << "missing/invalid <=8 dependency for material id=" << id << ":\n"
        << "  expected obsblk:  " << fn_obsblk.string() << " (" << N << " bytes)\n"
        << "  or legacy bin:   " << fn_legacy.string() << " (" << N << " bytes)\n"
        << "Build it first using geister_perfect_information_tb.cpp with --upto-total 8.";
    throw std::runtime_error(oss.str());
}

// ============================================================
// Load partition table for previous iteration
// ============================================================

[[maybe_unused]] static std::vector<std::uint8_t> load_partition_table_prev(
    const fs::path& root,
    const MaterialSpec& mat,
    const int prev_iter,
    const std::uint16_t part)
{
    if (prev_iter <= 0) {
        // iter=0 is implicit all-zero.
        return std::vector<std::uint8_t>((size_t)mat.entries_per_partition, 0);
    }

    const fs::path fn = part_file_path(root, mat, prev_iter, part);
    if (!fs::exists(fn)) {
        throw std::runtime_error("missing input partition file: " + fn.string());
    }

    return load_zstd_file_exact(fn, mat.entries_per_partition);
}

// ============================================================
// Compute next iteration for one partition (generic)
// ============================================================

struct TableView {
    const std::uint8_t* p = nullptr;
    std::uint64_t n = 0;
    [[nodiscard]] bool empty() const noexcept { return p == nullptr || n == 0; }
    [[nodiscard]] std::uint8_t get(std::uint64_t idx) const noexcept { return p[(size_t)idx]; }
};

struct PartTableSet {
    std::array<TableView, kNumPartitions> view{};
};

enum class CapBlueMode : std::uint8_t {
    ToPartitioned,
    ToLowerBin,
    None,
};

static std::vector<std::uint8_t> compute_next_partition_openmp(
    const PartitionIndex& pi,
    const MaterialSpec& self,
    const int depth,
    const std::uint16_t part,
    const std::vector<std::uint8_t>& cur_self,
    const MaterialSpec& child_noncap,
    const PartTableSet& noncap_child_tables,
    const CapBlueMode cap_mode,
    const MaterialSpec* cap_child_mat,
    const PartTableSet* cap_child_tables,
    const LowerDep* cap_lower,
    const int omp_chunk)
{
    const std::uint64_t N = self.entries_per_partition;
    if ((std::uint64_t)cur_self.size() != N) {
        throw std::runtime_error("cur_self size mismatch");
    }

    std::vector<std::uint8_t> next((size_t)N, 0);
    ParallelFailFlag fail;

    {
#pragma omp parallel for schedule(dynamic, omp_chunk)
        for (std::int64_t i = 0; i < (std::int64_t)N; ++i) {
            if (fail.has_failed()) continue;

            const std::uint64_t idx = (std::uint64_t)i;
            const std::uint8_t v = cur_self[(size_t)idx];
            if (v != 0) {
                next[(size_t)idx] = v;
                continue;
            }

            try {
                const UnrankedPos up = unrank_in_partition(pi, self, part, idx);

                perfect_information_geister pos{
                    player_board{ up.bb_player_red, up.bb_player_blue },
                    player_board{ up.bb_opponent_red, up.bb_opponent_blue }
                };

                if (pos.is_immediate_win()) { next[(size_t)idx] = 1; continue; }
                if (pos.is_immediate_loss()) { next[(size_t)idx] = 2; continue; }

                std::array<move, 32> moves{};
                const int nm = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
                if (nm <= 0) {
                    fail.fail(idx, TbFailKind::NoLegalMoves);
                    continue;
                }

                std::uint8_t out = 0;

                if (depth == 1) {
                    // Win immediately by capturing opponent's last blue.
                    if (self.ob == 1) {
                        for (int k = 0; k < nm; ++k) {
                            if (moves[(size_t)k].if_capture_blue()) { out = 1; break; }
                        }
                    }
                }
                else if (depth & 1) {
                    // winning ply
                    for (int k = 0; k < nm; ++k) {
                        const move m = moves[(size_t)k];

                        if (m.if_capture_blue() && self.ob == 1) { out = 1; break; }

                        // Capturing the opponent's last red is an immediate loss for the mover.
                        if (m.if_capture_red() && self.or_ == 1) {
                            continue;
                        }

                        std::uint8_t t = 0;

                        if (m.if_capture_blue()) {
                            if (self.ob == 1) {
                                // already handled
                                t = 0;
                            }
                            else if (cap_mode == CapBlueMode::ToLowerBin) {
                                if (!cap_lower || cap_lower->tb.empty()) continue;
                                perfect_information_geister child = pos;
                                perfect_information_geister::do_move(m, child);
                                const std::uint64_t code = rank_geister_perfect_information(
                                    child.bb_player.bb_blue, child.bb_player.bb_red,
                                    child.bb_opponent.bb_blue, child.bb_opponent.bb_red);
                                t = cap_lower->get(code);
                            }
                            else if (cap_mode == CapBlueMode::ToPartitioned) {
                                if (!cap_child_mat || !cap_child_tables) continue;
                                perfect_information_geister child = pos;
                                perfect_information_geister::do_move(m, child);
                                const RankResult rr = rank_partitioned(pi, *cap_child_mat, child);
                                const TableView tv = cap_child_tables->view[(size_t)rr.part];
                                if (tv.empty()) { fail.fail(idx, TbFailKind::BadRank); break; }
                                t = tv.get(rr.idx);
                            }
                            else {
                                continue;
                            }
                        }
                        else if (m.if_capture_red()) {
                            // self.or_>1 case not used in this restricted generator.
                            continue;
                        }
                        else {
                            perfect_information_geister child = pos;
                            perfect_information_geister::do_move(m, child);
                            const RankResult rr = rank_partitioned(pi, child_noncap, child);
                            const TableView tv = noncap_child_tables.view[(size_t)rr.part];
                            if (tv.empty()) { fail.fail(idx, TbFailKind::BadRank); break; }
                            t = tv.get(rr.idx);
                        }

                        if (t == (std::uint8_t)(depth - 1)) { out = (std::uint8_t)depth; break; }
                    }
                }
                else {
                    // losing ply
                    bool forced = true;
                    std::uint8_t max_odd = 0;

                    for (int k = 0; k < nm; ++k) {
                        const move m = moves[(size_t)k];

                        if (m.if_capture_blue() && self.ob == 1) { forced = false; break; }

                        std::uint8_t t = 0;

                        if (m.if_capture_blue()) {
                            if (self.ob == 1) {
                                forced = false; break;
                            }
                            else if (cap_mode == CapBlueMode::ToLowerBin) {
                                if (!cap_lower || cap_lower->tb.empty()) { forced = false; break; }
                                perfect_information_geister child = pos;
                                perfect_information_geister::do_move(m, child);
                                const std::uint64_t code = rank_geister_perfect_information(
                                    child.bb_player.bb_blue, child.bb_player.bb_red,
                                    child.bb_opponent.bb_blue, child.bb_opponent.bb_red);
                                t = cap_lower->get(code);
                            }
                            else if (cap_mode == CapBlueMode::ToPartitioned) {
                                if (!cap_child_mat || !cap_child_tables) { forced = false; break; }
                                perfect_information_geister child = pos;
                                perfect_information_geister::do_move(m, child);
                                const RankResult rr = rank_partitioned(pi, *cap_child_mat, child);
                                const TableView tv = cap_child_tables->view[(size_t)rr.part];
                                if (tv.empty()) { forced = false; break; }
                                t = tv.get(rr.idx);
                            }
                            else {
                                forced = false; break;
                            }
                        }
                        else if (m.if_capture_red()) {
                            if (self.or_ == 1) {
                                // capturing last red => immediate loss for mover => opponent wins in 1
                                t = 1;
                            }
                            else {
                                forced = false; break;
                            }
                        }
                        else {
                            perfect_information_geister child = pos;
                            perfect_information_geister::do_move(m, child);
                            const RankResult rr = rank_partitioned(pi, child_noncap, child);
                            const TableView tv = noncap_child_tables.view[(size_t)rr.part];
                            if (tv.empty()) { forced = false; break; }
                            t = tv.get(rr.idx);
                        }

                        if (t == 0) { forced = false; break; }
                        if ((t & 1U) == 0) { forced = false; break; }
                        if (t > max_odd) max_odd = t;
                    }

                    if (forced && max_odd == (std::uint8_t)(depth - 1)) {
                        out = (std::uint8_t)depth;
                    }
                }

                next[(size_t)idx] = out;
            }
            catch (...) {
                fail.fail(idx, TbFailKind::UnexpectedException);
            }
        }
    } // omp

    if (fail.has_failed()) {
        std::ostringstream oss;
        oss << "partition compute failed: part=" << part
            << " depth=" << depth
            << " idx=" << fail.bad_idx()
            << " kind=" << tb_fail_kind_to_cstr(fail.bad_kind());
        throw std::runtime_error(oss.str());
    }

    return next;
}

// ============================================================
// Iteration directory handling / resume
// ============================================================

// Parse iter directory name "iter_XXX" -> XXX, else -1.
static int parse_iter_dir_number(const std::string& name) {
    // expects "iter_XXX" (3 digits)
    if (name.size() != 8) return -1;
    if (name.rfind("iter_", 0) != 0) return -1;
    int v = 0;
    for (int i = 5; i < 8; ++i) {
        const char c = name[(size_t)i];
        if (c < '0' || c > '9') return -1;
        v = v * 10 + (c - '0');
    }
    return v;
}

static inline bool iter_complete_exists(const fs::path& iter_dir) {
    return fs::exists(iter_dir / ".complete");
}

static int find_last_complete_iter(const fs::path& root) {
    if (!fs::exists(root)) return 0;
    int last = 0;
    for (const auto& ent : fs::directory_iterator(root)) {
        if (!ent.is_directory()) continue;
        const std::string name = ent.path().filename().string();
        const int n = parse_iter_dir_number(name);
        if (n <= 0) continue;
        if (iter_complete_exists(ent.path())) {
            if (n > last) last = n;
        }
    }
    return last;
}

static void cleanup_incomplete_iters(const fs::path& root, const int last_complete) {
    if (!fs::exists(root)) return;
    for (const auto& ent : fs::directory_iterator(root)) {
        if (!ent.is_directory()) continue;
        const std::string name = ent.path().filename().string();
        const int n = parse_iter_dir_number(name);
        if (n <= 0) continue;

        if (n > last_complete || !iter_complete_exists(ent.path())) {
            tbutil::log_line("[RESUME] removing incomplete iter dir: ", ent.path().string());
            std::error_code ec;
            fs::remove_all(ent.path(), ec);
            if (ec) {
                throw std::runtime_error("failed to remove_all: " + ent.path().string() + " : " + ec.message());
            }
        }
    }
}

static void mark_iter_complete(const fs::path& iter_dir) {
    fs::create_directories(iter_dir);
    const fs::path marker = iter_dir / ".complete";
    std::ofstream ofs(marker, std::ios::binary | std::ios::trunc);
    ofs << "ok\n";
}

static void delete_iter_dir_if_exists(const fs::path& root, const int iter) {
    const fs::path dir = root / iter_dir_name(iter);
    if (!fs::exists(dir)) return;
    tbutil::log_line("[CLEAN] removing old iter dir: ", dir.string());
    std::error_code ec;
    fs::remove_all(dir, ec);
    if (ec) {
        throw std::runtime_error("failed to remove_all: " + dir.string() + " : " + ec.message());
    }
}


// ============================================================
// Partition table cache (zstd partitions)
// ============================================================

class PartitionTableCache {
public:
    PartitionTableCache() = default;

    PartitionTableCache(fs::path root, MaterialSpec mat)
        : root_(std::move(root)), mat_(std::move(mat)) {}

    void reset(fs::path root, MaterialSpec mat) {
        root_ = std::move(root);
        mat_ = std::move(mat);
        set_iter(-1);
    }

    void set_iter(const int it) {
        if (iter_ == it) return;
        iter_ = it;
        tables_.clear();
        bytes_loaded_ = 0;
        // iter=0 uses a shared all-zero table; drop it once we move past it.
        if (iter_ > 0) {
            zero_.reset();
        }
    }

    [[nodiscard]] int iter() const noexcept { return iter_; }
    [[nodiscard]] std::uint64_t bytes_loaded() const noexcept { return bytes_loaded_; }
    [[nodiscard]] std::size_t cached_parts() const noexcept { return tables_.size(); }

    // Load (or reuse) a partition table for the current iter.
    [[nodiscard]] std::shared_ptr<const std::vector<std::uint8_t>> get(const std::uint16_t part) {
        if (iter_ <= 0) {
            if (!zero_) {
                tbutil::log_line("[CACHE] allocate zero-table: mat=", mat_.tag, " bytes=", mat_.entries_per_partition);
                zero_ = std::make_shared<std::vector<std::uint8_t>>((size_t)mat_.entries_per_partition, 0);
            }
            return zero_;
        }

        auto it = tables_.find(part);
        if (it != tables_.end()) {
            return it->second;
        }

        const fs::path fn = part_file_path(root_, mat_, iter_, part);
        if (!fs::exists(fn)) {
            throw std::runtime_error("missing dependency partition file: " + fn.string());
        }

        auto vec = load_zstd_file_exact(fn, mat_.entries_per_partition);
        auto sp = std::make_shared<std::vector<std::uint8_t>>(std::move(vec));

        bytes_loaded_ += (std::uint64_t)sp->size();
        tables_.emplace(part, sp);

        return sp;
    }

    // Keep only the given part ids in the cache (exact "working set" retention).
    void retain_only(const std::uint16_t* keep_parts, const int keep_n) {
        if (iter_ <= 0) return;
        for (auto it = tables_.begin(); it != tables_.end();) {
            bool keep = false;
            for (int i = 0; i < keep_n; ++i) {
                if (it->first == keep_parts[i]) { keep = true; break; }
            }
            if (!keep) {
                bytes_loaded_ -= (std::uint64_t)it->second->size();
                it = tables_.erase(it);
            }
            else {
                ++it;
            }
        }
    }

private:
    fs::path root_{};
    MaterialSpec mat_{};
    int iter_ = -1;

    std::unordered_map<std::uint16_t, std::shared_ptr<const std::vector<std::uint8_t>>> tables_{};
    std::shared_ptr<const std::vector<std::uint8_t>> zero_{};

    std::uint64_t bytes_loaded_ = 0;
};

struct DepTables {
    PartTableSet views{};
    std::array<std::shared_ptr<const std::vector<std::uint8_t>>, 5> keepalive{};
    int keepalive_n = 0;
};

static DepTables make_dep_tables_from_cache(
    PartitionTableCache& cache,
    const MaterialSpec& mat,
    const PartitionMeta& pm,
    const std::uint16_t override_part,
    const std::vector<std::uint8_t>* override_vec)
{
    DepTables out{};
    for (auto& v : out.views.view) v = TableView{};

    // For iter=0 (depth=1), our 9/10 builders never consult child tables.
    // Avoid allocating huge all-zero deps for that case.
    if (cache.iter() <= 0) {
        return out;
    }

    out.keepalive_n = 0;
    for (std::uint8_t k = 0; k < pm.child_parts_n; ++k) {
        const std::uint16_t child_part = pm.child_parts[(size_t)k];

        if (override_vec && child_part == override_part) {
            out.views.view[(size_t)child_part] = TableView{ override_vec->data(), mat.entries_per_partition };
            continue;
        }

        auto sp = cache.get(child_part);
        out.keepalive[(size_t)out.keepalive_n++] = sp;
        out.views.view[(size_t)child_part] = TableView{ sp->data(), (std::uint64_t)sp->size() };
    }

    return out;
}

// ============================================================
// Build-group roots
// ============================================================

static inline fs::path out_root_tb9_base() {
    return g_cfg.out_dir / "tb9";
}
static inline fs::path out_root_tb10_base() {
    return g_cfg.out_dir / "tb10";
}

static inline fs::path with_part_range_subdir(const fs::path& base, const int p0, const int p1) {
    if (p0 == 0 && p1 == kNumPartitions) return base;
    std::ostringstream oss;
    oss << "parts_" << p0 << "_" << p1;
    return base / oss.str();
}

// ============================================================
// Build loop: 9-piece (m9a + m9b)
// ============================================================

static void build_tb9(
    const PartitionIndex& pi,
    const MaterialSpec& m9a,
    const MaterialSpec& m9b,
    const LowerDep& dep_2141,
    const LowerDep& dep_3131)
{
    const int p0 = std::max(0, g_cfg.start_partition);
    const int p1 = std::min(kNumPartitions, g_cfg.end_partition);

    const fs::path base = out_root_tb9_base();
    const fs::path root = with_part_range_subdir(base, p0, p1);
    fs::create_directories(root);

    const int last_complete = find_last_complete_iter(root);
    cleanup_incomplete_iters(root, last_complete);

    int start_iter = last_complete + 1;
    if (start_iter < 1) start_iter = 1;

    if (start_iter > g_cfg.max_depth) {
        tbutil::log_line("[TB9] already complete up to max_depth=", g_cfg.max_depth);
        return;
    }

    tbutil::log_line("[TB9] resume last_complete=", last_complete, " start_iter=", start_iter,
        " parts=[", p0, ",", p1, ") root=", root.string());

    PartitionTableCache cache9a(root, m9a);
    PartitionTableCache cache9b(root, m9b);

    for (int depth = start_iter; depth <= g_cfg.max_depth; ++depth) {
        const auto t_iter0 = tbutil::Clock::now();
        const int prev = depth - 1;

        tbutil::log_line("[TB9] begin depth=", depth);

        cache9a.set_iter(prev);
        cache9b.set_iter(prev);

        const fs::path out_iter_dir = root / iter_dir_name(depth);
        if (fs::exists(out_iter_dir)) {
            tbutil::log_line("[TB9] removing existing out dir (partial?): ", out_iter_dir.string());
            std::error_code ec;
            fs::remove_all(out_iter_dir, ec);
            if (ec) throw std::runtime_error("failed to remove_all: " + out_iter_dir.string());
        }
        fs::create_directories(out_iter_dir);

        for (int pid = p0; pid < p1; ++pid) {
            const auto t_part0 = tbutil::Clock::now();
            const std::uint16_t part = (std::uint16_t)pid;
            const PartitionMeta& pm = pi.meta[(size_t)part];

            if (g_cfg.verbose && (pid % 10 == 0)) {
                tbutil::log_line("[TB9] depth=", depth, " part=", pid, "/", (p1 - 1),
                    " cache9a(parts=", cache9a.cached_parts(), ",bytes=", cache9a.bytes_loaded(), ")",
                    " cache9b(parts=", cache9b.cached_parts(), ",bytes=", cache9b.bytes_loaded(), ")");
            }

            // 9A: non-capture -> 9B, capture blue -> dep_2141
            {
                const auto cur9a_sp = cache9a.get(part);
                const auto& cur9a = *cur9a_sp;

                DepTables dep9b{};
                if (prev > 0) {
                    dep9b = make_dep_tables_from_cache(cache9b, m9b, pm, UINT16_MAX, nullptr);
                }

                auto next9a = compute_next_partition_openmp(
                    pi, m9a, depth, part, cur9a,
                    m9b, dep9b.views,
                    CapBlueMode::ToLowerBin,
                    nullptr, nullptr,
                    &dep_2141,
                    g_cfg.omp_chunk);

                const fs::path fn = part_file_path(root, m9a, depth, part);
                write_zstd_file_atomic(fn, next9a.data(), m9a.entries_per_partition, g_cfg.zstd_level, g_cfg.zstd_workers);
            }

            // 9B: non-capture -> 9A, capture blue -> dep_3131
            {
                const auto cur9b_sp = cache9b.get(part);
                const auto& cur9b = *cur9b_sp;

                DepTables dep9a{};
                if (prev > 0) {
                    dep9a = make_dep_tables_from_cache(cache9a, m9a, pm, UINT16_MAX, nullptr);
                }

                auto next9b = compute_next_partition_openmp(
                    pi, m9b, depth, part, cur9b,
                    m9a, dep9a.views,
                    CapBlueMode::ToLowerBin,
                    nullptr, nullptr,
                    &dep_3131,
                    g_cfg.omp_chunk);

                const fs::path fn = part_file_path(root, m9b, depth, part);
                write_zstd_file_atomic(fn, next9b.data(), m9b.entries_per_partition, g_cfg.zstd_level, g_cfg.zstd_workers);
            }

            // Working-set eviction for (n-1)-th tables:
            // keep only the child partitions (<=5) that might be needed again soon.
            cache9a.retain_only(pm.child_parts.data(), pm.child_parts_n);
            cache9b.retain_only(pm.child_parts.data(), pm.child_parts_n);

            if (g_cfg.verbose && (pid % 10 == 0)) {
                const double sec = std::chrono::duration<double>(tbutil::Clock::now() - t_part0).count();
                tbutil::log_line("[TB9] depth=", depth, " part=", pid, " done (", std::fixed, std::setprecision(3), sec, "s)");
            }
        }

        mark_iter_complete(out_iter_dir);

        // Keep only prev + current.
        if (depth >= 3) {
            delete_iter_dir_if_exists(root, depth - 2);
        }

        const double sec_iter = std::chrono::duration<double>(tbutil::Clock::now() - t_iter0).count();
        tbutil::log_line("[TB9] done depth=", depth, " (", std::fixed, std::setprecision(3), sec_iter, "s)");
    }

    tbutil::log_line("[TB9] finished up to depth=", g_cfg.max_depth);
}

// ============================================================
// Build loop: 10-piece (m10), requires completed 9B as a fixed dependency.
// ============================================================

static void build_tb10(
    const PartitionIndex& pi,
    const MaterialSpec& m10,
    const MaterialSpec& dep9b_mat,
    const int dep9b_iter)
{
    const int p0 = std::max(0, g_cfg.start_partition);
    const int p1 = std::min(kNumPartitions, g_cfg.end_partition);

    const fs::path base10 = out_root_tb10_base();
    const fs::path root10 = with_part_range_subdir(base10, p0, p1);
    fs::create_directories(root10);

    const fs::path root9_base = out_root_tb9_base(); // dependency is always the full TB9 root.

    const int last_complete_10 = find_last_complete_iter(root10);
    cleanup_incomplete_iters(root10, last_complete_10);

    int start_iter = last_complete_10 + 1;
    if (start_iter < 1) start_iter = 1;

    if (start_iter > g_cfg.max_depth) {
        tbutil::log_line("[TB10] already complete up to max_depth=", g_cfg.max_depth);
        return;
    }

    tbutil::log_line("[TB10] resume last_complete=", last_complete_10, " start_iter=", start_iter,
        " parts=[", p0, ",", p1, ") root=", root10.string(),
        " dep9b_iter=", dep9b_iter, " (", (root9_base / iter_dir_name(dep9b_iter)).string(), ")");

    PartitionTableCache cache10(root10, m10);
    PartitionTableCache cache9b_dep(root9_base, dep9b_mat);
    cache9b_dep.set_iter(dep9b_iter);

    for (int depth = start_iter; depth <= g_cfg.max_depth; ++depth) {
        const auto t_iter0 = tbutil::Clock::now();
        const int prev = depth - 1;

        tbutil::log_line("[TB10] begin depth=", depth);

        cache10.set_iter(prev);

        const fs::path out_iter_dir = root10 / iter_dir_name(depth);
        if (fs::exists(out_iter_dir)) {
            tbutil::log_line("[TB10] removing existing out dir (partial?): ", out_iter_dir.string());
            std::error_code ec;
            fs::remove_all(out_iter_dir, ec);
            if (ec) throw std::runtime_error("failed to remove_all: " + out_iter_dir.string());
        }
        fs::create_directories(out_iter_dir);

        for (int pid = p0; pid < p1; ++pid) {
            const auto t_part0 = tbutil::Clock::now();
            const std::uint16_t part = (std::uint16_t)pid;
            const PartitionMeta& pm = pi.meta[(size_t)part];

            if (g_cfg.verbose && (pid % 10 == 0)) {
                tbutil::log_line("[TB10] depth=", depth, " part=", pid, "/", (p1 - 1),
                    " cache10(parts=", cache10.cached_parts(), ",bytes=", cache10.bytes_loaded(), ")",
                    " cache9b(parts=", cache9b_dep.cached_parts(), ",bytes=", cache9b_dep.bytes_loaded(), ")");
            }

            const auto cur10_sp = cache10.get(part);
            const auto& cur10 = *cur10_sp;

            DepTables dep10{};
            DepTables dep9b{};

            if (prev > 0) {
                dep10 = make_dep_tables_from_cache(cache10, m10, pm, part, &cur10);
            }
            if (depth > 1) {
                // depth==1 does not consult capture-blue tables for our materials (ob>1).
                dep9b = make_dep_tables_from_cache(cache9b_dep, dep9b_mat, pm, UINT16_MAX, nullptr);
            }

            auto next10 = compute_next_partition_openmp(
                pi, m10, depth, part, cur10,
                m10, dep10.views,
                CapBlueMode::ToPartitioned,
                &dep9b_mat, &dep9b.views,
                nullptr,
                g_cfg.omp_chunk);

            const fs::path fn = part_file_path(root10, m10, depth, part);
            write_zstd_file_atomic(fn, next10.data(), m10.entries_per_partition, g_cfg.zstd_level, g_cfg.zstd_workers);

            // Working-set eviction:
            cache10.retain_only(pm.child_parts.data(), pm.child_parts_n);
            cache9b_dep.retain_only(pm.child_parts.data(), pm.child_parts_n);

            if (g_cfg.verbose && (pid % 10 == 0)) {
                const double sec = std::chrono::duration<double>(tbutil::Clock::now() - t_part0).count();
                tbutil::log_line("[TB10] depth=", depth, " part=", pid, " done (", std::fixed, std::setprecision(3), sec, "s)");
            }
        }

        mark_iter_complete(out_iter_dir);

        if (depth >= 3) {
            delete_iter_dir_if_exists(root10, depth - 2);
        }

        const double sec_iter = std::chrono::duration<double>(tbutil::Clock::now() - t_iter0).count();
        tbutil::log_line("[TB10] done depth=", depth, " (", std::fixed, std::setprecision(3), sec_iter, "s)");
    }

    tbutil::log_line("[TB10] finished up to depth=", g_cfg.max_depth);
}

// ============================================================
// Main build dispatcher
// ============================================================

static void build_all() {
    // Materials
    const MaterialSpec m9a = make_material(MatKind::M9A);
    const MaterialSpec m9b = make_material(MatKind::M9B);
    const MaterialSpec m10 = make_material(MatKind::M10);

    tbutil::log_line("[INFO] entries/partition: 9=", m9a.entries_per_partition, " 10=", m10.entries_per_partition);
    #ifdef _OPENMP
    tbutil::log_line("[OMP] max_threads=", omp_get_max_threads(), " num_procs=", omp_get_num_procs());
#else
    tbutil::log_line("[OMP] OpenMP disabled at compile time.");
#endif

    // Build partition metadata.
    tbutil::log_line("[INIT] building partition index...");
    const PartitionIndex pi = build_partition_index();
    tbutil::log_line("[INIT] partition index done");

    // Load <=8 deps (only needed for TB9).
    LowerDep dep_2141;
    LowerDep dep_3131;
    if (g_cfg.build_9) {
        dep_2141 = load_lower_dep_bin(g_cfg.dep_dir, Count4{ 2,1,4,1 });
        dep_3131 = load_lower_dep_bin(g_cfg.dep_dir, Count4{ 3,1,3,1 });
    }

    if (g_cfg.build_9) {
        build_tb9(pi, m9a, m9b, dep_2141, dep_3131);
    }

    if (g_cfg.build_10) {
        // TB10 depends on a completed TB9 (9B) as a fixed lookup table.
        const fs::path root9 = out_root_tb9_base();
        const int last9 = find_last_complete_iter(root9);
        if (last9 < g_cfg.max_depth) {
            std::ostringstream oss;
            oss << "TB10 requires TB9 (tb9/) to be completed up to at least max_depth=" << g_cfg.max_depth << ".\n"
                << "Detected tb9 last_complete=" << last9 << ".\n"
                << "Run: ./geister_perfect_information_tb_9_10 --only-9 --out " << g_cfg.out_dir.string()
                << " --max-depth " << g_cfg.max_depth;
            throw std::runtime_error(oss.str());
        }

        build_tb10(pi, m10, m9b, last9);
    }

    tbutil::log_line("[DONE] all requested builds finished.");
}


// ============================================================
// main
// ============================================================

int main(int argc, char** argv) {
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg = argv[i];

            auto require_value = [&](const char* opt) -> std::string_view {
                if (i + 1 >= argc) {
                    throw std::runtime_error(std::string("missing value for ") + opt);
                }
                return std::string_view(argv[++i]);
            };

            if (arg == "-h" || arg == "--help") {
                print_usage(argv[0]);
                return 0;
            }
            else if (arg == "--out") {
                g_cfg.out_dir = fs::path(std::string(require_value("--out")));
            }
            else if (arg == "--dep") {
                g_cfg.dep_dir = fs::path(std::string(require_value("--dep")));
            }
            else if (arg == "--max-depth") {
                int v = 0;
                if (!tbutil::parse_int(require_value("--max-depth"), v) || v <= 0 || v > 210) {
                    throw std::runtime_error("invalid --max-depth");
                }
                g_cfg.max_depth = v;
            }
            else if (arg == "--zstd-level") {
                int v = 0;
                if (!tbutil::parse_int(require_value("--zstd-level"), v)) {
                    throw std::runtime_error("invalid --zstd-level");
                }
                g_cfg.zstd_level = v;
            }
            else if (arg == "--zstd-workers") {
                int v = 0;
                if (!tbutil::parse_int(require_value("--zstd-workers"), v) || v < 0) {
                    throw std::runtime_error("invalid --zstd-workers");
                }
                g_cfg.zstd_workers = v;
            }
            else if (arg == "--omp-chunk") {
                int v = 0;
                if (!tbutil::parse_int(require_value("--omp-chunk"), v) || v <= 0) {
                    throw std::runtime_error("invalid --omp-chunk");
                }
                g_cfg.omp_chunk = v;
            }
            else if (arg == "--start-part") {
                int v = 0;
                if (!tbutil::parse_int(require_value("--start-part"), v) || v < 0 || v > kNumPartitions) {
                    throw std::runtime_error("invalid --start-part");
                }
                g_cfg.start_partition = v;
            }
            else if (arg == "--end-part") {
                int v = 0;
                if (!tbutil::parse_int(require_value("--end-part"), v) || v < 0 || v > kNumPartitions) {
                    throw std::runtime_error("invalid --end-part");
                }
                g_cfg.end_partition = v;
            }
            else if (arg == "--only-9") {
                g_cfg.build_9 = true;
                g_cfg.build_10 = false;
            }
            else if (arg == "--only-10") {
                g_cfg.build_9 = false;
                g_cfg.build_10 = true;
            }
            else {
                throw std::runtime_error(std::string("unknown option: ") + std::string(arg));
            }
        }

        if (g_cfg.start_partition < 0) g_cfg.start_partition = 0;
        if (g_cfg.end_partition > kNumPartitions) g_cfg.end_partition = kNumPartitions;
        if (g_cfg.end_partition < g_cfg.start_partition) g_cfg.end_partition = g_cfg.start_partition;

        tbutil::log_line("[CFG] out=", g_cfg.out_dir.string(),
            " dep=", g_cfg.dep_dir.string(),
            " max_depth=", g_cfg.max_depth,
            " zstd(level=", g_cfg.zstd_level, ",workers=", g_cfg.zstd_workers, ")",
            " omp_chunk=", g_cfg.omp_chunk,
            " parts=[", g_cfg.start_partition, ",", g_cfg.end_partition, ")",
            " build9=", (g_cfg.build_9 ? "yes" : "no"),
            " build10=", (g_cfg.build_10 ? "yes" : "no"));

        build_all();
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
        return 1;
    }
}
