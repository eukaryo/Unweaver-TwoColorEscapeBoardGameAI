// geister_perfect_information_tb_9_10_repack_obsblk_blocked2.cpp
//
// Blocked repack (I/O-friendly) with *OOM-resistant* output flushing:
//   Convert geister_perfect_information_tb_9_10.cpp outputs (630 partitioned .zst files)
//   into a single uncompressed *_obsblk.bin per material, but avoid random disk writes.
//
// Why this variant exists:
//   A 32GiB RAM chunk + a large src buffer (up to 1.27GiB) fits in 64GB RAM, BUT
//   sequential write() into a normal file uses the kernel page cache and can create
//   tens of GiB of *dirty* cache pages. If dirty pages from the previous chunk are
//   still pending when the next chunk starts writing, the system can hit severe
//   memory pressure and trigger the OOM killer (sometimes killing tmux).
//
// This version can (optionally, default ON) do:
//   - flush written data per chunk (Linux: sync_file_range if available, else fdatasync)
//   - advise the kernel to drop cached pages for the written range (posix_fadvise)
//
// Requirements:
//   - BMI2 (_pdep_u64/_pext_u64): compile with -mbmi2
//   - zstd: link with -lzstd
//   - OpenMP optional but recommended: compile with -fopenmp
//
// Default chunk size: 32 GiB.

#define _GNU_SOURCE 1

#include <array>
#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <immintrin.h>
#include <zstd.h>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#ifdef _OPENMP
#include <omp.h>
#endif

import geister_core;
import geister_rank;
import geister_rank_triplet;
import geister_rank_obsblk;

namespace fs = std::filesystem;

namespace {

static constexpr const char* kMagic = "GSTB";
static constexpr int kNumPartitions = 18 * 35; // 630

// 6x6 board mask (8x8 embedded)
static constexpr std::uint64_t kBoardMask64 = bb_board;

// Left half (columns A-C)
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

[[nodiscard]] inline std::uint64_t pdep36(std::uint64_t pat36) noexcept {
    return _pdep_u64(pat36, kBoardMask64);
}

// -----------------------------------------------------------------------------
// Mirror LR helper
// -----------------------------------------------------------------------------
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
}

// -----------------------------------------------------------------------------
// Small utilities
// -----------------------------------------------------------------------------

[[nodiscard]] inline std::uint64_t comb_u64(int n, int k) {
    if (k < 0 || k > n) return 0;
    k = std::min(k, n - k);
    unsigned __int128 r = 1;
    for (int i = 1; i <= k; ++i) {
        r = (r * static_cast<unsigned __int128>(n - k + i)) / static_cast<unsigned __int128>(i);
    }
    if (r > (unsigned __int128)std::numeric_limits<std::uint64_t>::max()) {
        throw std::runtime_error("comb_u64 overflow (unexpected for n<=36)");
    }
    return (std::uint64_t)r;
}

// Next higher integer with same popcount ("snoob").
[[nodiscard]] inline std::uint64_t next_combination(std::uint64_t x) noexcept {
    const std::uint64_t c = x & (~x + 1);
    const std::uint64_t r = x + c;
    const std::uint64_t ones = (((r ^ x) >> 2) / c);
    return r | ones;
}

[[nodiscard]] inline fs::path tmp_path_for(const fs::path& filename) {
    fs::path tmp = filename;
    tmp += ".tmp";
    return tmp;
}

inline void replace_by_rename_best_effort(const fs::path& tmp, const fs::path& dst) {
    std::error_code ec;
    fs::rename(tmp, dst, ec);
    if (!ec) return;

    std::error_code ec2;
    fs::remove(dst, ec2);
    fs::rename(tmp, dst, ec);
    if (ec) {
        std::error_code ec3;
        fs::remove(tmp, ec3);
        throw std::runtime_error(std::string("failed to rename tmp file: ") + ec.message());
    }
}

[[nodiscard]] inline double to_gib(std::uint64_t bytes) noexcept {
    return static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
}

// -----------------------------------------------------------------------------
// Combinadic unrank in colex order for up to P<=36
// -----------------------------------------------------------------------------

static consteval auto make_comb36() {
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
static constexpr auto COMB36 = make_comb36();

[[nodiscard]] inline std::uint64_t unrank_patterns_colex_upto36(std::uint64_t r, int P, int S) {
    if (S == 0) return 0;
    if (P < 0 || P > 36 || S < 0 || S > P) {
        throw std::runtime_error("unrank_patterns_colex: invalid P/S");
    }
    if (r >= COMB36[P][S]) {
        throw std::runtime_error("unrank_patterns_colex: r out of range");
    }
    std::uint64_t x = 0;
    int p = P;
    for (int i = S; i >= 1; --i) {
        int lo = i - 1;
        int hi = p;
        while (lo + 1 < hi) {
            const int mid = (lo + hi) >> 1;
            if (COMB36[mid][i] <= r) lo = mid; else hi = mid;
        }
        p = lo;
        x |= (1ULL << p);
        r -= COMB36[p][i];
    }
    return x;
}

// -----------------------------------------------------------------------------
// Zstd streaming decompressor reused across many files
// -----------------------------------------------------------------------------

class ZstdExactReader {
public:
    ZstdExactReader() {
        dctx_ = ZSTD_createDCtx();
        if (!dctx_) throw std::runtime_error("ZSTD_createDCtx failed");
        in_buf_.resize(ZSTD_DStreamInSize());
        out_buf_.resize(ZSTD_DStreamOutSize());
    }
    ~ZstdExactReader() {
        if (dctx_) ZSTD_freeDCtx(dctx_);
    }
    ZstdExactReader(const ZstdExactReader&) = delete;
    ZstdExactReader& operator=(const ZstdExactReader&) = delete;

    void decompress_file_exact(const fs::path& filename, std::uint8_t* dst, std::uint64_t expected_size) {
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs) {
            throw std::runtime_error("failed to open for read: " + filename.string());
        }

        ZSTD_DCtx_reset(dctx_, ZSTD_reset_session_only);

        std::uint64_t written = 0;
        ZSTD_inBuffer input{ in_buf_.data(), 0, 0 };
        bool done = false;

        while (!done) {
            if (input.pos == input.size) {
                ifs.read(in_buf_.data(), (std::streamsize)in_buf_.size());
                const std::streamsize got = ifs.gcount();
                if (got <= 0) {
                    input = { in_buf_.data(), 0, 0 };
                }
                else {
                    input = { in_buf_.data(), (size_t)got, 0 };
                }
            }

            ZSTD_outBuffer output{ out_buf_.data(), out_buf_.size(), 0 };
            const size_t ret = ZSTD_decompressStream(dctx_, &output, &input);
            if (ZSTD_isError(ret)) {
                throw std::runtime_error(std::string("ZSTD_decompressStream failed: ") + ZSTD_getErrorName(ret));
            }

            if (output.pos) {
                if (written + output.pos > expected_size) {
                    throw std::runtime_error("decompressed data exceeds expected_size");
                }
                std::memcpy(dst + (std::size_t)written, out_buf_.data(), output.pos);
                written += (std::uint64_t)output.pos;
            }

            if (ret == 0) {
                done = true;
            }
            else {
                if (ifs.eof() && input.pos == input.size) {
                    break;
                }
            }
        }

        if (written != expected_size) {
            std::ostringstream oss;
            oss << "decompressed size mismatch: got=" << written << " expected=" << expected_size
                << " file=" << filename.string();
            throw std::runtime_error(oss.str());
        }
    }

private:
    ZSTD_DCtx* dctx_ = nullptr;
    std::vector<char> in_buf_;
    std::vector<char> out_buf_;
};

// -----------------------------------------------------------------------------
// POSIX write + flush helpers
// -----------------------------------------------------------------------------

#if defined(__unix__) || defined(__APPLE__)

inline void write_all_fd(int fd, const std::uint8_t* data, std::uint64_t nbytes, std::uint32_t step_mib) {
    std::uint64_t off = 0;
    const std::uint64_t step = (std::uint64_t)step_mib * (1ULL << 20);
    if (step == 0) throw std::runtime_error("write step must be >0");
    while (off < nbytes) {
        const std::size_t cur = (std::size_t)std::min<std::uint64_t>(nbytes - off, step);
        const ssize_t w = ::write(fd, data + off, cur);
        if (w < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("write failed: ") + std::strerror(errno));
        }
        if (w == 0) {
            throw std::runtime_error("write returned 0 (unexpected)");
        }
        off += (std::uint64_t)w;
    }
}

enum class FlushMode : std::uint8_t {
    None = 0,
    Fdatasync = 1,
    SyncFileRange = 2,
};

inline void flush_and_drop_cache(int fd, std::uint64_t offset, std::uint64_t len, FlushMode mode) {
    if (mode == FlushMode::None) return;

#if defined(__linux__) && defined(SYNC_FILE_RANGE_WRITE) && defined(SYNC_FILE_RANGE_WAIT_BEFORE) && defined(SYNC_FILE_RANGE_WAIT_AFTER)
    if (mode == FlushMode::SyncFileRange) {
        // Push the written range to disk, then wait for completion.
        if (::sync_file_range(fd, (off_t)offset, (off_t)len, SYNC_FILE_RANGE_WRITE) != 0) {
            throw std::runtime_error(std::string("sync_file_range(WRITE) failed: ") + std::strerror(errno));
        }
        const int flags = SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER;
        if (::sync_file_range(fd, (off_t)offset, (off_t)len, flags) != 0) {
            throw std::runtime_error(std::string("sync_file_range(WAIT) failed: ") + std::strerror(errno));
        }
    } else
#endif
    {
        if (::fdatasync(fd) != 0) {
            throw std::runtime_error(std::string("fdatasync failed: ") + std::strerror(errno));
        }
    }

#if defined(POSIX_FADV_DONTNEED)
    // Hint the kernel that we won't reuse cache pages for this range.
    // This helps to prevent page cache from accumulating across chunks.
    (void)::posix_fadvise(fd, (off_t)offset, (off_t)len, POSIX_FADV_DONTNEED);
#endif
}

#endif

// -----------------------------------------------------------------------------
// Material specs and filenames
// -----------------------------------------------------------------------------

enum class MatKind : std::uint8_t {
    M9A = 0, // (4,1,3,1)
    M9B = 1, // (3,1,4,1)
    M10 = 2, // (4,1,4,1)
};

struct MaterialSpec {
    MatKind kind{};
    int pb = 0;
    int pr = 1;
    int ob = 0;
    int or_ = 1;
    std::string tag;
    std::uint64_t entries_per_partition = 0;
    std::uint64_t pb_count = 0;
    std::uint64_t ob_count = 0;

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
        m.pb = 4; m.ob = 3; m.tag = "m9_pb4ob3"; break;
    case MatKind::M9B:
        m.pb = 3; m.ob = 4; m.tag = "m9_pb3ob4"; break;
    case MatKind::M10:
        m.pb = 4; m.ob = 4; m.tag = "m10_pb4ob4"; break;
    default:
        throw std::runtime_error("unknown MatKind");
    }
    m.pb_count = comb_u64(34, m.pb);
    m.ob_count = comb_u64(34 - m.pb, m.ob);
    m.entries_per_partition = m.pb_count * m.ob_count;
    return m;
}

[[nodiscard]] static std::string iter_dir_name(int iter) {
    std::ostringstream oss;
    oss << "iter_" << std::setw(3) << std::setfill('0') << iter;
    return oss.str();
}

[[nodiscard]] static std::string part_file_name(const MaterialSpec& m, int iter, int part) {
    std::ostringstream oss;
    oss << m.tag
        << "_i" << std::setw(3) << std::setfill('0') << part
        << "_n" << std::setw(3) << std::setfill('0') << iter
        << "_" << kMagic
        << "_" << m.entries_per_partition
        << ".zst";
    return oss.str();
}

[[nodiscard]] static std::string out_obsblk_bin_name(const Count4& c) {
    const std::uint16_t id = rank_material_configuration(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
    std::ostringstream oss;
    oss << "id" << std::setw(3) << std::setfill('0') << (int)id
        << "_pb" << (int)c.pop_pb
        << "pr" << (int)c.pop_pr
        << "ob" << (int)c.pop_ob
        << "or" << (int)c.pop_or
        << "_obsblk.bin";
    return oss.str();
}

// -----------------------------------------------------------------------------
// Partition meta: part -> (rp_full, ro_full)
// -----------------------------------------------------------------------------

struct PartMeta {
    int rp_full = -1;
    int ro_full = -1;
    std::uint64_t rp_bb64 = 0;
    std::uint64_t ro_bb64 = 0;
    std::uint64_t rem_mask64 = 0; // remaining squares after removing both reds
};

static std::array<int, 18> build_left_to_full() {
    std::array<int, 18> out{};
    int n = 0;
    for (int full = 0; full < 36; ++full) {
        const std::uint64_t bb = pdep36(1ULL << full);
        if ((bb & kLeftMask64) != 0) {
            out[(std::size_t)n++] = full;
        }
    }
    if (n != 18) throw std::runtime_error("left_to_full must have 18 squares");
    return out;
}

static PartMeta part_meta_from_id(int part, const std::array<int, 18>& left_to_full) {
    if (part < 0 || part >= kNumPartitions) throw std::runtime_error("part out of range");
    const int rpL = part / 35;
    const int roOrd = part % 35;
    const int rp_full = left_to_full[(std::size_t)rpL];
    // ro_full enumerates full indices [0..35] excluding rp_full.
    const int ro_full = roOrd + ((roOrd >= rp_full) ? 1 : 0);

    PartMeta pm{};
    pm.rp_full = rp_full;
    pm.ro_full = ro_full;
    pm.rp_bb64 = pdep36(1ULL << rp_full);
    pm.ro_bb64 = pdep36(1ULL << ro_full);
    const std::uint64_t all36 = (1ULL << 36) - 1;
    const std::uint64_t rem36 = all36 ^ (1ULL << rp_full) ^ (1ULL << ro_full);
    pm.rem_mask64 = pdep36(rem36);
    return pm;
}

// -----------------------------------------------------------------------------
// Core: fill one chunk in RAM by scanning all partitions
// -----------------------------------------------------------------------------

static void fill_chunk_for_partition(
    std::uint8_t* chunk,
    std::uint64_t chunk_base,
    std::uint64_t chunk_len,
    const std::uint8_t* src,
    const MaterialSpec& m,
    const PartMeta& pm)
{
    const int pb = m.pb;
    const int ob = m.ob;
    const int k = ob + 1; // opponent pieces total (ob + or(=1)) -> inner_domain = C(k,1)=k
    const std::uint64_t pb_count = m.pb_count;
    const std::uint64_t ob_count = m.ob_count;

    const std::uint64_t pr_bb64 = pm.rp_bb64;
    const std::uint64_t or_bb64 = pm.ro_bb64;
    const std::uint64_t or_mirror = mirror::mirror_lr_u64(or_bb64);
    const std::uint64_t rem_mask64 = pm.rem_mask64;

    // Parallelize over pb-rank. Each pb-rank covers a contiguous slice in src.
    #pragma omp parallel for schedule(static) if(pb_count >= 1024)
    for (std::int64_t i_pb_signed = 0; i_pb_signed < (std::int64_t)pb_count; ++i_pb_signed) {
        const std::uint64_t i_pb = (std::uint64_t)i_pb_signed;
        const std::uint64_t src_base = i_pb * ob_count;

        const std::uint64_t pb_pat34 = unrank_patterns_colex_upto36(i_pb, 34, pb);
        const std::uint64_t pb_bb64 = _pdep_u64(pb_pat34, rem_mask64);
        const std::uint64_t rem2_mask64 = rem_mask64 ^ pb_bb64;

        // Canonicalization decision inside rank_triplet_canon for this domain:
        // pr=1 is fixed on left in the INPUT format, so cmp never ties.
        // Therefore mirrored iff pb_left >= pb_right.
        const int pb_left = std::popcount(pb_bb64 & kLeftMask64);
        const int pb_right = pb - pb_left;
        const bool mirrored = (pb_left >= pb_right);
        const std::uint64_t inner_mask = (mirrored ? (or_mirror - 1ULL) : (or_bb64 - 1ULL));

        // Enumerate ob combinations in colex order over rem2_mask64 (size 34-pb).
        std::uint64_t ob_patN = (ob == 0) ? 0ULL : ((1ULL << ob) - 1ULL);
        for (std::uint64_t i_ob = 0; i_ob < ob_count; ++i_ob) {
            const std::uint8_t v = src[src_base + i_ob];

            const std::uint64_t ob_bb64 = _pdep_u64(ob_patN, rem2_mask64);
            const std::uint64_t U_bb64 = ob_bb64 | or_bb64;

            const std::uint64_t outer = rank_triplet_canon(pb_bb64, pr_bb64, U_bb64);

            std::uint64_t inner = 0;
            if (!mirrored) {
                inner = (std::uint64_t)std::popcount(ob_bb64 & inner_mask);
            }
            else {
                const std::uint64_t ob_m = mirror::mirror_lr_u64(ob_bb64);
                inner = (std::uint64_t)std::popcount(ob_m & inner_mask);
            }

            const std::uint64_t dst_idx = outer * (std::uint64_t)k + inner;
            if (dst_idx >= chunk_base && dst_idx < chunk_base + chunk_len) {
                chunk[dst_idx - chunk_base] = v;
            }

            if (i_ob + 1 < ob_count) {
                ob_patN = next_combination(ob_patN);
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Repack one material (blocked)
// -----------------------------------------------------------------------------

static void repack_one_material_blocked(
    const fs::path& in_iter_dir,
    const fs::path& out_dir,
    int iter,
    const MaterialSpec& m,
    const std::array<int, 18>& left_to_full,
    std::uint64_t chunk_bytes,
    bool verify_filled,
    std::uint32_t write_step_mib,
    FlushMode flush_mode)
{
    const Count4 c = m.as_count4();
    const std::uint16_t id = rank_material_configuration(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);

    const std::uint64_t out_entries = obsblk_states_for_counts(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
    const std::uint64_t expected_old = total_states_for_counts(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
    if (out_entries != expected_old) {
        std::ostringstream oss;
        oss << "obsblk_states_for_counts != total_states_for_counts for ("
            << (int)c.pop_pb << "," << (int)c.pop_pr << "," << (int)c.pop_ob << "," << (int)c.pop_or << ")"
            << " obsblk=" << out_entries << " old=" << expected_old
            << "\nThis blocked repacker assumes bijection (no mirror-fixed states).";
        throw std::runtime_error(oss.str());
    }

    fs::create_directories(out_dir);
    const fs::path out_file = out_dir / out_obsblk_bin_name(c);
    const fs::path tmp = tmp_path_for(out_file);

    std::cout << "[repack9_10_blocked2] material=" << m.tag
        << " id=" << id
        << " out_entries=" << out_entries
        << " (" << std::fixed << std::setprecision(2) << to_gib(out_entries) << " GiB)"
        << " chunk=" << std::fixed << std::setprecision(2) << to_gib(chunk_bytes) << " GiB"
        << " write_step=" << write_step_mib << " MiB"
        << " flush=" << (flush_mode == FlushMode::None ? "none" : (flush_mode == FlushMode::SyncFileRange ? "sync_file_range" : "fdatasync"))
        << "\n"
        << "  input_iter_dir=" << in_iter_dir.string() << "\n"
        << "  output=" << out_file.string() << "\n";

#if !defined(__unix__) && !defined(__APPLE__)
    throw std::runtime_error("This tool currently requires POSIX file APIs for huge sequential writes.");
#else
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error(std::string("open output failed: ") + std::strerror(errno));
    }

    if (::ftruncate(fd, (off_t)out_entries) != 0) {
        ::close(fd);
        throw std::runtime_error(std::string("ftruncate failed: ") + std::strerror(errno));
    }
    if (::lseek(fd, 0, SEEK_SET) < 0) {
        ::close(fd);
        throw std::runtime_error(std::string("lseek failed: ") + std::strerror(errno));
    }

    if (chunk_bytes == 0) {
        ::close(fd);
        throw std::runtime_error("chunk size must be >0");
    }
    if (chunk_bytes > (1ULL << 35)) {
        ::close(fd);
        throw std::runtime_error("chunk size too large");
    }

    std::vector<std::uint8_t> chunk;
    chunk.resize((std::size_t)std::min<std::uint64_t>(chunk_bytes, out_entries));

    std::vector<std::uint8_t> src_buf;
    src_buf.resize((std::size_t)m.entries_per_partition);
    ZstdExactReader zstd;

    const int total_chunks = (int)((out_entries + chunk_bytes - 1) / chunk_bytes);

    for (int chunk_idx = 0; chunk_idx < total_chunks; ++chunk_idx) {
        const std::uint64_t chunk_base = (std::uint64_t)chunk_idx * chunk_bytes;
        const std::uint64_t chunk_len = std::min<std::uint64_t>(chunk_bytes, out_entries - chunk_base);
        if (chunk.size() < (std::size_t)chunk_len) chunk.resize((std::size_t)chunk_len);

        std::memset(chunk.data(), 0xFF, (std::size_t)chunk_len);

        std::cout << "  [chunk " << (chunk_idx + 1) << "/" << total_chunks << "]"
            << " base=" << chunk_base
            << " len=" << chunk_len
            << " (" << std::fixed << std::setprecision(2) << to_gib(chunk_len) << " GiB)\n";

        for (int part = 0; part < kNumPartitions; ++part) {
            const PartMeta pm = part_meta_from_id(part, left_to_full);
            const fs::path in_file = in_iter_dir / part_file_name(m, iter, part);

            zstd.decompress_file_exact(in_file, src_buf.data(), m.entries_per_partition);

            fill_chunk_for_partition(
                chunk.data(), chunk_base, chunk_len,
                src_buf.data(), m, pm);
        }

        if (verify_filled) {
            std::uint64_t bad = 0;
            for (std::size_t i = 0; i < (std::size_t)chunk_len; ++i) {
                bad += (chunk[i] == 0xFF) ? 1ULL : 0ULL;
            }
            if (bad != 0) {
                ::close(fd);
                throw std::runtime_error("chunk verification failed: some bytes were not filled (sentinel remains)");
            }
        }

        // Sequential write (but throttle the write granularity to avoid huge dirty spikes).
        write_all_fd(fd, chunk.data(), chunk_len, write_step_mib);

        // Important: flush and drop cache for the range we just wrote.
        // This prevents dirty page cache accumulation across chunks (common OOM trigger on 64GB machines).
        flush_and_drop_cache(fd, chunk_base, chunk_len, flush_mode);
    }

    ::close(fd);
    replace_by_rename_best_effort(tmp, out_file);
    std::cout << "[repack9_10_blocked2] done: " << out_file.string() << "\n";
#endif
}

// -----------------------------------------------------------------------------
// CLI
// -----------------------------------------------------------------------------

struct Args {
    fs::path in_root = "tb_9_10";
    fs::path out_dir = ".";
    int iter = 210;
    bool do_9 = true;
    bool do_10 = true;
    std::uint64_t chunk_gib = 32;
    bool verify = false;
    int threads = 0;
    std::uint32_t write_step_mib = 256; // smaller than 1GiB to reduce dirty spikes
    FlushMode flush_mode = FlushMode::SyncFileRange; // Linux if available, else auto-fallback
};

static void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0
        << " [--in DIR] [--out DIR] [--iter N] [--only-9] [--only-10] [--chunk-gib G] [--verify] [--threads T]"
        << " [--write-step-mib M] [--flush none|fdatasync|sync_range]\n"
        << "  --in DIR             base directory that contains tb9/ and tb10/ (default: tb_9_10)\n"
        << "  --out DIR            output directory for *_obsblk.bin (default: current dir)\n"
        << "  --iter N             iteration depth (e.g., 210) (default: 210)\n"
        << "  --only-9             repack only the 9-piece materials (m9_pb4ob3, m9_pb3ob4)\n"
        << "  --only-10            repack only the 10-piece material (m10_pb4ob4)\n"
        << "  --chunk-gib G        RAM chunk size in GiB (default: 32)\n"
        << "  --verify             (slow) verify each chunk has no sentinel bytes left\n"
        << "  --threads T          set OpenMP threads (default: use OMP_NUM_THREADS)\n"
        << "  --write-step-mib M   granularity of write() calls (default: 256 MiB)\n"
        << "  --flush MODE         per-chunk flush+cache-drop: none | fdatasync | sync_range (default: sync_range)\n";
}

static Args parse_args(int argc, char** argv) {
    Args a{};
    for (int i = 1; i < argc; ++i) {
        const std::string_view s(argv[i]);
        auto need_value = [&](std::string_view opt) -> std::string_view {
            if (i + 1 >= argc) {
                throw std::runtime_error(std::string("missing value for ") + std::string(opt));
            }
            return std::string_view(argv[++i]);
        };

        if (s == "--in") {
            a.in_root = fs::path(std::string(need_value(s)));
        }
        else if (s == "--out") {
            a.out_dir = fs::path(std::string(need_value(s)));
        }
        else if (s == "--iter") {
            a.iter = std::stoi(std::string(need_value(s)));
        }
        else if (s == "--only-9") {
            a.do_9 = true;
            a.do_10 = false;
        }
        else if (s == "--only-10") {
            a.do_9 = false;
            a.do_10 = true;
        }
        else if (s == "--chunk-gib") {
            a.chunk_gib = (std::uint64_t)std::stoull(std::string(need_value(s)));
            if (a.chunk_gib == 0) throw std::runtime_error("chunk-gib must be >=1");
        }
        else if (s == "--verify") {
            a.verify = true;
        }
        else if (s == "--threads") {
            a.threads = std::stoi(std::string(need_value(s)));
            if (a.threads < 0) throw std::runtime_error("threads must be >=0");
        }
        else if (s == "--write-step-mib") {
            const auto v = (std::uint64_t)std::stoull(std::string(need_value(s)));
            if (v == 0 || v > (1ULL << 20)) throw std::runtime_error("write-step-mib out of range");
            a.write_step_mib = (std::uint32_t)v;
        }
        else if (s == "--flush") {
            const auto v = need_value(s);
            if (v == "none") a.flush_mode = FlushMode::None;
            else if (v == "fdatasync") a.flush_mode = FlushMode::Fdatasync;
            else if (v == "sync_range") a.flush_mode = FlushMode::SyncFileRange;
            else throw std::runtime_error("unknown --flush mode");
        }
        else if (s == "--help" || s == "-h") {
            print_usage(argv[0]);
            std::exit(0);
        }
        else {
            throw std::runtime_error(std::string("unknown option: ") + std::string(s));
        }
    }
    return a;
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Args a = parse_args(argc, argv);

#ifdef _OPENMP
        if (a.threads > 0) {
            omp_set_num_threads(a.threads);
        }
#endif

        const auto left_to_full = build_left_to_full();
        const fs::path tb9_iter = a.in_root / "tb9" / iter_dir_name(a.iter);
        const fs::path tb10_iter = a.in_root / "tb10" / iter_dir_name(a.iter);
        const std::uint64_t chunk_bytes = a.chunk_gib * (1ULL << 30);

        if (a.do_9) {
            const MaterialSpec m9a = make_material(MatKind::M9A);
            const MaterialSpec m9b = make_material(MatKind::M9B);
            repack_one_material_blocked(tb9_iter, a.out_dir, a.iter, m9a, left_to_full, chunk_bytes, a.verify, a.write_step_mib, a.flush_mode);
            repack_one_material_blocked(tb9_iter, a.out_dir, a.iter, m9b, left_to_full, chunk_bytes, a.verify, a.write_step_mib, a.flush_mode);
        }
        if (a.do_10) {
            const MaterialSpec m10 = make_material(MatKind::M10);
            repack_one_material_blocked(tb10_iter, a.out_dir, a.iter, m10, left_to_full, chunk_bytes, a.verify, a.write_step_mib, a.flush_mode);
        }

        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        print_usage(argv[0]);
        return 1;
    }
}
