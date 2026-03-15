// seekable_zstd_mt.cpp
// C++20 / POSIX (mmap) implementation.
//
// Compress a single input file into "Zstandard Seekable Format":
//   [zstd frame][zstd f:contentReference[oaicite:4]{index=4}seek table as skippable frame]
//
// Fixed requirements:
//   * Compression level: 22
//   * Frame content size: 8 KiB (8192 bytes; last frame may be smaller)
//   * Seek table checksum: disabled (entries are 8 bytes: compSize + decompSize)
//   * mmap input
//   * seek table entry log is written to a temporary seeklog file (seeklog temp)
//
// Build example (clang++-20):
//   clang++-20 -std=c++20 -O3 -DNDEBUG -march=native -mtune=native -flto -pthread \
//       seekable_zstd_mt.cpp -lzstd -o seekable_zstd_mt
//
// Usage:
//   ./seekable_zstd_mt [-j THREADS] [-b BATCH_FRAMES] [-i INFLIGHT_BATCHES] input.bin output.zst
//
// Notes:
//   - This is frame-level parallelism: each 8 KiB chunk is compressed as an independent zstd frame.
//   - The seek table is appended at the end as a skippable frame.
//   - seek table checksum flag bit is 0 (no per-entry checksum).
//
// Format constants/structure are based on zstd contrib "seekable_format" documentation.

#include <zstd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <semaphore>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace fs = std::filesystem;

static constexpr uint32_t kSeekTableSkippableMagic = 0x184D2A5E;
static constexpr uint32_t kSeekableMagic = 0x8F92EAB1;

static constexpr size_t kFrameContentSize = 8 * 1024; // 8 KiB
static constexpr int kCompressionLevel = 22;

// Seek table footer size in bytes: Number_Of_Frames (4) + Descriptor (1) + Seekable_Magic_Number (4)
static constexpr uint32_t kSeekTableFooterSize = 9;

// We match the provided Python script's behavior: no checksum field in seek table entries.
// That means descriptor checksum flag bit7 = 0 and entry size = 8 bytes.
static constexpr uint8_t kSeekTableDescriptor = 0; // checksum_flag=0, reserved bits=0
static constexpr uint32_t kSeekTableEntrySize = 8;

[[noreturn]] static void die_errno(std::string_view what) {
    int e = errno;
    std::cerr << "error: " << what << ": " << std::strerror(e) << " (errno=" << e << ")\n";
    std::exit(1);
}

[[noreturn]] static void die_msg(std::string_view what) {
    std::cerr << "error: " << what << "\n";
    std::exit(1);
}

static void write_all(int fd, const void* buf, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    while (len > 0) {
        ssize_t n = ::write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            die_errno("write");
        }
        if (n == 0) {
            die_msg("write returned 0");
        }
        p += static_cast<size_t>(n);
        len -= static_cast<size_t>(n);
    }
}

static void read_all(int fd, void* buf, size_t len) {
    uint8_t* p = static_cast<uint8_t*>(buf);
    while (len > 0) {
        ssize_t n = ::read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            die_errno("read");
        }
        if (n == 0) {
            die_msg("unexpected EOF while reading seeklog");
        }
        p += static_cast<size_t>(n);
        len -= static_cast<size_t>(n);
    }
}

static uint32_t u32_le(uint32_t v) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return v;
#else
    return ((v & 0x000000FFu) << 24) |
           ((v & 0x0000FF00u) << 8)  |
           ((v & 0x00FF0000u) >> 8)  |
           ((v & 0xFF000000u) >> 24);
#endif
}

static void append_u32_le(std::vector<uint8_t>& out, uint32_t v) {
    uint32_t le = u32_le(v);
    const size_t pos = out.size();
    out.resize(pos + 4);
    std::memcpy(out.data() + pos, &le, 4);
}

struct Fd {
    int fd{-1};

    Fd() = default;
    explicit Fd(int f) : fd(f) {}
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;

    Fd(Fd&& other) noexcept : fd(other.fd) { other.fd = -1; }
    Fd& operator=(Fd&& other) noexcept {
        if (this != &other) {
            reset();
            fd = other.fd;
            other.fd = -1;
        }
        return *this;
    }

    ~Fd() { reset(); }

    void reset(int newfd = -1) noexcept {
        if (fd >= 0) ::close(fd);
        fd = newfd;
    }

    int get() const noexcept { return fd; }
    explicit operator bool() const noexcept { return fd >= 0; }
};

struct MMap {
    const uint8_t* data{nullptr};
    size_t size{0};

    MMap() = default;
    MMap(const MMap&) = delete;
    MMap& operator=(const MMap&) = delete;

    MMap(MMap&& other) noexcept : data(other.data), size(other.size) {
        other.data = nullptr;
        other.size = 0;
    }
    MMap& operator=(MMap&& other) noexcept {
        if (this != &other) {
            reset();
            data = other.data;
            size = other.size;
            other.data = nullptr;
            other.size = 0;
        }
        return *this;
    }

    ~MMap() { reset(); }

    void reset() noexcept {
        if (data && size) {
            ::munmap(const_cast<uint8_t*>(data), size);
        }
        data = nullptr;
        size = 0;
    }
};

template <class T>
class BlockingQueue {
public:
    void push(T v) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            q_.push_back(std::move(v));
        }
        cv_.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&]{ return !q_.empty(); });
        T v = std::move(q_.front());
        q_.pop_front();
        return v;
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<T> q_;
};

struct BatchResult {
    uint64_t batch_index{};
    uint32_t frame_count{};
    std::vector<uint32_t> comp_sizes;   // per-frame compressed sizes
    std::vector<uint32_t> decomp_sizes; // per-frame decompressed sizes (<=8192)
    std::vector<uint8_t> data;          // concatenated compressed frames
};

struct ResultItem {
    // Either a valid result, or an error message.
    std::optional<BatchResult> result;
    std::string error; // non-empty => error
};

struct Args {
    fs::path input_path;
    fs::path output_path;
    uint32_t threads = 0;
    uint32_t batch_frames = 256;
    uint32_t inflight_batches = 0;
};

static void print_usage(const char* argv0) {
    std::cerr
        << "Usage:\n"
        << "  " << argv0 << " [-j THREADS] [-b BATCH_FRAMES] [-i INFLIGHT_BATCHES] input.bin output.zst\n"
        << "\n"
        << "Fixed:\n"
        << "  level=" << kCompressionLevel << ", frame_size=" << kFrameContentSize << " bytes, seek table checksum disabled\n"
        << "\n"
        << "Options:\n"
        << "  -j THREADS          Worker threads (default: hardware_concurrency)\n"
        << "  -b BATCH_FRAMES     Frames per batch task (default: 256). Each frame is 8KiB.\n"
        << "  -i INFLIGHT_BATCHES Max batches in-flight (default: 2*THREADS, min 1)\n";
}

static bool parse_u32(std::string_view s, uint32_t& out) {
    if (s.empty()) return false;
    uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        v = v * 10 + static_cast<uint64_t>(c - '0');
        if (v > 0xFFFFFFFFu) return false;
    }
    out = static_cast<uint32_t>(v);
    return true;
}

static Args parse_args(int argc, char** argv) {
    Args a;
    if (argc < 3) {
        print_usage(argv[0]);
        std::exit(2);
    }

    int i = 1;
    while (i < argc) {
        std::string_view arg(argv[i]);
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        } else if (arg == "-j") {
            if (i + 1 >= argc) die_msg("missing value after -j");
            uint32_t v = 0;
            if (!parse_u32(argv[i + 1], v) || v == 0) die_msg("invalid -j THREADS");
            a.threads = v;
            i += 2;
        } else if (arg == "-b") {
            if (i + 1 >= argc) die_msg("missing value after -b");
            uint32_t v = 0;
            if (!parse_u32(argv[i + 1], v) || v == 0) die_msg("invalid -b BATCH_FRAMES");
            a.batch_frames = v;
            i += 2;
        } else if (arg == "-i") {
            if (i + 1 >= argc) die_msg("missing value after -i");
            uint32_t v = 0;
            if (!parse_u32(argv[i + 1], v) || v == 0) die_msg("invalid -i INFLIGHT_BATCHES");
            a.inflight_batches = v;
            i += 2;
        } else {
            break;
        }
    }

    if (i + 2 != argc) {
        print_usage(argv[0]);
        std::exit(2);
    }
    a.input_path = fs::path(argv[i]);
    a.output_path = fs::path(argv[i + 1]);

    if (a.threads == 0) {
        unsigned hc = std::thread::hardware_concurrency();
        a.threads = hc ? static_cast<uint32_t>(hc) : 1;
    }
    if (a.inflight_batches == 0) {
        a.inflight_batches = std::max<uint32_t>(1, a.threads * 2);
    }
    return a;
}

static MMap mmap_file_readonly(int fd, size_t size) {
    if (size == 0) {
        return MMap{}; // empty file: no mapping needed
    }
    void* p = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) die_errno("mmap");
    MMap mm;
    mm.data = static_cast<const uint8_t*>(p);
    mm.size = size;
    return mm;
}

static std::string make_temp_path(const fs::path& base, std::string_view suffix) {
    // base + suffix + ".XXXXXX" template for mkstemp
    // Example: /path/out.zst + ".tmp" => "/path/out.zst.tmp.XXXXXX"
    std::string tmpl = base.string();
    tmpl += std::string(suffix);
    tmpl += ".XXXXXX";
    return tmpl;
}

static Fd create_temp_file_near(const fs::path& base, std::string_view suffix, fs::path& out_path) {
    std::string tmpl = make_temp_path(base, suffix);
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');

    int fd = ::mkstemp(buf.data());
    if (fd < 0) die_errno("mkstemp");
    out_path = fs::path(buf.data());
    return Fd(fd);
}

static void safe_unlink(const fs::path& p) {
    std::error_code ec;
    fs::remove(p, ec);
}

static void safe_rename_overwrite(const fs::path& from, const fs::path& to) {
    std::error_code ec;
    fs::remove(to, ec); // ignore errors (e.g., doesn't exist)
    fs::rename(from, to); // may throw
}

int main(int argc, char** argv) {
    const Args args = parse_args(argc, argv);

    // Ensure output directory exists (best-effort).
    if (auto parent = args.output_path.parent_path(); !parent.empty()) {
        std::error_code ec;
        fs::create_directories(parent, ec);
    }

    // Open input
    Fd in_fd(::open(args.input_path.c_str(), O_RDONLY));
    if (!in_fd) die_errno("open input");

    struct stat st{};
    if (::fstat(in_fd.get(), &st) != 0) die_errno("fstat input");
    if (!S_ISREG(st.st_mode)) die_msg("input is not a regular file");

    const uint64_t input_size_u64 = static_cast<uint64_t>(st.st_size);
    if (input_size_u64 > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        die_msg("input too large to mmap on this platform");
    }
    const size_t input_size = static_cast<size_t>(input_size_u64);

    // mmap input (or leave empty mapping for size==0)
    MMap mm = mmap_file_readonly(in_fd.get(), input_size);

    // Determine frame / batch counts
    const uint64_t num_frames = (input_size_u64 + (kFrameContentSize - 1)) / kFrameContentSize;
    if (num_frames > 0xFFFFFFFFu) {
        die_msg("too many frames for seekable format (num_frames exceeds uint32)");
    }
    const uint64_t num_batches = (num_frames + (args.batch_frames - 1)) / args.batch_frames;

    // Prepare output temp and seeklog temp
    fs::path out_tmp_path;
    Fd out_fd = create_temp_file_near(args.output_path, ".outtmp", out_tmp_path);

    // Match output file permissions to input (best-effort). mkstemp() creates 0600.
    (void)::fchmod(out_fd.get(), st.st_mode & 0777);

    fs::path seeklog_path;
    // O_RDWR so we can rewind and copy at the end
    Fd seeklog_fd = create_temp_file_near(args.output_path, ".seeklog", seeklog_path);
    // mkstemp uses O_RDWR by default.

    // We'll use a semaphore to bound in-flight batches (memory usage).
    std::counting_semaphore<> inflight(static_cast<ptrdiff_t>(args.inflight_batches));

    BlockingQueue<ResultItem> results;
    std::atomic<uint64_t> next_batch{0};
    std::atomic<bool> cancel{false};

    std::mutex err_mu;
    std::string err_msg;

    auto acquire_permit = [&](std::counting_semaphore<>& sem) -> bool {
        // Cancel-aware acquisition.
        while (!cancel.load(std::memory_order_relaxed)) {
            if (sem.try_acquire_for(std::chrono::milliseconds(200))) {
                return true;
            }
        }
        return false;
    };

    auto worker_fn = [&](uint32_t /*tid*/) {
        // Per-thread zstd context
        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        if (!cctx) {
            std::lock_guard<std::mutex> lk(err_mu);
            if (err_msg.empty()) err_msg = "ZSTD_createCCtx failed";
            cancel.store(true);
            results.push(ResultItem{std::nullopt, err_msg});
            return;
        }

        auto cleanup = [&]() { ZSTD_freeCCtx(cctx); };

        try {
            while (true) {
                if (!acquire_permit(inflight)) {
                    break; // cancelled
                }

                if (cancel.load(std::memory_order_relaxed)) {
                    inflight.release();
                    break;
                }

                const uint64_t b = next_batch.fetch_add(1, std::memory_order_relaxed);
                if (b >= num_batches) {
                    inflight.release();
                    break;
                }

                const uint64_t first_frame = b * static_cast<uint64_t>(args.batch_frames);
                const uint64_t remain = num_frames - first_frame;
                const uint32_t frames_in_batch = static_cast<uint32_t>(
                    std::min<uint64_t>(remain, args.batch_frames));

                BatchResult r;
                r.batch_index = b;
                r.frame_count = frames_in_batch;
                r.comp_sizes.resize(frames_in_batch);
                r.decomp_sizes.resize(frames_in_batch);

                // Conservative reserve: worst-case for full 8KiB frames
                const size_t bound_full = ZSTD_compressBound(kFrameContentSize);
                r.data.reserve(static_cast<size_t>(frames_in_batch) * bound_full);

                for (uint32_t i = 0; i < frames_in_batch; ++i) {
                    if (cancel.load(std::memory_order_relaxed)) {
                        throw std::runtime_error("cancelled");
                    }

                    const uint64_t frame_index = first_frame + i;
                    const uint64_t off = frame_index * kFrameContentSize;
                    const uint64_t left = input_size_u64 - off;
                    const size_t src_size = static_cast<size_t>(std::min<uint64_t>(left, kFrameContentSize));
                    const uint8_t* src_ptr = (src_size == 0) ? nullptr : (mm.data + static_cast<size_t>(off));

                    // Reset context for an independent frame
                    size_t zr = ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);
                    if (ZSTD_isError(zr)) {
                        throw std::runtime_error(std::string("ZSTD_CCtx_reset: ") + ZSTD_getErrorName(zr));
                    }

                    // Fix parameters explicitly:
                    //  - compression level 22
                    //  - checksumFlag=0 (no 4-byte frame checksum)
                    zr = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, kCompressionLevel);
                    if (ZSTD_isError(zr)) {
                        throw std::runtime_error(std::string("ZSTD_CCtx_setParameter(level): ") + ZSTD_getErrorName(zr));
                    }
                    zr = ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 0);
                    if (ZSTD_isError(zr)) {
                        throw std::runtime_error(std::string("ZSTD_CCtx_setParameter(checksumFlag): ") + ZSTD_getErrorName(zr));
                    }

                    const size_t bound = ZSTD_compressBound(src_size);
                    const size_t pos = r.data.size();
                    r.data.resize(pos + bound);

                    const size_t csize = ZSTD_compress2(
                        cctx,
                        r.data.data() + pos,
                        bound,
                        src_ptr,
                        src_size);

                    if (ZSTD_isError(csize)) {
                        throw std::runtime_error(std::string("ZSTD_compress2: ") + ZSTD_getErrorName(csize));
                    }

                    r.data.resize(pos + csize);
                    r.comp_sizes[i] = static_cast<uint32_t>(csize);
                    r.decomp_sizes[i] = static_cast<uint32_t>(src_size);
                }

                // Hand off to writer. Permit is NOT released here:
                // it represents memory ownership for this batch until writer finishes.
                results.push(ResultItem{std::move(r), ""});
            }
        } catch (const std::exception& ex) {
            // Release the permit we hold for the current batch (writer won't see it).
            inflight.release();

            {
                std::lock_guard<std::mutex> lk(err_mu);
                if (err_msg.empty()) err_msg = ex.what();
            }
            cancel.store(true);
            results.push(ResultItem{std::nullopt, err_msg});
        }

        cleanup();
    };

    // Spawn workers
    std::vector<std::thread> workers;
    workers.reserve(args.threads);
    for (uint32_t t = 0; t < args.threads; ++t) {
        workers.emplace_back(worker_fn, t);
    }

    // Writer loop (in main thread)
    bool ok = true;
    std::string local_err;

    std::unordered_map<uint64_t, BatchResult> pending;
    pending.reserve(static_cast<size_t>(args.inflight_batches) * 2);

    uint64_t next_to_write = 0;

    auto write_seeklog_entries = [&](const BatchResult& r) {
        // Build a contiguous byte buffer for all entries in this batch to reduce syscalls.
        std::vector<uint8_t> buf;
        buf.reserve(static_cast<size_t>(r.frame_count) * kSeekTableEntrySize);
        for (uint32_t i = 0; i < r.frame_count; ++i) {
            append_u32_le(buf, r.comp_sizes[i]);
            append_u32_le(buf, r.decomp_sizes[i]);
        }
        write_all(seeklog_fd.get(), buf.data(), buf.size());
    };

    try {
        while (next_to_write < num_batches) {
            ResultItem item = results.pop();
            if (!item.error.empty()) {
                ok = false;
                local_err = item.error;
                cancel.store(true);
                break;
            }
            if (!item.result.has_value()) {
                ok = false;
                local_err = "internal error: empty result without error message";
                cancel.store(true);
                break;
            }

            BatchResult r = std::move(*item.result);
            pending.emplace(r.batch_index, std::move(r));

            while (true) {
                auto it = pending.find(next_to_write);
                if (it == pending.end()) break;

                const BatchResult& wr = it->second;
                if (!wr.data.empty()) {
                    write_all(out_fd.get(), wr.data.data(), wr.data.size());
                }
                write_seeklog_entries(wr);

                pending.erase(it);
                ++next_to_write;

                // Release exactly one permit per batch after writer has consumed its memory.
                inflight.release();
            }
        }
    } catch (const std::exception& ex) {
        ok = false;
        local_err = ex.what();
        cancel.store(true);
    }

    // Join workers
    for (auto& th : workers) {
        if (th.joinable()) th.join();
    }

    if (!ok) {
        // Cleanup temps
        safe_unlink(out_tmp_path);
        safe_unlink(seeklog_path);
        die_msg(local_err.empty() ? "compression failed" : local_err);
    }

    // Finalize: write seek table at end of output.
    // Verify seeklog size matches expected.
    off_t seeklog_end = ::lseek(seeklog_fd.get(), 0, SEEK_END);
    if (seeklog_end < 0) {
        safe_unlink(out_tmp_path);
        safe_unlink(seeklog_path);
        die_errno("lseek(seeklog, end)");
    }
    const uint64_t seeklog_size = static_cast<uint64_t>(seeklog_end);
    const uint64_t expected_seeklog = num_frames * static_cast<uint64_t>(kSeekTableEntrySize);
    if (seeklog_size != expected_seeklog) {
        safe_unlink(out_tmp_path);
        safe_unlink(seeklog_path);
        die_msg("seeklog size mismatch (internal error)");
    }

    const uint64_t payload_size_u64 = seeklog_size + kSeekTableFooterSize;
    if (payload_size_u64 > 0xFFFFFFFFu) {
        safe_unlink(out_tmp_path);
        safe_unlink(seeklog_path);
        die_msg("seek table payload exceeds 4GiB (format limitation)");
    }
    const uint32_t payload_size = static_cast<uint32_t>(payload_size_u64);

    // Write skippable frame header: magic + frame_size(payload)
    {
        uint32_t magic_le = u32_le(kSeekTableSkippableMagic);
        uint32_t size_le = u32_le(payload_size);
        uint8_t hdr[8];
        std::memcpy(&hdr[0], &magic_le, 4);
        std::memcpy(&hdr[4], &size_le, 4);
        write_all(out_fd.get(), hdr, sizeof(hdr));
    }

    // Copy seeklog entries into output
    if (::lseek(seeklog_fd.get(), 0, SEEK_SET) < 0) {
        safe_unlink(out_tmp_path);
        safe_unlink(seeklog_path);
        die_errno("lseek(seeklog, set)");
    }
    {
        std::vector<uint8_t> buf(1 << 20); // 1 MiB
        uint64_t remaining = seeklog_size;
        while (remaining > 0) {
            const size_t chunk = static_cast<size_t>(std::min<uint64_t>(remaining, buf.size()));
            read_all(seeklog_fd.get(), buf.data(), chunk);
            write_all(out_fd.get(), buf.data(), chunk);
            remaining -= chunk;
        }
    }

    // Footer: num_frames (u32) + descriptor (1) + seekable magic (u32)
    {
        std::vector<uint8_t> footer;
        footer.reserve(kSeekTableFooterSize);
        append_u32_le(footer, static_cast<uint32_t>(num_frames));
        footer.push_back(kSeekTableDescriptor);
        append_u32_le(footer, kSeekableMagic);
        if (footer.size() != kSeekTableFooterSize) {
            safe_unlink(out_tmp_path);
            safe_unlink(seeklog_path);
            die_msg("footer size mismatch (internal error)");
        }
        write_all(out_fd.get(), footer.data(), footer.size());
    }

    // Flush and close output
    if (::fsync(out_fd.get()) != 0) {
        safe_unlink(out_tmp_path);
        safe_unlink(seeklog_path);
        die_errno("fsync(output)");
    }

    out_fd.reset();
    seeklog_fd.reset();

    // Move output temp -> final
    try {
        safe_rename_overwrite(out_tmp_path, args.output_path);
    } catch (const std::exception& ex) {
        safe_unlink(out_tmp_path);
        safe_unlink(seeklog_path);
        die_msg(std::string("rename output failed: ") + ex.what());
    }

    // Delete seeklog temp
    safe_unlink(seeklog_path);

    return 0;
}