// tablebase_io.cxx
//   Tablebase I/O utilities for Geister endgame DTW tablebases.
//
// Formats:
//   1) Hex-lines text: 3 or 4 bytes/entry
//        - "xx\n"   (LF)
//        - "xx\r\n" (CRLF)
//   2) Raw binary: 1 byte/entry (headerless)
//
// This module also provides a small read-only mmap wrapper to support
// high-throughput runtime probing.
//
// Platform notes:
//   - POSIX: uses mmap(2)
//   - Windows: uses CreateFileMapping / MapViewOfFile

module;

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#elif defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

export module tablebase_io;

export namespace tbio {

	// ============================================================
	//  Hex-lines text format (.txt)
	// ============================================================

	// Roughly check whether the file size matches expected_entries * (3 or 4).
	//   - 3 bytes/entry: "xx\n"
	//   - 4 bytes/entry: "xx\r\n"
	[[nodiscard]] bool tablebase_file_looks_valid(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries) noexcept;

	// Read "xx\n" or "xx\r\n" into a vector<uint8_t> (throw if a value exceeds max_value).
	[[nodiscard]] std::vector<std::uint8_t> load_tablebase_hex_lines_streaming(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries,
		std::uint8_t max_value = 210);

	// Write vector<uint8_t> as "xx\n" (LF only) in binary mode.
	//  - To avoid leaving a corrupted file, write to filename.tmp first and then replace via rename.
	void write_tablebase_hex_lines_streaming(
		const std::vector<std::uint8_t>& tb,
		const std::filesystem::path& filename);

	// ============================================================
	//  Raw binary format (.bin) : 1 byte/entry (headerless)
	// ============================================================

	// Check whether the file size matches expected_entries (= 1 byte/entry).
	[[nodiscard]] bool tablebase_bin_looks_valid(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries) noexcept;

	// Read .bin into a vector<uint8_t> (throw if a value exceeds max_value).
	[[nodiscard]] std::vector<std::uint8_t> load_tablebase_bin_streaming(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries,
		std::uint8_t max_value = 210);

	// Write vector<uint8_t> as .bin (1 byte/entry).
	//  - To avoid leaving a corrupted file, write to filename.tmp first and then replace via rename.
	void write_tablebase_bin_streaming(
		const std::vector<std::uint8_t>& tb,
		const std::filesystem::path& filename);

} // namespace tbio

export namespace tbio::mmap {

	// madvise equivalent. Best-effort.
	enum class advice : std::uint8_t {
		none = 0,
		normal,
		random,
		sequential,
		will_need,
		dont_need,
	};

	// Read-only mmap wrapper (RAII).
	class mapped_file {
	public:
		mapped_file() noexcept = default;
		mapped_file(mapped_file&& other) noexcept;
		mapped_file& operator=(mapped_file&& other) noexcept;
		mapped_file(const mapped_file&) = delete;
		mapped_file& operator=(const mapped_file&) = delete;
		~mapped_file();

		// Open + mmap a file read-only.
		static mapped_file open_readonly(const std::filesystem::path& filename);

		[[nodiscard]] std::size_t size() const noexcept { return size_; }
		[[nodiscard]] const std::byte* data() const noexcept { return static_cast<const std::byte*>(addr_); }
		[[nodiscard]] std::span<const std::byte> bytes() const noexcept {
			return { data(), size_ };
		}
		[[nodiscard]] std::span<const std::uint8_t> u8span() const noexcept {
			return { reinterpret_cast<const std::uint8_t*>(addr_), size_ };
		}

		[[nodiscard]] bool empty() const noexcept { return addr_ == nullptr || size_ == 0; }

		// Best-effort advice.
		void advise(advice a) noexcept;

		// Best-effort page touch (useful to measure/trigger paging behaviour).
		void touch_pages(std::size_t stride_bytes = 4096) const noexcept;

	private:
		void* addr_ = nullptr;
		std::size_t size_ = 0;
	};

	// Convenience: open a headerless .bin tablebase and verify size.
	[[nodiscard]] mapped_file open_tablebase_bin_readonly(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries);

} // namespace tbio::mmap


export namespace tbio::seekable_zstd {

	// ============================================================
	//  Seekable Zstandard format (.zst/.zstd) random-access reader
	// ============================================================
	//
	// This is a thin wrapper over Zstandard's "seekable format" implementation
	// (contrib/seekable_format).
	//
	// Key properties for this project:
	//   - The compressed file is mmapped (read-only).
	//   - We build the seek table from the mmapped buffer (ZSTD_seekable_initBuff).
	//   - Runtime probing reads bytes at arbitrary decompressed offsets.
	//
	// Important:
	//   - Random access requires decompressing the whole frame that contains the
	//     requested offset. This wrapper caches the last decompressed frame
	//     (very small cache, but effective for localised access patterns).
	//   - Thread-safe: read_u8() serialises frame decompression with a mutex.
	//
	// External dependency:
	//   - Needs zstd's seekable_format decompressor symbols at link time
	//     (zstdseek_decompress.c) in addition to libzstd.

	class mapped_seekable_file {
	public:
		mapped_seekable_file() noexcept;
		mapped_seekable_file(mapped_seekable_file&& other) noexcept;
		mapped_seekable_file& operator=(mapped_seekable_file&& other) noexcept;
		mapped_seekable_file(const mapped_seekable_file&) = delete;
		mapped_seekable_file& operator=(const mapped_seekable_file&) = delete;
		~mapped_seekable_file();

		// Open + mmap a seekable-zstd file and verify its decompressed size.
		// Throws std::runtime_error on failure.
		static mapped_seekable_file open_readonly(
			const std::filesystem::path& filename,
			std::uint64_t expected_decompressed_size);

		[[nodiscard]] std::uint64_t decompressed_size() const noexcept { return decompressed_size_; }
		[[nodiscard]] std::size_t compressed_size() const noexcept { return mf_.size(); }
		[[nodiscard]] bool empty() const noexcept { return mf_.empty(); }

		// Propagate madvise/touch to the underlying compressed mmap (best-effort).
		void advise(tbio::mmap::advice a) noexcept { mf_.advise(a); }
		void touch_pages(std::size_t stride_bytes = 4096) const noexcept { mf_.touch_pages(stride_bytes); }

		// Read exactly 1 byte from the decompressed stream at `offset`.
		// Returns false on error (out-of-range, corrupted file, decompression error).
		[[nodiscard]] bool read_u8(std::uint64_t offset, std::uint8_t& out) const noexcept;

	private:
		struct ctx;

		tbio::mmap::mapped_file mf_{};
		std::uint64_t decompressed_size_{ 0 };
		std::unique_ptr<ctx> ctx_{};
	};

	// Convenience wrapper (mirrors tbio::mmap::open_tablebase_bin_readonly).
	[[nodiscard]] mapped_seekable_file open_tablebase_seekable_zstd_readonly(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries);

} // namespace tbio::seekable_zstd


module :private;

// -----------------------------------------------------------------------------
//  Implementation (private)
// -----------------------------------------------------------------------------

namespace tbio::detail {

	[[noreturn]] inline void fail(const char* msg) {
		throw std::runtime_error(msg);
	}
	[[noreturn]] inline void fail_msg(const std::string& msg) {
		throw std::runtime_error(msg);
	}

	[[nodiscard]] inline std::uint8_t hexval(char c) {
		if ('0' <= c && c <= '9') return static_cast<std::uint8_t>(c - '0');
		if ('a' <= c && c <= 'f') return static_cast<std::uint8_t>(10 + (c - 'a'));
		if ('A' <= c && c <= 'F') return static_cast<std::uint8_t>(10 + (c - 'A'));
		fail("invalid hex digit");
	}

	[[nodiscard]] inline std::uint8_t parse_hex_byte(char hi, char lo) {
		return static_cast<std::uint8_t>((hexval(hi) << 4) | hexval(lo));
	}

} // namespace tbio::detail

namespace tbio {

	[[nodiscard]] bool tablebase_file_looks_valid(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries) noexcept
	{
		try {
			const auto sz = std::filesystem::file_size(filename);
			// accept 3 or 4 bytes/entry.
			return (sz == expected_entries * 3ULL) || (sz == expected_entries * 4ULL);
		}
		catch (...) {
			return false;
		}
	}

	[[nodiscard]] std::vector<std::uint8_t> load_tablebase_hex_lines_streaming(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries,
		std::uint8_t max_value)
	{
		std::ifstream ifs(filename, std::ios::binary);
		if (!ifs) tbio::detail::fail_msg("failed to open file: " + filename.string());

		// Peek file size to decide LF vs CRLF.
		const auto sz = std::filesystem::file_size(filename);
		const bool is_lf = (sz == expected_entries * 3ULL);
		const bool is_crlf = (sz == expected_entries * 4ULL);
		if (!is_lf && !is_crlf) {
			std::ostringstream oss;
			oss << "invalid hex-lines size: file=" << sz
				<< " expected=" << (expected_entries * 3ULL) << " or " << (expected_entries * 4ULL);
			throw std::runtime_error(oss.str());
		}

		std::vector<std::uint8_t> tb;
		tb.reserve(static_cast<std::size_t>(expected_entries));

		for (std::uint64_t i = 0; i < expected_entries; ++i) {
			char hi = 0, lo = 0;
			ifs.get(hi);
			ifs.get(lo);
			if (!ifs) tbio::detail::fail("unexpected EOF while reading hex byte");

			const std::uint8_t v = tbio::detail::parse_hex_byte(hi, lo);
			if (v > max_value) tbio::detail::fail("table value exceeds max_value");
			tb.push_back(v);

			char nl = 0;
			ifs.get(nl);
			if (!ifs) tbio::detail::fail("unexpected EOF while reading newline");

			if (is_lf) {
				if (nl != '\n') tbio::detail::fail("expected LF");
			}
			else {
				// CRLF
				if (nl != '\r') tbio::detail::fail("expected CR");
				ifs.get(nl);
				if (!ifs) tbio::detail::fail("unexpected EOF while reading LF");
				if (nl != '\n') tbio::detail::fail("expected LF");
			}
		}

		// Ensure no extra trailing data.
		char extra = 0;
		if (ifs.get(extra)) tbio::detail::fail("extra trailing data found");

		return tb;
	}

	void write_tablebase_hex_lines_streaming(
		const std::vector<std::uint8_t>& tb,
		const std::filesystem::path& filename)
	{
		const std::filesystem::path tmp = filename.string() + ".tmp";
		{
			std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
			if (!ofs) tbio::detail::fail_msg("failed to open tmp file: " + tmp.string());

			static constexpr char hex[] = "0123456789abcdef";

			for (const std::uint8_t v : tb) {
				ofs.put(hex[(v >> 4) & 0xF]);
				ofs.put(hex[v & 0xF]);
				ofs.put('\n');
				if (!ofs) tbio::detail::fail("write failed");
			}
		}
		std::filesystem::rename(tmp, filename);
	}

	[[nodiscard]] bool tablebase_bin_looks_valid(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries) noexcept
	{
		try {
			return std::filesystem::file_size(filename) == expected_entries;
		}
		catch (...) {
			return false;
		}
	}

	[[nodiscard]] std::vector<std::uint8_t> load_tablebase_bin_streaming(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries,
		std::uint8_t max_value)
	{
		std::ifstream ifs(filename, std::ios::binary);
		if (!ifs) tbio::detail::fail_msg("failed to open file: " + filename.string());

		const auto sz = std::filesystem::file_size(filename);
		if (sz != expected_entries) {
			std::ostringstream oss;
			oss << "invalid .bin size: file=" << sz << " expected=" << expected_entries;
			throw std::runtime_error(oss.str());
		}

		std::vector<std::uint8_t> tb(static_cast<std::size_t>(expected_entries));
		if (!ifs.read(reinterpret_cast<char*>(tb.data()), static_cast<std::streamsize>(tb.size()))) {
			tbio::detail::fail("failed to read full .bin");
		}

		for (const auto v : tb) {
			if (v > max_value) tbio::detail::fail("table value exceeds max_value");
		}

		// Ensure no extra trailing data.
		char extra = 0;
		if (ifs.get(extra)) tbio::detail::fail("extra trailing data found");

		return tb;
	}

	void write_tablebase_bin_streaming(
		const std::vector<std::uint8_t>& tb,
		const std::filesystem::path& filename)
	{
		const std::filesystem::path tmp = filename.string() + ".tmp";
		{
			std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
			if (!ofs) tbio::detail::fail_msg("failed to open tmp file: " + tmp.string());

			if (!ofs.write(reinterpret_cast<const char*>(tb.data()), static_cast<std::streamsize>(tb.size()))) {
				tbio::detail::fail("write failed");
			}
		}
		std::filesystem::rename(tmp, filename);
	}

} // namespace tbio

namespace tbio::mmap {

#if defined(__unix__) || defined(__APPLE__)

	mapped_file::mapped_file(mapped_file&& other) noexcept {
		addr_ = other.addr_;
		size_ = other.size_;
		other.addr_ = nullptr;
		other.size_ = 0;
	}

	mapped_file& mapped_file::operator=(mapped_file&& other) noexcept {
		if (this == &other) return *this;
		// release current
		if (addr_) {
			::munmap(addr_, size_);
		}
		addr_ = other.addr_;
		size_ = other.size_;
		other.addr_ = nullptr;
		other.size_ = 0;
		return *this;
	}

	mapped_file::~mapped_file() {
		if (addr_) {
			::munmap(addr_, size_);
		}
		addr_ = nullptr;
		size_ = 0;
	}

	mapped_file mapped_file::open_readonly(const std::filesystem::path& filename) {
		const int fd = ::open(filename.c_str(), O_RDONLY);
		if (fd < 0) {
			std::ostringstream oss;
			oss << "open failed: " << filename.string() << " errno=" << errno;
			throw std::runtime_error(oss.str());
		}

		struct stat st {};
		if (::fstat(fd, &st) != 0) {
			const int e = errno;
			::close(fd);
			std::ostringstream oss;
			oss << "fstat failed: " << filename.string() << " errno=" << e;
			throw std::runtime_error(oss.str());
		}

		const std::uint64_t sz64 = static_cast<std::uint64_t>(st.st_size);
		if (sz64 > static_cast<std::uint64_t>((std::numeric_limits<std::size_t>::max)())) {
			::close(fd);
			throw std::runtime_error("file too large to mmap on this platform");
		}
		const std::size_t sz = static_cast<std::size_t>(sz64);

		void* addr = nullptr;
		if (sz != 0) {
			addr = ::mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
			if (addr == MAP_FAILED) {
				const int e = errno;
				::close(fd);
				std::ostringstream oss;
				oss << "mmap failed: " << filename.string() << " errno=" << e;
				throw std::runtime_error(oss.str());
			}
		}

		::close(fd);

		mapped_file out;
		out.addr_ = addr;
		out.size_ = sz;
		return out;
	}

	void mapped_file::advise(advice a) noexcept {
		if (!addr_ || size_ == 0) return;

		int madv = MADV_NORMAL;
		switch (a) {
		case advice::none:
		case advice::normal: madv = MADV_NORMAL; break;
		case advice::random: madv = MADV_RANDOM; break;
		case advice::sequential: madv = MADV_SEQUENTIAL; break;
		case advice::will_need: madv = MADV_WILLNEED; break;
		case advice::dont_need: madv = MADV_DONTNEED; break;
		default: madv = MADV_NORMAL; break;
		}

		(void)::madvise(addr_, size_, madv);
	}

	void mapped_file::touch_pages(std::size_t stride_bytes) const noexcept {
		if (!addr_ || size_ == 0) return;
		if (stride_bytes == 0) stride_bytes = 4096;

		volatile std::uint8_t sink = 0;
		const auto* p = reinterpret_cast<const std::uint8_t*>(addr_);
		for (std::size_t i = 0; i < size_; i += stride_bytes) {
			sink ^= p[i];
		}
		(void)sink;
	}

#elif defined(_WIN32)

	mapped_file::mapped_file(mapped_file&& other) noexcept {
		addr_ = other.addr_;
		size_ = other.size_;
		other.addr_ = nullptr;
		other.size_ = 0;
	}

	mapped_file& mapped_file::operator=(mapped_file&& other) noexcept {
		if (this == &other) return *this;
		// release current
		if (addr_) {
			(void)::UnmapViewOfFile(addr_);
		}
		addr_ = other.addr_;
		size_ = other.size_;
		other.addr_ = nullptr;
		other.size_ = 0;
		return *this;
	}

	mapped_file::~mapped_file() {
		if (addr_) {
			(void)::UnmapViewOfFile(addr_);
		}
		addr_ = nullptr;
		size_ = 0;
	}

	mapped_file mapped_file::open_readonly(const std::filesystem::path& filename) {
		// NOTE:
		//   - std::filesystem::path::c_str() is wchar_t* on Windows.
		//   - We intentionally share READ/WRITE/DELETE to behave closer to POSIX.
		//     (Allows other processes to rename/delete while this process mmaps.)
		const DWORD share = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
		const DWORD flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS;

		HANDLE hFile = ::CreateFileW(
			filename.c_str(),
			GENERIC_READ,
			share,
			nullptr,
			OPEN_EXISTING,
			flags,
			nullptr);

		if (hFile == INVALID_HANDLE_VALUE) {
			const DWORD e = ::GetLastError();
			std::ostringstream oss;
			oss << "CreateFileW failed: " << filename.string() << " gle=" << e;
			throw std::runtime_error(oss.str());
		}

		LARGE_INTEGER li{};
		if (!::GetFileSizeEx(hFile, &li)) {
			const DWORD e = ::GetLastError();
			(void)::CloseHandle(hFile);
			std::ostringstream oss;
			oss << "GetFileSizeEx failed: " << filename.string() << " gle=" << e;
			throw std::runtime_error(oss.str());
		}

		const std::uint64_t sz64 = static_cast<std::uint64_t>(li.QuadPart);
		if (sz64 > static_cast<std::uint64_t>((std::numeric_limits<std::size_t>::max)())) {
			(void)::CloseHandle(hFile);
			throw std::runtime_error("file too large to mmap on this platform");
		}
		const std::size_t sz = static_cast<std::size_t>(sz64);

		if (sz == 0) {
			// Empty file: map nothing.
			(void)::CloseHandle(hFile);
			mapped_file out;
			out.addr_ = nullptr;
			out.size_ = 0;
			return out;
		}

		HANDLE hMap = ::CreateFileMappingW(
			hFile,
			nullptr,
			PAGE_READONLY,
			0,
			0,
			nullptr);

		if (hMap == nullptr) {
			const DWORD e = ::GetLastError();
			(void)::CloseHandle(hFile);
			std::ostringstream oss;
			oss << "CreateFileMappingW failed: " << filename.string() << " gle=" << e;
			throw std::runtime_error(oss.str());
		}

		void* addr = ::MapViewOfFile(
			hMap,
			FILE_MAP_READ,
			0,
			0,
			0);

		if (addr == nullptr) {
			const DWORD e = ::GetLastError();
			(void)::CloseHandle(hMap);
			(void)::CloseHandle(hFile);
			std::ostringstream oss;
			oss << "MapViewOfFile failed: " << filename.string() << " gle=" << e;
			throw std::runtime_error(oss.str());
		}

		// After the view is created, it remains valid even if we close the handles.
		// (Common Windows mmap pattern.)
		(void)::CloseHandle(hMap);
		(void)::CloseHandle(hFile);

		mapped_file out;
		out.addr_ = addr;
		out.size_ = sz;
		return out;
	}

	void mapped_file::advise(advice a) noexcept {
		if (!addr_ || size_ == 0) return;

		// Best-effort Windows equivalents.
		// We deliberately do *not* hard-depend on newer SDK declarations; instead we
		// use GetProcAddress to call APIs only when available.
		//
		// - will_need / sequential : PrefetchVirtualMemory (Win8+)
		// - dont_need              : DiscardVirtualMemory  (Win8.1+)

		const HMODULE k32 = ::GetModuleHandleW(L"kernel32.dll");
		if (!k32) return;

		if (a == advice::will_need || a == advice::sequential) {
			using PrefetchVirtualMemoryFn = BOOL (WINAPI*)(HANDLE, std::size_t, const void*, ULONG);
			auto fn = reinterpret_cast<PrefetchVirtualMemoryFn>(
				::GetProcAddress(k32, "PrefetchVirtualMemory"));
			if (!fn) return;

			struct range_entry {
				void* VirtualAddress;
				std::size_t NumberOfBytes;
			};
			range_entry e{ addr_, size_ };
			(void)fn(::GetCurrentProcess(), 1, &e, 0);
			return;
		}

		if (a == advice::dont_need) {
			using DiscardVirtualMemoryFn = DWORD (WINAPI*)(void*, std::size_t);
			auto fn = reinterpret_cast<DiscardVirtualMemoryFn>(
				::GetProcAddress(k32, "DiscardVirtualMemory"));
			if (!fn) return;
			(void)fn(addr_, size_);
			return;
		}

		// normal/random/none: no-op.
	}

	void mapped_file::touch_pages(std::size_t stride_bytes) const noexcept {
		if (!addr_ || size_ == 0) return;
		if (stride_bytes == 0) stride_bytes = 4096;

		volatile std::uint8_t sink = 0;
		const auto* p = reinterpret_cast<const std::uint8_t*>(addr_);
		for (std::size_t i = 0; i < size_; i += stride_bytes) {
			sink ^= p[i];
		}
		(void)sink;
	}

#else

	mapped_file::mapped_file(mapped_file&&) noexcept = default;
	mapped_file& mapped_file::operator=(mapped_file&&) noexcept = default;
	mapped_file::~mapped_file() = default;

	mapped_file mapped_file::open_readonly(const std::filesystem::path&) {
		throw std::runtime_error("mmap is not supported on this platform (tablebase_io)");
	}

	void mapped_file::advise(advice) noexcept {}
	void mapped_file::touch_pages(std::size_t) const noexcept {}

#endif

	[[nodiscard]] mapped_file open_tablebase_bin_readonly(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries)
	{
		auto mf = mapped_file::open_readonly(filename);
		if (static_cast<std::uint64_t>(mf.size()) != expected_entries) {
			std::ostringstream oss;
			oss << "tablebase .bin size mismatch: file=" << mf.size()
				<< " expected=" << expected_entries
				<< " (" << filename.string() << ")";
			throw std::runtime_error(oss.str());
		}
		return mf;
	}

} // namespace tbio::mmap


// -----------------------------------------------------------------------------
//  Seekable Zstandard (contrib/seekable_format) implementation
// -----------------------------------------------------------------------------

extern "C" {

	// Minimal declarations for zstd seekable-format decoder API.
	// We intentionally avoid including zstd headers here to keep build integration
	// flexible (system zstd headers vs vendored zstd).
	typedef struct ZSTD_seekable_s ZSTD_seekable;

	ZSTD_seekable* ZSTD_seekable_create(void);
	size_t ZSTD_seekable_free(ZSTD_seekable* zs);

	size_t ZSTD_seekable_initBuff(ZSTD_seekable* zs, const void* src, size_t srcSize);

	size_t ZSTD_seekable_decompress(ZSTD_seekable* zs, void* dst, size_t dstSize, unsigned long long offset);
	size_t ZSTD_seekable_decompressFrame(ZSTD_seekable* zs, void* dst, size_t dstSize, unsigned frameIndex);

	unsigned ZSTD_seekable_getNumFrames(const ZSTD_seekable* zs);
	unsigned long long ZSTD_seekable_getFrameDecompressedOffset(const ZSTD_seekable* zs, unsigned frameIndex);
	size_t ZSTD_seekable_getFrameDecompressedSize(const ZSTD_seekable* zs, unsigned frameIndex);
	unsigned ZSTD_seekable_offsetToFrameIndex(const ZSTD_seekable* zs, unsigned long long offset);

	// zstd error helpers
	unsigned ZSTD_isError(size_t code);
	const char* ZSTD_getErrorName(size_t code);
}

namespace tbio::seekable_zstd {

	namespace detail {

		[[nodiscard]] inline std::string zstd_errname(size_t code) {
			const char* s = ZSTD_getErrorName(code);
			return s ? std::string(s) : std::string("ZSTD_error");
		}

		[[nodiscard]] inline std::uint64_t checked_add_u64(std::uint64_t a, std::uint64_t b, const char* what) {
			if (a > (std::numeric_limits<std::uint64_t>::max)() - b) {
				throw std::runtime_error(std::string("overflow while computing ") + what);
			}
			return a + b;
		}

	} // namespace detail

	struct mapped_seekable_file::ctx {
		ZSTD_seekable* zs = nullptr;
		unsigned num_frames = 0;

		// One-frame cache.
		unsigned cached_frame = (std::numeric_limits<unsigned>::max)();
		std::uint64_t cached_off = 0;
		std::size_t cached_size = 0;
		std::vector<std::uint8_t> cached_buf{};

		// Serialise decompression / cache updates.
		std::mutex mtx{};

		~ctx() {
			if (zs) {
				(void)ZSTD_seekable_free(zs);
			}
			zs = nullptr;
			num_frames = 0;
		}
	};

	// NOTE:
	//   MSVC named-modules builds may emit duplicate external definitions for
	//   these out-of-line defaulted special members. Keeping them inline avoids
	//   LNK2005/LNK1169 while preserving the intended semantics.
	inline mapped_seekable_file::mapped_seekable_file() noexcept = default;
	inline mapped_seekable_file::mapped_seekable_file(mapped_seekable_file&& other) noexcept = default;
	inline mapped_seekable_file& mapped_seekable_file::operator=(mapped_seekable_file&& other) noexcept = default;
	inline mapped_seekable_file::~mapped_seekable_file() = default;

	mapped_seekable_file mapped_seekable_file::open_readonly(
		const std::filesystem::path& filename,
		std::uint64_t expected_decompressed_size)
	{
		// mmap compressed file first
		tbio::mmap::mapped_file mf = tbio::mmap::mapped_file::open_readonly(filename);

		// Construct seekable decoder and parse seek table from the buffer.
		ZSTD_seekable* zs = ZSTD_seekable_create();
		if (!zs) {
			throw std::runtime_error("ZSTD_seekable_create() failed");
		}

		const void* src = static_cast<const void*>(mf.data());
		const size_t srcSize = mf.size();

		const size_t init_r = ZSTD_seekable_initBuff(zs, src, srcSize);
		if (ZSTD_isError(init_r)) {
			const std::string msg = std::string("ZSTD_seekable_initBuff failed: ") + detail::zstd_errname(init_r);
			(void)ZSTD_seekable_free(zs);
			throw std::runtime_error(msg);
		}

		const unsigned nf = ZSTD_seekable_getNumFrames(zs);
		if (nf == 0) {
			(void)ZSTD_seekable_free(zs);
			throw std::runtime_error("seekable zstd: getNumFrames() returned 0 (invalid/corrupt file?)");
		}

		const unsigned last = nf - 1;
		const std::uint64_t last_off = static_cast<std::uint64_t>(ZSTD_seekable_getFrameDecompressedOffset(zs, last));
		const std::uint64_t last_sz = static_cast<std::uint64_t>(ZSTD_seekable_getFrameDecompressedSize(zs, last));
		const std::uint64_t total = detail::checked_add_u64(last_off, last_sz, "seekable decompressed size");

		if (total != expected_decompressed_size) {
			std::ostringstream oss;
			oss << "seekable zstd decompressed size mismatch: expected=" << expected_decompressed_size
				<< " actual=" << total;
			(void)ZSTD_seekable_free(zs);
			throw std::runtime_error(oss.str());
		}

		mapped_seekable_file out;
		out.mf_ = std::move(mf);
		out.decompressed_size_ = total;
		out.ctx_ = std::make_unique<ctx>();
		out.ctx_->zs = zs;
		out.ctx_->num_frames = nf;

		return out;
	}

	bool mapped_seekable_file::read_u8(std::uint64_t offset, std::uint8_t& out) const noexcept {
		if (!ctx_) return false;
		if (offset >= decompressed_size_) return false;

		ctx& c = *ctx_;
		std::lock_guard<std::mutex> lock(c.mtx);

		// Cache hit?
		if (c.cached_frame != (std::numeric_limits<unsigned>::max)()) {
			const std::uint64_t begin = c.cached_off;
			const std::uint64_t end = begin + static_cast<std::uint64_t>(c.cached_size);
			if (begin <= offset && offset < end) {
				const std::size_t idx = static_cast<std::size_t>(offset - begin);
				out = c.cached_buf[idx];
				return true;
			}
		}

		// Determine frame index for this offset.
		const unsigned frame = ZSTD_seekable_offsetToFrameIndex(c.zs, static_cast<unsigned long long>(offset));
		if (frame >= c.num_frames) return false;

		const std::uint64_t frame_off = static_cast<std::uint64_t>(ZSTD_seekable_getFrameDecompressedOffset(c.zs, frame));
		const std::size_t frame_sz = ZSTD_seekable_getFrameDecompressedSize(c.zs, frame);
		if (frame_sz == 0) return false;

		// Defensive range check.
		const std::uint64_t frame_end = frame_off + static_cast<std::uint64_t>(frame_sz);
		if (!(frame_off <= offset && offset < frame_end)) return false;

		// Decompress entire frame into cache buffer.
		try {
			c.cached_buf.resize(frame_sz);
		}
		catch (...) {
			return false; // OOM
		}

		const size_t dec_r = ZSTD_seekable_decompressFrame(
			c.zs,
			static_cast<void*>(c.cached_buf.data()),
			frame_sz,
			frame);

		if (ZSTD_isError(dec_r) || dec_r != frame_sz) {
			return false;
		}

		c.cached_frame = frame;
		c.cached_off = frame_off;
		c.cached_size = frame_sz;

		const std::size_t idx = static_cast<std::size_t>(offset - frame_off);
		out = c.cached_buf[idx];
		return true;
	}

	mapped_seekable_file open_tablebase_seekable_zstd_readonly(
		const std::filesystem::path& filename,
		std::uint64_t expected_entries)
	{
		return mapped_seekable_file::open_readonly(filename, expected_entries);
	}

} // namespace tbio::seekable_zstd
