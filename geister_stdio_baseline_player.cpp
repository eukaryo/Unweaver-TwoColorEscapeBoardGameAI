#include <array>
#include <cctype>
#include <cstdint>
#include <exception>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

import geister_core;
import geister_interface;
import geister_random_player;
import geister_proven_escape;
import geister_purple_winning;
import confident_player;
import geister_tb_handler;

namespace {

[[nodiscard]] inline bool starts_with(std::string_view s, std::string_view prefix) noexcept {
	return (s.size() >= prefix.size()) && (s.substr(0, prefix.size()) == prefix);
}

inline void trim_inplace(std::string& s) {
	auto is_ws = [](unsigned char ch) { return std::isspace(ch) != 0; };
	std::size_t b = 0;
	while (b < s.size() && is_ws(static_cast<unsigned char>(s[b]))) ++b;
	std::size_t e = s.size();
	while (e > b && is_ws(static_cast<unsigned char>(s[e - 1]))) --e;
	s = s.substr(b, e - b);
}

inline void drop_trailing_cr_inplace(std::string& s) {
	if (!s.empty() && s.back() == '\r') s.pop_back();
}

[[nodiscard]] inline std::string compact_alnum(std::string_view s) {
	std::string out;
	out.reserve(s.size());
	for (char c : s) {
		const unsigned char uc = static_cast<unsigned char>(c);
		if (std::isalnum(uc)) out.push_back(c);
	}
	return out;
}

[[nodiscard]] inline char tolower_ascii(char c) noexcept {
	const unsigned char uc = static_cast<unsigned char>(c);
	return static_cast<char>(std::tolower(uc));
}

struct parsed_turn {
	board_observation obs{};
	std::array<std::uint8_t, 8> my_sq{};
	std::array<char, 8> my_code{};
	std::string board48;
};

[[nodiscard]] inline std::uint8_t xy_to_sq(const int x, const int y) noexcept {
	return static_cast<std::uint8_t>((y + 1) * 8 + (x + 1));
}

[[nodiscard]] bool parse_board48(std::string_view board48, parsed_turn& out) {
	if (board48.size() < 48) return false;

	out.board48.assign(board48.substr(0, 48));
	out.obs = board_observation{};
	out.my_sq.fill(0xFF);
	for (int i = 0; i < 8; ++i) out.my_code[i] = static_cast<char>('A' + i);

	int captured_opp_red = 0;
	for (int i = 0; i < 16; ++i) {
		const char cx = out.board48[3 * i + 0];
		const char cy = out.board48[3 * i + 1];
		const char cc = out.board48[3 * i + 2];
		if (cx < '0' || cx > '9' || cy < '0' || cy > '9') return false;
		const int x = cx - '0';
		const int y = cy - '0';
		const char lc = tolower_ascii(cc);

		const bool on_board = (0 <= x && x <= 5 && 0 <= y && y <= 5);
		const bool captured = (x == 9 && y == 9);

		if (i < 8) {
			if (!on_board) continue;
			const std::uint8_t sq = xy_to_sq(x, y);
			out.my_sq[i] = sq;
			if (lc == 'r') out.obs.bb_my_red |= (1ULL << sq);
			else if (lc == 'b') out.obs.bb_my_blue |= (1ULL << sq);
		}
		else {
			if (on_board) {
				const std::uint8_t sq = xy_to_sq(x, y);
				out.obs.bb_opponent_unknown |= (1ULL << sq);
			}
			else if (captured && lc == 'r') {
				++captured_opp_red;
			}
		}
	}

	out.obs.pop_captured_opponent_red = static_cast<std::uint8_t>(captured_opp_red);
	return true;
}

[[nodiscard]] inline std::string direction_to_protocol_string(const std::uint8_t dir) {
	switch (dir & 3ULL) {
	case DIRECTIONS::UP: return "NORTH";
	case DIRECTIONS::DOWN: return "SOUTH";
	case DIRECTIONS::LEFT: return "WEST";
	case DIRECTIONS::RIGHT: return "EAST";
	default: return "NORTH";
	}
}

[[nodiscard]] inline std::string format_mov(char piece_code, std::string_view dir) {
	std::string s;
	s.reserve(16);
	s += "MOV:";
	s.push_back(piece_code);
	s.push_back(',');
	s.append(dir);
	return s;
}

[[nodiscard]] inline std::string choose_set_response() {
	if (const char* env = std::getenv("GEISTER_RED")) {
		std::string s(env);
		trim_inplace(s);
		if (s.size() == 4) {
			bool ok = true;
			bool seen[8] = { false, false, false, false, false, false, false, false };
			for (char c : s) {
				if (c < 'A' || c > 'H') { ok = false; break; }
				const int idx = c - 'A';
				if (seen[idx]) { ok = false; break; }
				seen[idx] = true;
			}
			if (ok) return "SET:" + s;
		}
	}
	return "SET:ABCD";
}

[[nodiscard]] inline char find_my_piece_on_square(const parsed_turn& st, const std::uint8_t sq) {
	for (int i = 0; i < 8; ++i) {
		if (st.my_sq[i] == sq) return st.my_code[i];
	}
	return '?';
}

[[nodiscard]] inline std::optional<protocol_move> maybe_escape_move(const parsed_turn& st) {
	const std::uint8_t a1 = static_cast<std::uint8_t>(POSITIONS::A1);
	const std::uint8_t f1 = static_cast<std::uint8_t>(POSITIONS::F1);
	if ((st.obs.bb_my_blue & (1ULL << a1)) != 0ULL) {
		return protocol_move(a1, static_cast<std::uint8_t>(DIRECTIONS::LEFT));
	}
	if ((st.obs.bb_my_blue & (1ULL << f1)) != 0ULL) {
		return protocol_move(f1, static_cast<std::uint8_t>(DIRECTIONS::RIGHT));
	}
	return std::nullopt;
}

[[nodiscard]] inline std::uint64_t splitmix64_next(std::uint64_t& state) noexcept {
	state += 0x9E3779B97F4A7C15ULL;
	std::uint64_t z = state;
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
	z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
	return z ^ (z >> 31);
}

[[nodiscard]] inline std::uint64_t default_seed_from_position(const parsed_turn& st) noexcept {
	std::uint64_t s = 0xD1B54A32D192ED03ULL;
	s ^= st.obs.bb_my_blue + 0x9E3779B97F4A7C15ULL + (s << 6) + (s >> 2);
	s ^= st.obs.bb_my_red + 0x9E3779B97F4A7C15ULL + (s << 6) + (s >> 2);
	s ^= st.obs.bb_opponent_unknown + 0x9E3779B97F4A7C15ULL + (s << 6) + (s >> 2);
	s ^= static_cast<std::uint64_t>(st.obs.pop_captured_opponent_red) + 0x9E3779B97F4A7C15ULL + (s << 6) + (s >> 2);
	return s;
}

[[nodiscard]] inline protocol_move choose_random_best_move(const std::vector<protocol_move>& best, const parsed_turn& st) {
	if (best.empty()) return protocol_move{};
	std::uint64_t state = default_seed_from_position(st);
	const std::uint64_t r = splitmix64_next(state);
	return best[static_cast<std::size_t>(r % best.size())];
}

[[nodiscard]] inline std::string format_protocol_move(const parsed_turn& st, const protocol_move pm) {
	const char piece_code = find_my_piece_on_square(st, pm.from);
	const std::string dir = direction_to_protocol_string(pm.dir);
	return format_mov((piece_code != '?' ? piece_code : 'A'), dir);
}

[[nodiscard]] inline std::string decide_move_response(const parsed_turn& st) {
	if (const auto esc = maybe_escape_move(st)) {
		return format_protocol_move(st, *esc);
	}

	const std::uint64_t seed = default_seed_from_position(st);

	if (const auto proven = proven_escape_move(
		st.obs.bb_my_blue,
		st.obs.bb_my_red,
		st.obs.bb_opponent_unknown,
		static_cast<int>(st.obs.pop_captured_opponent_red),
		seed))
	{
		return format_protocol_move(st, *proven);
	}

	if (const auto purple = purple_winning_move(
		st.obs.bb_my_blue,
		st.obs.bb_my_red,
		st.obs.bb_opponent_unknown,
		static_cast<int>(st.obs.pop_captured_opponent_red),
		seed))
	{
		return format_protocol_move(st, *purple);
	}

	if (const auto best = confident_player(
		st.obs.bb_my_blue,
		st.obs.bb_my_red,
		st.obs.bb_opponent_unknown,
		static_cast<int>(st.obs.pop_captured_opponent_red)))
	{
		if (!best->empty()) {
			return format_protocol_move(st, choose_random_best_move(*best, st));
		}
	}

	const protocol_move m = random_player(
		st.obs.bb_my_blue,
		st.obs.bb_my_red,
		st.obs.bb_opponent_unknown,
		static_cast<int>(st.obs.pop_captured_opponent_red));

	return format_protocol_move(st, m);
}

[[nodiscard]] inline std::optional<std::string> extract_mov_payload(std::string_view line) {
	if (line.size() < 4) return std::nullopt;
	if (!(starts_with(line, "MOV?") || starts_with(line, "MOV:"))) return std::nullopt;

	std::size_t i = 4;
	while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i]))) ++i;
	return std::string(line.substr(i));
}

[[nodiscard]] bool setup_tablebase_directory_from_argv(int argc, char** argv) {
	if (argc <= 1) return true;
	const std::string_view arg1 = argv[1];
	if (arg1 == "-h" || arg1 == "--help") {
		std::cout << "Usage: " << argv[0] << " [TB_DIR]\n";
		std::cout << "  TB_DIR omitted => use current directory\n";
		return false;
	}
	try {
		std::filesystem::current_path(std::filesystem::path(argv[1]));
		return true;
	}
	catch (const std::exception& e) {
		std::cerr << "[geister_stdio_baseline_player] ERROR: failed to enter TB_DIR '"
			<< argv[1] << "': " << e.what() << "\n";
		return false;
	}
}

} // namespace

int main(int argc, char** argv) {
	setvbuf(stdout, nullptr, _IONBF, 0);
	setvbuf(stderr, nullptr, _IONBF, 0);

	std::ios::sync_with_stdio(false);
	std::cin.tie(nullptr);
	std::cout.setf(std::ios::unitbuf);
	std::cerr.setf(std::ios::unitbuf);

	if (!setup_tablebase_directory_from_argv(argc, argv)) {
		return (argc > 1 && (std::string_view(argv[1]) == "-h" || std::string_view(argv[1]) == "--help")) ? 0 : 2;
	}

	geister_tb::start_background_load();

	const bool log_incoming = (std::getenv("GEISTER_LOG_PROTOCOL") != nullptr);

	std::string line;
	while (std::getline(std::cin, line)) {
		drop_trailing_cr_inplace(line);
		if (log_incoming) std::cerr << "[IN ] " << line << "\n";

		std::string trimmed = line;
		trim_inplace(trimmed);
		std::string response;

		if (trimmed == "/exit") {
			std::cout << "\r\n" << std::flush;
			break;
		}
		if (trimmed.find("SET?") != std::string::npos) {
			response = choose_set_response();
		}
		else if (auto payload = extract_mov_payload(trimmed)) {
			const std::string board_compact = compact_alnum(*payload);
			parsed_turn st;
			if (!parse_board48(board_compact, st)) {
				std::cerr << "[geister_stdio_baseline_player] ERROR: failed to parse board payload: '"
					<< *payload << "'\n";
				response = "MOV:A,NORTH";
			}
			else {
				response = decide_move_response(st);
			}
		}
		else {
			response.clear();
		}

		if (!response.empty()) {
			if (log_incoming) std::cerr << "[OUT] " << response << "\n";
			std::cout << response << "\r\n" << std::flush;
		}
		else {
			std::cout << "\r\n" << std::flush;
		}
	}

	return 0;
}
