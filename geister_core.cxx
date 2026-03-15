module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <exception>
#include <utility>

export module geister_core;

export enum POSITIONS {
	A1 = 8 * 1 + 1, B1, C1, D1, E1, F1,
	A2 = 8 * 2 + 1, B2, C2, D2, E2, F2,
	A3 = 8 * 3 + 1, B3, C3, D3, E3, F3,
	A4 = 8 * 4 + 1, B4, C4, D4, E4, F4,
	A5 = 8 * 5 + 1, B5, C5, D5, E5, F5,
	A6 = 8 * 6 + 1, B6, C6, D6, E6, F6,
};

export enum DIRECTIONS {
	UP = 0, DOWN = 1, LEFT = 2, RIGHT = 3,
};

export inline constexpr std::int8_t DIFF_SQUARE[4] = { -8, 8, -1, 1 };

export inline constexpr std::uint16_t CAPTURE_RED_FLAG  = 1U << 14;
export inline constexpr std::uint16_t CAPTURE_BLUE_FLAG = 1U << 15;

// 6x6 board embedded in 8x8
export inline constexpr std::uint64_t bb_board        = 0x007E'7E7E'7E7E'7E00ULL;
export inline constexpr std::uint64_t bb_initial_self = 0x003C'3C00'0000'0000ULL;

export [[nodiscard]] std::uint64_t bit_reverse64(std::uint64_t x) noexcept;

export struct move {
	std::uint16_t m;
	move() noexcept;

	[[nodiscard]] std::uint64_t get_from() const noexcept;
	[[nodiscard]] std::uint64_t get_to() const noexcept;
	[[nodiscard]] std::uint64_t get_direction() const noexcept;

	[[nodiscard]] bool if_capture_red() const noexcept;
	[[nodiscard]] bool if_capture_blue() const noexcept;
	[[nodiscard]] bool if_capture_any() const noexcept;
};

export struct player_board {
	std::uint64_t bb_piece, bb_red, bb_blue;

	player_board(std::uint64_t red);
	player_board(std::uint64_t red, std::uint64_t blue);

	[[nodiscard]] int gen_moves(
		std::uint64_t bb_opponent_red,
		std::uint64_t bb_opponent_blue,
		std::array<move, 32>& dest) const noexcept;
};

export struct perfect_information_geister {
	player_board bb_player, bb_opponent;

	// A perfect-information position must always be constructed with explicit boards.
	// Default construction is forbidden on purpose.
	perfect_information_geister() = delete;
	perfect_information_geister(player_board player, player_board opponent) noexcept;

	static void do_move(const move m, perfect_information_geister& position) noexcept;

	[[nodiscard]] bool is_immediate_win() const noexcept;
	[[nodiscard]] bool is_immediate_loss() const noexcept;
};

module:private;

[[nodiscard]] std::uint64_t bit_reverse64(std::uint64_t x) noexcept {
	// Portable bit-reversal (byte/word swaps).
	x = ((x & 0x5555555555555555ULL) << 1) | ((x >> 1) & 0x5555555555555555ULL);
	x = ((x & 0x3333333333333333ULL) << 2) | ((x >> 2) & 0x3333333333333333ULL);
	x = ((x & 0x0F0F0F0F0F0F0F0FULL) << 4) | ((x >> 4) & 0x0F0F0F0F0F0F0F0FULL);
	x = ((x & 0x00FF00FF00FF00FFULL) << 8) | ((x >> 8) & 0x00FF00FF00FF00FFULL);
	x = ((x & 0x0000FFFF0000FFFFULL) << 16) | ((x >> 16) & 0x0000FFFF0000FFFFULL);
	x = (x << 32) | (x >> 32);
	return x;
}

move::move() noexcept : m(0) {}

std::uint64_t move::get_from() const noexcept { return m & 63U; }
std::uint64_t move::get_to() const noexcept { return (m >> 6) & 63U; }
std::uint64_t move::get_direction() const noexcept { return (m >> 12) & 3U; }

bool move::if_capture_red() const noexcept { return (m & CAPTURE_RED_FLAG) != 0; }
bool move::if_capture_blue() const noexcept { return (m & CAPTURE_BLUE_FLAG) != 0; }
bool move::if_capture_any() const noexcept { return (m & (CAPTURE_RED_FLAG | CAPTURE_BLUE_FLAG)) != 0; }

player_board::player_board(const std::uint64_t red) {
	assert(std::popcount(red) == 4);
	assert((red & bb_initial_self) == red);
	bb_red = red;
	bb_blue = bb_initial_self ^ red;
	bb_piece = bb_initial_self;
}

player_board::player_board(const std::uint64_t red, const std::uint64_t blue)
	: bb_piece(red | blue), bb_red(red), bb_blue(blue)
{
	if ((bb_piece & ~bb_board) != 0) throw std::exception{};
	if ((bb_red & ~bb_board) != 0) throw std::exception{};
	if ((bb_blue & ~bb_board) != 0) throw std::exception{};
	if ((bb_red & bb_blue) != 0) throw std::exception{};
	if (bb_piece != (bb_red | bb_blue)) throw std::exception{};
}

int player_board::gen_moves(
	const std::uint64_t bb_opponent_red,
	const std::uint64_t bb_opponent_blue,
	std::array<move, 32>& dest) const noexcept
{
	int count = 0;
	const std::uint64_t bb_movable = bb_board ^ bb_piece;

	// up
	for (std::uint64_t bb_from = ((bb_movable << 8) & bb_piece); bb_from; bb_from &= (bb_from - 1)) {
		const std::uint64_t from_index = std::countr_zero(bb_from);
		const std::uint64_t to_index = from_index + DIFF_SQUARE[DIRECTIONS::UP];
		dest[count++].m = (std::uint16_t)(from_index + (to_index << 6) + (DIRECTIONS::UP << 12)) |
			((bb_opponent_red & (1ULL << to_index)) ? CAPTURE_RED_FLAG : 0) |
			((bb_opponent_blue & (1ULL << to_index)) ? CAPTURE_BLUE_FLAG : 0);
	}
	// down
	for (std::uint64_t bb_from = ((bb_movable >> 8) & bb_piece); bb_from; bb_from &= (bb_from - 1)) {
		const std::uint64_t from_index = std::countr_zero(bb_from);
		const std::uint64_t to_index = from_index + DIFF_SQUARE[DIRECTIONS::DOWN];
		dest[count++].m = (std::uint16_t)(from_index + (to_index << 6) + (DIRECTIONS::DOWN << 12)) |
			((bb_opponent_red & (1ULL << to_index)) ? CAPTURE_RED_FLAG : 0) |
			((bb_opponent_blue & (1ULL << to_index)) ? CAPTURE_BLUE_FLAG : 0);
	}
	// left
	for (std::uint64_t bb_from = ((bb_movable << 1) & bb_piece); bb_from; bb_from &= (bb_from - 1)) {
		const std::uint64_t from_index = std::countr_zero(bb_from);
		const std::uint64_t to_index = from_index + DIFF_SQUARE[DIRECTIONS::LEFT];
		dest[count++].m = (std::uint16_t)(from_index + (to_index << 6) + (DIRECTIONS::LEFT << 12)) |
			((bb_opponent_red & (1ULL << to_index)) ? CAPTURE_RED_FLAG : 0) |
			((bb_opponent_blue & (1ULL << to_index)) ? CAPTURE_BLUE_FLAG : 0);
	}
	// right
	for (std::uint64_t bb_from = ((bb_movable >> 1) & bb_piece); bb_from; bb_from &= (bb_from - 1)) {
		const std::uint64_t from_index = std::countr_zero(bb_from);
		const std::uint64_t to_index = from_index + DIFF_SQUARE[DIRECTIONS::RIGHT];
		dest[count++].m = (std::uint16_t)(from_index + (to_index << 6) + (DIRECTIONS::RIGHT << 12)) |
			((bb_opponent_red & (1ULL << to_index)) ? CAPTURE_RED_FLAG : 0) |
			((bb_opponent_blue & (1ULL << to_index)) ? CAPTURE_BLUE_FLAG : 0);
	}

	return count;
}


perfect_information_geister::perfect_information_geister(
	player_board player,
	player_board opponent) noexcept
	: bb_player(std::move(player)), bb_opponent(std::move(opponent))
{}

void perfect_information_geister::do_move(const move m, perfect_information_geister& position) noexcept {
	// apply move
	position.bb_player.bb_piece ^= (1ULL << m.get_from()) | (1ULL << m.get_to());

	const bool moving_is_blue = (position.bb_player.bb_blue & (1ULL << m.get_from())) != 0;

	position.bb_player.bb_blue &= ~(1ULL << m.get_from());
	position.bb_player.bb_red &= ~(1ULL << m.get_from());

	position.bb_player.bb_blue |= moving_is_blue ? (1ULL << m.get_to()) : 0;
	position.bb_player.bb_red |= moving_is_blue ? 0 : (1ULL << m.get_to());

	position.bb_opponent.bb_piece &= ~(1ULL << m.get_to());
	position.bb_opponent.bb_blue &= ~(1ULL << m.get_to());
	position.bb_opponent.bb_red &= ~(1ULL << m.get_to());

	// swap side to move (bit reverse + swap boards)
	position.bb_player.bb_piece = bit_reverse64(position.bb_player.bb_piece);
	position.bb_player.bb_blue = bit_reverse64(position.bb_player.bb_blue);
	position.bb_player.bb_red = bit_reverse64(position.bb_player.bb_red);
	position.bb_opponent.bb_piece = bit_reverse64(position.bb_opponent.bb_piece);
	position.bb_opponent.bb_blue = bit_reverse64(position.bb_opponent.bb_blue);
	position.bb_opponent.bb_red = bit_reverse64(position.bb_opponent.bb_red);

	std::swap(position.bb_player, position.bb_opponent);
}

bool perfect_information_geister::is_immediate_win() const noexcept {
	// called at turn start of side-to-move.
	if (bb_player.bb_red == 0) return true; // opponent captured all our reds => opponent loses
	if (bb_opponent.bb_blue == 0) return true; // we captured all opponent blues
	if (bb_player.bb_blue & ((1ULL << POSITIONS::A1) | (1ULL << POSITIONS::F1))) return true; // escape available => DTW=1
	return false;
}

bool perfect_information_geister::is_immediate_loss() const noexcept {
	// called at turn start of side-to-move.
	if (bb_player.bb_blue == 0) return true; // opponent captured all our blues
	if (bb_opponent.bb_red == 0) return true; // we captured all opponent reds (bad)
	return false;
}

// -----------------------------------------------------------------------------
// Sanity checks
// -----------------------------------------------------------------------------

static_assert(std::popcount(bb_board) == 36);
static_assert(bb_initial_self ==
	(
		(1ULL << POSITIONS::B5) |
		(1ULL << POSITIONS::C5) |
		(1ULL << POSITIONS::D5) |
		(1ULL << POSITIONS::E5) |
		(1ULL << POSITIONS::B6) |
		(1ULL << POSITIONS::C6) |
		(1ULL << POSITIONS::D6) |
		(1ULL << POSITIONS::E6)
		));
