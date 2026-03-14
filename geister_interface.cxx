module;

#include <bit>
#include <cassert>
#include <cstdint>
#include <optional>

export module geister_interface;

import geister_core;

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Board-observation snapshot from the current player's POV.
//
// This is the minimal information we assume the player function can observe:
//   - My pieces are fully known by color.
//   - Opponent pieces on-board are known by location only (unknown color).
//   - The number of opponent red pieces already captured is known (0..3).
//
// NOTE:
//   The caller is responsible for not calling player functions on terminal
//   positions (escape available, captured-all conditions, etc.).
//   Helpers below are provided for convenience.
export struct board_observation {
	std::uint64_t bb_my_blue = 0;
	std::uint64_t bb_my_red = 0;
	std::uint64_t bb_opponent_unknown = 0;
	std::uint8_t pop_captured_opponent_red = 0;
};

// A protocol-level move representation.
//
// We intentionally model the two escape moves explicitly in the same shape as
// normal moves:
//   - (from = A1, dir = LEFT)
//   - (from = F1, dir = RIGHT)
//
// This makes it easy for a caller (e.g., server/protocol layer) to express an
// escape action without forcing internal move generators to include off-board
// destinations.
export struct protocol_move {
	std::uint8_t from = 0;  // 0..63 (8x8 index)
	std::uint8_t dir = 0;   // DIRECTIONS (0..3)

	protocol_move() noexcept = default;
	constexpr protocol_move(std::uint8_t from_sq, std::uint8_t direction) noexcept
		: from(from_sq), dir(direction) {}

	[[nodiscard]] constexpr bool is_escape() const noexcept {
		return (from == static_cast<std::uint8_t>(POSITIONS::A1) && dir == static_cast<std::uint8_t>(DIRECTIONS::LEFT))
			|| (from == static_cast<std::uint8_t>(POSITIONS::F1) && dir == static_cast<std::uint8_t>(DIRECTIONS::RIGHT));
	}

	[[nodiscard]] constexpr std::uint8_t to() const noexcept {
		return static_cast<std::uint8_t>(static_cast<int>(from) + static_cast<int>(DIFF_SQUARE[dir & 3U]));
	}
};

// Convenience: check whether a square index is inside the 6x6 playable board.
export [[nodiscard]] bool is_board_square(std::uint64_t sq) noexcept;

// Escape is available for the side to move if a blue piece sits on A1 or F1.
export [[nodiscard]] bool escape_available(std::uint64_t bb_my_blue) noexcept;
export [[nodiscard]] bool escape_available(const board_observation& obs) noexcept;

// Convert between geister_core::move (on-board move only) and protocol_move.
//
// Notes:
//   - to_protocol_move never produces an escape move.
//   - to_core_move returns nullopt for escape moves and for any off-board move.
export [[nodiscard]] protocol_move to_protocol_move(move m) noexcept;
export [[nodiscard]] std::optional<move> to_core_move(protocol_move pm) noexcept;

// Infer opponent remaining/captured counts from (bb_opponent_unknown, k).
// This is useful for terminal detection in the caller.
export struct opponent_material {
	std::uint8_t remaining_total = 0;
	std::uint8_t remaining_blue = 0;
	std::uint8_t remaining_red = 0;
	std::uint8_t captured_total = 0;
	std::uint8_t captured_blue = 0;
	std::uint8_t captured_red = 0;
};

export [[nodiscard]] opponent_material infer_opponent_material(
	std::uint64_t bb_opponent_unknown,
	int pop_captured_opponent_red) noexcept;

export [[nodiscard]] opponent_material infer_opponent_material(const board_observation& obs) noexcept;

module:private;

// -----------------------------------------------------------------------------
// Implementations
// -----------------------------------------------------------------------------

[[nodiscard]] bool is_board_square(const std::uint64_t sq) noexcept {
	if (sq >= 64) return false;
	return (bb_board & (1ULL << sq)) != 0;
}

[[nodiscard]] bool escape_available(const std::uint64_t bb_my_blue) noexcept {
	return (bb_my_blue & ((1ULL << POSITIONS::A1) | (1ULL << POSITIONS::F1))) != 0;
}

[[nodiscard]] bool escape_available(const board_observation& obs) noexcept {
	return escape_available(obs.bb_my_blue);
}

[[nodiscard]] protocol_move to_protocol_move(const move m) noexcept {
	return protocol_move(
		static_cast<std::uint8_t>(m.get_from()),
		static_cast<std::uint8_t>(m.get_direction()));
}

[[nodiscard]] std::optional<move> to_core_move(const protocol_move pm) noexcept {
	// Validate direction range.
	if (pm.dir > 3) return std::nullopt;
	if (!is_board_square(pm.from)) return std::nullopt;

	// Escape moves are intentionally not representable as geister_core::move.
	if (pm.is_escape()) return std::nullopt;

	const std::uint64_t to_sq = static_cast<std::uint64_t>(pm.to());
	if (!is_board_square(to_sq)) return std::nullopt;

	move m;
	m.m = static_cast<std::uint16_t>(pm.from + (to_sq << 6) + (static_cast<std::uint64_t>(pm.dir) << 12));
	return m;
}

[[nodiscard]] opponent_material infer_opponent_material(
	const std::uint64_t bb_opponent_unknown,
	const int pop_captured_opponent_red) noexcept
{
	opponent_material out{};

	const int remaining_total = static_cast<int>(std::popcount(bb_opponent_unknown));
	const int captured_total = 8 - remaining_total;
	const int captured_red = pop_captured_opponent_red;
	const int captured_blue = captured_total - captured_red;
	const int remaining_red = 4 - captured_red;
	const int remaining_blue = 4 - captured_blue;

	// Saturate to 0 on any negative intermediate (indicates invalid input).
	out.remaining_total = static_cast<std::uint8_t>((remaining_total < 0) ? 0 : remaining_total);
	out.captured_total = static_cast<std::uint8_t>((captured_total < 0) ? 0 : captured_total);
	out.captured_red = static_cast<std::uint8_t>((captured_red < 0) ? 0 : captured_red);
	out.captured_blue = static_cast<std::uint8_t>((captured_blue < 0) ? 0 : captured_blue);
	out.remaining_red = static_cast<std::uint8_t>((remaining_red < 0) ? 0 : remaining_red);
	out.remaining_blue = static_cast<std::uint8_t>((remaining_blue < 0) ? 0 : remaining_blue);

	return out;
}

[[nodiscard]] opponent_material infer_opponent_material(const board_observation& obs) noexcept {
	return infer_opponent_material(obs.bb_opponent_unknown, static_cast<int>(obs.pop_captured_opponent_red));
}
