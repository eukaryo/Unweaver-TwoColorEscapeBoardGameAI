#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <random>

import geister_core;
import geister_rank;

#ifdef GEISTER_ENABLE_TRIPLET_RANK_TESTS
import geister_rank_triplet;
#endif

consteval auto make_board_bits36() {
	std::array<uint64_t, 36> bits{};
	uint64_t m = bb_board;
	int idx = 0;
	while (m) {
		const unsigned p = std::countr_zero(m);
		bits[idx++] = 1ULL << p;
		m &= (m - 1);
	}
	if (idx != 36) throw "compile-time failure (at least in C++20, C++23)";
	return bits;
}
inline constexpr auto BOARD_BITS36 = make_board_bits36();

inline void assert_valid_position(
	uint64_t bb_player_blue, uint64_t bb_player_red,
	uint64_t bb_opponent_blue, uint64_t bb_opponent_red)
{
	if ((bb_player_blue & ~bb_board) != 0) throw std::exception{};
	if ((bb_player_red & ~bb_board) != 0) throw std::exception{};
	if ((bb_opponent_blue & ~bb_board) != 0) throw std::exception{};
	if ((bb_opponent_red & ~bb_board) != 0) throw std::exception{};

	if ((bb_player_blue & bb_player_red) != 0) throw std::exception{};
	if ((bb_player_blue & bb_opponent_blue) != 0) throw std::exception{};
	if ((bb_player_blue & bb_opponent_red) != 0) throw std::exception{};
	if ((bb_player_red & bb_opponent_blue) != 0) throw std::exception{};
	if ((bb_player_red & bb_opponent_red) != 0) throw std::exception{};
	if ((bb_opponent_blue & bb_opponent_red) != 0) throw std::exception{};
}

inline std::array<uint64_t, 4> make_random_geister_position(
	std::mt19937_64& rng,
	int pop_pb, int pop_pr, int pop_ob, int pop_or)
{
	if (!(1 <= pop_pb && pop_pb <= 4)) throw std::exception{};
	if (!(1 <= pop_pr && pop_pr <= 4)) throw std::exception{};
	if (!(1 <= pop_ob && pop_ob <= 4)) throw std::exception{};
	if (!(1 <= pop_or && pop_or <= 4)) throw std::exception{};

	std::array<uint64_t, 36> bits = BOARD_BITS36;
	std::shuffle(bits.begin(), bits.end(), rng);

	uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
	int pos = 0;

	for (int i = 0; i < pop_pb; ++i) bb_player_blue |= bits[pos++];
	for (int i = 0; i < pop_pr; ++i) bb_player_red |= bits[pos++];
	for (int i = 0; i < pop_ob; ++i) bb_opponent_blue |= bits[pos++];
	for (int i = 0; i < pop_or; ++i) bb_opponent_red |= bits[pos++];

	assert_valid_position(bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);
	if (std::popcount(bb_player_blue) != pop_pb) throw std::exception{};
	if (std::popcount(bb_player_red) != pop_pr) throw std::exception{};
	if (std::popcount(bb_opponent_blue) != pop_ob) throw std::exception{};
	if (std::popcount(bb_opponent_red) != pop_or) throw std::exception{};

	return { bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red };
}

// ============================================================
//  movegen / do_move consistency test (random)
//  - Tests module: geister_core
// ============================================================

inline void check_player_board_invariants(const player_board& b) {
	if ((b.bb_piece & ~bb_board) != 0) throw std::exception{};
	if ((b.bb_red & ~bb_board) != 0) throw std::exception{};
	if ((b.bb_blue & ~bb_board) != 0) throw std::exception{};
	if ((b.bb_red & b.bb_blue) != 0) throw std::exception{};
	if (b.bb_piece != (b.bb_red | b.bb_blue)) throw std::exception{};
}

inline void check_position_invariants(const perfect_information_geister& pos) {
	check_player_board_invariants(pos.bb_player);
	check_player_board_invariants(pos.bb_opponent);

	if ((pos.bb_player.bb_piece & pos.bb_opponent.bb_piece) != 0) throw std::exception{};

	assert_valid_position(
		pos.bb_player.bb_blue, pos.bb_player.bb_red,
		pos.bb_opponent.bb_blue, pos.bb_opponent.bb_red
	);
}

inline perfect_information_geister make_position_from_4bbs(
	uint64_t bb_player_blue, uint64_t bb_player_red,
	uint64_t bb_opponent_blue, uint64_t bb_opponent_red)
{
	perfect_information_geister pos{
		player_board{bb_player_red, bb_player_blue},
		player_board{bb_opponent_red, bb_opponent_blue}
	};
	check_position_invariants(pos);
	return pos;
}

void test_movegen_domove_random(const int TRIALS = 200000) {
	std::mt19937_64 rng(1);
	std::uniform_int_distribution<int> dist_pop(1, 4);

	std::array<move, 32> moves{};

	for (int t = 0; t < TRIALS; ++t) {
		const int pop_pb = dist_pop(rng);
		const int pop_pr = dist_pop(rng);
		const int pop_ob = dist_pop(rng);
		const int pop_or = dist_pop(rng);

		const auto pos4 = make_random_geister_position(rng, pop_pb, pop_pr, pop_ob, pop_or);
		const uint64_t bb_player_blue0 = pos4[0];
		const uint64_t bb_player_red0 = pos4[1];
		const uint64_t bb_opponent_blue0 = pos4[2];
		const uint64_t bb_opponent_red0 = pos4[3];

		const perfect_information_geister pos = make_position_from_4bbs(
			bb_player_blue0, bb_player_red0, bb_opponent_blue0, bb_opponent_red0);

		const int n = pos.bb_player.gen_moves(pos.bb_opponent.bb_red, pos.bb_opponent.bb_blue, moves);
		if (!(0 <= n && n <= 32)) throw std::exception{};

		const int total_before =
			std::popcount(bb_player_blue0) + std::popcount(bb_player_red0) +
			std::popcount(bb_opponent_blue0) + std::popcount(bb_opponent_red0);

		for (int i = 0; i < n; ++i) {
			const move m = moves[i];

			const int from = (int)m.get_from();
			const int to = (int)m.get_to();
			const int dir = (int)m.get_direction();

			if (!(0 <= from && from < 64)) throw std::exception{};
			if (!(0 <= to && to < 64)) throw std::exception{};
			if (!(0 <= dir && dir < 4)) throw std::exception{};

			const uint64_t bb_from = 1ULL << from;
			const uint64_t bb_to = 1ULL << to;

			if ((pos.bb_player.bb_piece & bb_from) == 0) throw std::exception{};
			if ((bb_board & bb_to) == 0) throw std::exception{};
			if ((pos.bb_player.bb_piece & bb_to) != 0) throw std::exception{};

			if (to != from + DIFF_SQUARE[dir]) throw std::exception{};

			const bool capR = (pos.bb_opponent.bb_red & bb_to) != 0;
			const bool capB = (pos.bb_opponent.bb_blue & bb_to) != 0;
			if (capR && capB) throw std::exception{};

			if (m.if_capture_red() != capR) throw std::exception{};
			if (m.if_capture_blue() != capB) throw std::exception{};
			if (m.if_capture_any() != (capR || capB)) throw std::exception{};

			// reference update
			uint64_t bb_player_blue1 = bb_player_blue0, bb_player_red1 = bb_player_red0;
			uint64_t bb_opponent_blue1 = bb_opponent_blue0, bb_opponent_red1 = bb_opponent_red0;

			const bool moving_is_blue = (bb_player_blue1 & bb_from) != 0;

			bb_player_blue1 &= ~bb_from;
			bb_player_red1 &= ~bb_from;
			if (moving_is_blue) bb_player_blue1 |= bb_to;
			else                bb_player_red1 |= bb_to;

			bb_opponent_blue1 &= ~bb_to;
			bb_opponent_red1 &= ~bb_to;

			const uint64_t ref_new_player_blue = bit_reverse64(bb_opponent_blue1);
			const uint64_t ref_new_player_red = bit_reverse64(bb_opponent_red1);
			const uint64_t ref_new_opponent_blue = bit_reverse64(bb_player_blue1);
			const uint64_t ref_new_opponent_red = bit_reverse64(bb_player_red1);

			perfect_information_geister next = pos;
			perfect_information_geister::do_move(m, next);

			check_position_invariants(next);

			const int total_after =
				std::popcount(next.bb_player.bb_piece) + std::popcount(next.bb_opponent.bb_piece);

			const int expected_after = total_before - ((capR || capB) ? 1 : 0);
			if (total_after != expected_after) throw std::exception{};

			if (next.bb_player.bb_blue != ref_new_player_blue) throw std::exception{};
			if (next.bb_player.bb_red != ref_new_player_red) throw std::exception{};
			if (next.bb_opponent.bb_blue != ref_new_opponent_blue) throw std::exception{};
			if (next.bb_opponent.bb_red != ref_new_opponent_red) throw std::exception{};
		}
	}
}

// ============================================================
//  canonicalize (LR-mirror) helper for tests
//  - Uses geister.rank's rank/unrank canonical domain.
// ============================================================

[[nodiscard]] inline uint64_t mirror_lr_u64(uint64_t x) noexcept {
	// Left-right mirror on embedded 8x8: reverse bits within each byte.
	// (bit i in a byte maps to bit 7-i). Our 6x6 board lives in columns 1..6,
	// so this maps col1<->6, col2<->5, col3<->4 (and swaps unused col0<->7).
	x = ((x & 0x5555555555555555ULL) << 1) | ((x >> 1) & 0x5555555555555555ULL);
	x = ((x & 0x3333333333333333ULL) << 2) | ((x >> 2) & 0x3333333333333333ULL);
	x = ((x & 0x0F0F0F0F0F0F0F0FULL) << 4) | ((x >> 4) & 0x0F0F0F0F0F0F0F0FULL);
	return x;
}

inline void canonicalize_lr_pos4(
	uint64_t& bb_player_blue, uint64_t& bb_player_red,
	uint64_t& bb_opponent_blue, uint64_t& bb_opponent_red)
{
	const int pop_pb = std::popcount(bb_player_blue);
	const int pop_pr = std::popcount(bb_player_red);
	const int pop_ob = std::popcount(bb_opponent_blue);
	const int pop_or = std::popcount(bb_opponent_red);

	const uint64_t code = rank_geister_perfect_information(
		bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

	uint64_t cbp = 0, cpr = 0, cob = 0, cor = 0;
	unrank_geister_perfect_information(
		code, pop_pb, pop_pr, pop_ob, pop_or,
		cbp, cpr, cob, cor);

	bb_player_blue = cbp;
	bb_player_red = cpr;
	bb_opponent_blue = cob;
	bb_opponent_red = cor;
}

// ============================================================
//  Rank/Unrank tests (material domain)
// ============================================================

void test_rank_unrank() {
	// bijection: id <-> (pb,pr,ob,or)
	std::array<uint8_t, DOMAIN_TOTAL> seen{};
	seen.fill(0);

	for (uint16_t id = 0; id < DOMAIN_TOTAL; ++id) {
		const Count4 c = unrank_material_configuration(id);

		if (!(1 <= c.pop_pb && c.pop_pb <= MAX_PER_TYPE)) throw std::exception{};
		if (!(1 <= c.pop_pr && c.pop_pr <= MAX_PER_TYPE)) throw std::exception{};
		if (!(1 <= c.pop_ob && c.pop_ob <= MAX_PER_TYPE)) throw std::exception{};
		if (!(1 <= c.pop_or && c.pop_or <= MAX_PER_TYPE)) throw std::exception{};

		const uint16_t back = rank_material_configuration(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
		if (back != id) throw std::exception{};

		if (seen[id]) throw std::exception{};
		seen[id] = 1;
	}

	// prefix sanity: x_upto_total(N) counts tuples with total pieces <= N
	for (int N = 0; N <= MAX_TOTAL; ++N) {
		uint16_t cnt = 0;
		for (int pb = 1; pb <= MAX_PER_TYPE; ++pb) {
			for (int pr = 1; pr <= MAX_PER_TYPE; ++pr) {
				for (int ob = 1; ob <= MAX_PER_TYPE; ++ob) {
					for (int oc = 1; oc <= MAX_PER_TYPE; ++oc) {
						const int sum = pb + pr + ob + oc;
						if (sum <= N) ++cnt;
					}
				}
			}
		}
		if (x_upto_total(N) != cnt) throw std::exception{};
	}
}

// ============================================================
//  Canonical domain size test (Burnside)
// ============================================================

[[nodiscard]] inline uint64_t binom_u64(int n, int k) noexcept {
	if (k < 0 || k > n) return 0;
	if (k == 0 || k == n) return 1;
	k = std::min(k, n - k);

	uint64_t r = 1;
	// n <= 36, so exact division is always safe in uint64_t.
	for (int i = 1; i <= k; ++i) {
		r = (r * (uint64_t)(n - k + i)) / (uint64_t)i;
	}
	return r;
}

[[nodiscard]] inline uint64_t full_total_states_for_counts(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	int rem = 36;
	uint64_t t = 1;
	t *= binom_u64(rem, pop_pb); rem -= pop_pb;
	t *= binom_u64(rem, pop_pr); rem -= pop_pr;
	t *= binom_u64(rem, pop_ob); rem -= pop_ob;
	t *= binom_u64(rem, pop_or);
	return t;
}

[[nodiscard]] inline uint64_t fixed_by_mirror_states(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	if ((pop_pb & 1) || (pop_pr & 1) || (pop_ob & 1) || (pop_or & 1)) return 0;

	int a = pop_pb / 2;
	int b = pop_pr / 2;
	int c = pop_ob / 2;
	int d = pop_or / 2;

	int rem = 18;
	uint64_t t = 1;
	t *= binom_u64(rem, a); rem -= a;
	t *= binom_u64(rem, b); rem -= b;
	t *= binom_u64(rem, c); rem -= c;
	t *= binom_u64(rem, d);
	return t;
}

void test_canonical_state_counts() {
	for (int pb = 1; pb <= 4; ++pb) {
		for (int pr = 1; pr <= 4; ++pr) {
			for (int ob = 1; ob <= 4; ++ob) {
				for (int oc = 1; oc <= 4; ++oc) {
					const uint64_t full = full_total_states_for_counts(pb, pr, ob, oc);
					const uint64_t fixed = fixed_by_mirror_states(pb, pr, ob, oc);
					const uint64_t canon = total_states_for_counts(pb, pr, ob, oc);

					// Burnside: 2*canon == full + fixed
					const uint64_t rhs = full + fixed;
					if ((rhs & 1ULL) != 0ULL) throw std::exception{};
					if (canon * 2 != rhs) throw std::exception{};
				}
			}
		}
	}
}

// ============================================================
//  Rank/Unrank tests
// ============================================================

void test_geister_rank_unrank_random(const int TRIALS = 200000) {
	std::mt19937_64 rng(2);
	std::uniform_int_distribution<int> dist_pop(1, 4);

	for (int t = 0; t < TRIALS; ++t) {
		const int pop_pb = dist_pop(rng);
		const int pop_pr = dist_pop(rng);
		const int pop_ob = dist_pop(rng);
		const int pop_or = dist_pop(rng);

		auto pos = make_random_geister_position(rng, pop_pb, pop_pr, pop_ob, pop_or);
		uint64_t bb_player_blue_1 = pos[0], bb_player_red_1 = pos[1];
		uint64_t bb_opponent_blue_1 = pos[2], bb_opponent_red_1 = pos[3];

		// canonicalize reference
		uint64_t cbp = bb_player_blue_1, cpr = bb_player_red_1, cob = bb_opponent_blue_1, cor = bb_opponent_red_1;
		canonicalize_lr_pos4(cbp, cpr, cob, cor);

		const uint64_t code = rank_geister_perfect_information(
			bb_player_blue_1, bb_player_red_1, bb_opponent_blue_1, bb_opponent_red_1);

		// rank is invariant under mirror
		const uint64_t code_m = rank_geister_perfect_information(
			mirror_lr_u64(bb_player_blue_1), mirror_lr_u64(bb_player_red_1),
			mirror_lr_u64(bb_opponent_blue_1), mirror_lr_u64(bb_opponent_red_1));
		if (code != code_m) throw std::exception{};

		uint64_t bb_player_blue_2 = 0, bb_player_red_2 = 0, bb_opponent_blue_2 = 0, bb_opponent_red_2 = 0;
		unrank_geister_perfect_information(
			code, pop_pb, pop_pr, pop_ob, pop_or,
			bb_player_blue_2, bb_player_red_2, bb_opponent_blue_2, bb_opponent_red_2);

		// unrank(rank(p)) == canonicalize(p)
		if (bb_player_blue_2 != cbp) throw std::exception{};
		if (bb_player_red_2 != cpr) throw std::exception{};
		if (bb_opponent_blue_2 != cob) throw std::exception{};
		if (bb_opponent_red_2 != cor) throw std::exception{};

		// rank(unrank(code)) == code
		const uint64_t back = rank_geister_perfect_information(
			bb_player_blue_2, bb_player_red_2, bb_opponent_blue_2, bb_opponent_red_2);
		if (back != code) throw std::exception{};
	}
}

void test_geister_rank_unrank_exhaustive_total(int max_total_pieces = 4) {
	for (int pop_pb = 1; pop_pb <= 4; ++pop_pb) {
		for (int pop_pr = 1; pop_pr <= 4; ++pop_pr) {
			for (int pop_ob = 1; pop_ob <= 4; ++pop_ob) {
				for (int pop_or = 1; pop_or <= 4; ++pop_or) {
					const int total_pieces = pop_pb + pop_pr + pop_ob + pop_or;
					if (total_pieces > max_total_pieces) continue;

					const uint64_t total = total_states_for_counts(pop_pb, pop_pr, pop_ob, pop_or);

					for (uint64_t number = 0; number < total; ++number) {
						uint64_t bb_player_blue = 0, bb_player_red = 0, bb_opponent_blue = 0, bb_opponent_red = 0;
						unrank_geister_perfect_information(
							number, pop_pb, pop_pr, pop_ob, pop_or,
							bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);

						assert_valid_position(bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);
						if (std::popcount(bb_player_blue) != pop_pb) throw std::exception{};
						if (std::popcount(bb_player_red) != pop_pr) throw std::exception{};
						if (std::popcount(bb_opponent_blue) != pop_ob) throw std::exception{};
						if (std::popcount(bb_opponent_red) != pop_or) throw std::exception{};

						// must be canonical already
						uint64_t cbp = bb_player_blue, cpr = bb_player_red, cob = bb_opponent_blue, cor = bb_opponent_red;
						canonicalize_lr_pos4(cbp, cpr, cob, cor);
						if (cbp != bb_player_blue || cpr != bb_player_red || cob != bb_opponent_blue || cor != bb_opponent_red) {
							throw std::exception{};
						}

						const uint64_t back = rank_geister_perfect_information(
							bb_player_blue, bb_player_red, bb_opponent_blue, bb_opponent_red);
						if (back != number) throw std::exception{};
					}
				}
			}
		}
	}
}



// ============================================================
//  Triplet rank/unrank tests (3 labeled sets, LR-canonical)
//  - Tests module: geister_rank_triplet
//  Enabled when GEISTER_ENABLE_TRIPLET_RANK_TESTS is defined.
// ============================================================

#ifdef GEISTER_ENABLE_TRIPLET_RANK_TESTS

inline void assert_valid_triplet(uint64_t A, uint64_t B, uint64_t C) {
	if ((A & ~bb_board) != 0) throw std::exception{};
	if ((B & ~bb_board) != 0) throw std::exception{};
	if ((C & ~bb_board) != 0) throw std::exception{};
	if ((A & B) != 0) throw std::exception{};
	if ((A & C) != 0) throw std::exception{};
	if ((B & C) != 0) throw std::exception{};
}

inline std::array<uint64_t, 3> make_random_triplet_position(
	std::mt19937_64& rng,
	int pop_a, int pop_b, int pop_c)
{
	if (!(0 <= pop_a && pop_a <= kTripletRankMaxCount)) throw std::exception{};
	if (!(0 <= pop_b && pop_b <= kTripletRankMaxCount)) throw std::exception{};
	if (!(0 <= pop_c && pop_c <= kTripletRankMaxCount)) throw std::exception{};
	if (pop_a + pop_b + pop_c > 36) throw std::exception{};

	std::array<uint64_t, 36> bits = BOARD_BITS36;
	std::shuffle(bits.begin(), bits.end(), rng);

	uint64_t A = 0, B = 0, C = 0;
	int pos = 0;
	for (int i = 0; i < pop_a; ++i) A |= bits[pos++];
	for (int i = 0; i < pop_b; ++i) B |= bits[pos++];
	for (int i = 0; i < pop_c; ++i) C |= bits[pos++];

	assert_valid_triplet(A, B, C);
	if (std::popcount(A) != pop_a) throw std::exception{};
	if (std::popcount(B) != pop_b) throw std::exception{};
	if (std::popcount(C) != pop_c) throw std::exception{};
	return { A, B, C };
}

[[nodiscard]] inline uint64_t full_total_states_for_counts3(int a, int b, int c) noexcept {
	int rem = 36;
	uint64_t t = 1;
	t *= binom_u64(rem, a); rem -= a;
	t *= binom_u64(rem, b); rem -= b;
	t *= binom_u64(rem, c);
	return t;
}

[[nodiscard]] inline uint64_t fixed_by_mirror_states3(int a, int b, int c) noexcept {
	if ((a & 1) || (b & 1) || (c & 1)) return 0;
	int aa = a / 2;
	int bb = b / 2;
	int cc = c / 2;
	int rem = 18;
	uint64_t t = 1;
	t *= binom_u64(rem, aa); rem -= aa;
	t *= binom_u64(rem, bb); rem -= bb;
	t *= binom_u64(rem, cc);
	return t;
}

void test_triplet_canonical_state_counts() {
	for (int a = 0; a <= kTripletRankMaxCount; ++a) {
		for (int b = 0; b <= kTripletRankMaxCount; ++b) {
			for (int c = 0; c <= kTripletRankMaxCount; ++c) {
				const uint64_t full = full_total_states_for_counts3(a, b, c);
				const uint64_t fixed = fixed_by_mirror_states3(a, b, c);
				const uint64_t rhs = full + fixed;
				if ((rhs & 1ULL) != 0ULL) throw std::exception{};
				const uint64_t canon = rhs / 2;

				const uint64_t got = states_for_counts((uint8_t)a, (uint8_t)b, (uint8_t)c);
				if (got != canon) throw std::exception{};
			}
		}
	}
}

void test_triplet_rank_unrank_random_numbers(const int TRIALS = 200000) {
	std::mt19937_64 rng(10);
	std::uniform_int_distribution<int> dist_pop(0, kTripletRankMaxCount);

	for (int t = 0; t < TRIALS; ++t) {
		const int a = dist_pop(rng);
		const int b = dist_pop(rng);
		const int c = dist_pop(rng);

		const uint64_t total = states_for_counts((uint8_t)a, (uint8_t)b, (uint8_t)c);
		if (total == 0) throw std::exception{};
		const uint64_t number = rng() % total;

		uint64_t A = 0, B = 0, C = 0;
		unrank_triplet_canon(number, (uint8_t)a, (uint8_t)b, (uint8_t)c, A, B, C);
		assert_valid_triplet(A, B, C);
		if (std::popcount(A) != a) throw std::exception{};
		if (std::popcount(B) != b) throw std::exception{};
		if (std::popcount(C) != c) throw std::exception{};

		const uint64_t back = rank_triplet_canon(A, B, C);
		if (back != number) throw std::exception{};

		// rank is invariant under LR mirror
		const uint64_t back_m = rank_triplet_canon(mirror_lr_u64(A), mirror_lr_u64(B), mirror_lr_u64(C));
		if (back_m != number) throw std::exception{};
	}
}

void test_triplet_rank_unrank_random_positions(const int TRIALS = 200000) {
	std::mt19937_64 rng(11);
	std::uniform_int_distribution<int> dist_pop(0, kTripletRankMaxCount);

	for (int t = 0; t < TRIALS; ++t) {
		const int a = dist_pop(rng);
		const int b = dist_pop(rng);
		const int c = dist_pop(rng);

		const auto trip = make_random_triplet_position(rng, a, b, c);
		const uint64_t A = trip[0];
		const uint64_t B = trip[1];
		const uint64_t C = trip[2];

		const uint64_t code = rank_triplet_canon(A, B, C);

		// rank is invariant under LR mirror
		const uint64_t code_m = rank_triplet_canon(mirror_lr_u64(A), mirror_lr_u64(B), mirror_lr_u64(C));
		if (code != code_m) throw std::exception{};

		// unrank(rank(p)) stays in the same equivalence class (canonical rep)
		uint64_t A2 = 0, B2 = 0, C2 = 0;
		unrank_triplet_canon(code, (uint8_t)a, (uint8_t)b, (uint8_t)c, A2, B2, C2);
		assert_valid_triplet(A2, B2, C2);
		if (rank_triplet_canon(A2, B2, C2) != code) throw std::exception{};
	}
}

#endif // GEISTER_ENABLE_TRIPLET_RANK_TESTS
namespace {

template <class Fn>
void run_one(const char* name, Fn&& fn) {
    using clock = std::chrono::steady_clock;
    const auto t0 = clock::now();
    std::cerr << "[TEST] " << name << " ... " << std::flush;
    fn();
    const auto t1 = clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cerr << "ok (" << ms << " ms)" << "\n";
}

} // namespace

int main() {
    try {
        run_one("material rank/unrank", [] { test_rank_unrank(); });
        run_one("canonical domain size (Burnside)", [] { test_canonical_state_counts(); });
        run_one("movegen/do_move random", [] { test_movegen_domove_random(); });
        run_one("geister rank/unrank random", [] { test_geister_rank_unrank_random(); });
        run_one("geister rank/unrank exhaustive (total<=5)", [] { test_geister_rank_unrank_exhaustive_total(5); });
#ifdef GEISTER_ENABLE_TRIPLET_RANK_TESTS
        run_one("triplet canonical domain size (Burnside)", [] { test_triplet_canonical_state_counts(); });
        run_one("triplet rank/unrank random numbers", [] { test_triplet_rank_unrank_random_numbers(); });
        run_one("triplet rank/unrank random positions", [] { test_triplet_rank_unrank_random_positions(); });
#endif
        std::cerr << "All blackbox tests passed.\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAILED with std::exception: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "FAILED with unknown exception.\n";
        return 1;
    }
}
