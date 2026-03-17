module;

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <utility>

// BMI2 intrinsics: _pext_u64 / _pdep_u64
#include <immintrin.h>

export module geister_rank;

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

export inline constexpr int MAX_PER_TYPE = 4;                 // max pieces of each color per side
export inline constexpr int RADIX        = MAX_PER_TYPE;
export inline constexpr int MAX_TOTAL    = 4 * MAX_PER_TYPE;
export inline constexpr int DOMAIN_TOTAL = RADIX * RADIX * RADIX * RADIX; // 4^4 = 256

/// Material counts for a perfect-information Geister state *from the side-to-move perspective*.
///
/// Meaning in game theory:
///   pop_pb/pop_pr : #blue/#red of the player to move
///   pop_ob/pop_or : #blue/#red of the opponent
///
/// Meaning in code:
///   Defines the domain of a tablebase file (index space of rank/unrank).
export struct Count4 {
	std::uint8_t pop_pb, pop_pr, pop_ob, pop_or;
};

/// Rank a material configuration (pb,pr,ob,or) into [0, DOMAIN_TOTAL).
export [[nodiscard]] std::uint16_t rank_material_configuration(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept;

/// Unrank a material configuration id into (pb,pr,ob,or).
export [[nodiscard]] Count4 unrank_material_configuration(std::uint16_t id) noexcept;

/// #tuples whose total piece count is <= N.
export [[nodiscard]] std::uint16_t x_upto_total(int N) noexcept;

/// (pb,pr,ob,or) -> (ob,or,pb,pr)
export [[nodiscard]] Count4 swap_material(const Count4& c) noexcept;

export [[nodiscard]] std::uint16_t material_id_of(const Count4& c) noexcept;

/// Canonical (LR-mirror quotient) number of positions for the given piece counts.
export [[nodiscard]] std::uint64_t total_states_for_counts(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept;

export [[nodiscard]] std::uint64_t states_of(const Count4& c) noexcept;

/// Rank a perfect-information 4-bitboard position into the canonical LR-mirror domain.
export [[nodiscard]] std::uint64_t rank_geister_perfect_information(
	std::uint64_t bb_player_blue,
	std::uint64_t bb_player_red,
	std::uint64_t bb_opponent_blue,
	std::uint64_t bb_opponent_red) noexcept;

/// Unrank a canonical LR-mirror index into the corresponding bitboards.
export void unrank_geister_perfect_information(
	std::uint64_t number,
	int pop_pb, int pop_pr, int pop_ob, int pop_or,
	std::uint64_t& bb_player_blue,
	std::uint64_t& bb_player_red,
	std::uint64_t& bb_opponent_blue,
	std::uint64_t& bb_opponent_red) noexcept;

module:private;

// -----------------------------------------------------------------------------
// Internal constants (6x6 board embedded in 8x8)
// -----------------------------------------------------------------------------

namespace {

consteval std::uint64_t make_mask_cols(int c_lo, int c_hi_inclusive) {
	std::uint64_t m = 0;
	for (int r = 1; r <= 6; ++r) {
		for (int c = c_lo; c <= c_hi_inclusive; ++c) {
			const int sq = 8 * r + c;
			m |= (1ULL << sq);
		}
	}
	return m;
}

inline constexpr std::uint64_t bb_board = make_mask_cols(1, 6);
inline constexpr std::uint64_t bb_left  = make_mask_cols(1, 3);
inline constexpr std::uint64_t bb_right = make_mask_cols(4, 6);

static_assert(std::popcount(bb_board) == 36);
static_assert(std::popcount(bb_left) == 18);
static_assert(std::popcount(bb_right) == 18);
static_assert((bb_left & bb_right) == 0);
static_assert((bb_left | bb_right) == bb_board);

// -----------------------------------------------------------------------------
// Compile-time nCk table (N up to 36)
// -----------------------------------------------------------------------------

constexpr int MAXN = 36;

consteval auto make_comb_table() {
	std::array<std::array<std::uint64_t, MAXN + 1>, MAXN + 1> C{};
	for (int n = 0; n <= MAXN; ++n) {
		C[n][0] = 1;
		C[n][n] = 1;
		for (int k = 1; k < n; ++k) {
			C[n][k] = C[n - 1][k - 1] + C[n - 1][k];
		}
	}
	return C;
}

inline constexpr auto COMB = make_comb_table();

// -----------------------------------------------------------------------------
// Rank/Unrank among N-bit patterns (colex), 0-based.
// Here S <= 4 is assumed (Geister piece count constraint).
// -----------------------------------------------------------------------------

[[nodiscard]] inline std::uint64_t rank_patterns_colex(std::uint64_t x, [[maybe_unused]] const int N, const int S) noexcept {
	assert(0 <= S && S <= 4);
	assert(S <= N && N <= MAXN);
	assert(std::popcount(x) == S);

	if (S == 0) return 0;

	std::uint64_t rank = 0;
	for (int i = 1; x; x &= x - 1) {
		const int p = std::countr_zero(x);
		rank += COMB[p][i++];
	}
	return rank;
}

[[nodiscard]] inline std::uint64_t unrank_patterns_colex(std::uint64_t r, int N, int S) noexcept {
	assert(0 <= S && S <= 4);
	assert(S <= N && N <= MAXN);
	assert(r < COMB[N][S]);

	if (S == 0) return 0;

	std::uint64_t x = 0;
	int hi = N - 1;

	for (int i = S; i >= 2; --i) {
		int p = hi;
		while (COMB[p][i] > r) --p;
		x |= 1ULL << p;
		r -= COMB[p][i];
		hi = p - 1;
	}

	// i=1 => C(p,1)=p
	x |= 1ULL << static_cast<int>(r);
	return x;
}

// -----------------------------------------------------------------------------
// Left-right mirror on embedded 8x8: reverse bits within each byte.
// Maps file A<->H, B<->G, C<->F, D<->E (rank unchanged).
// -----------------------------------------------------------------------------

constexpr std::array<std::uint8_t, 256> make_rev8_table() {
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

// -----------------------------------------------------------------------------
// Half-board ranking/unranking (18 squares) for 4 disjoint sets
// -----------------------------------------------------------------------------

constexpr int HALF_N = 18;

[[nodiscard]] inline int split_index5(int a, int b, int c, int d) noexcept {
	return ((a * 5 + b) * 5 + c) * 5 + d;
}

[[nodiscard]] inline std::uint64_t ways_half_counts(int a, int b, int c, int d) noexcept {
	assert(0 <= a && a <= 4);
	assert(0 <= b && b <= 4);
	assert(0 <= c && c <= 4);
	assert(0 <= d && d <= 4);
	assert(a + b + c + d <= HALF_N);

	int rem = HALF_N;
	std::uint64_t t = 1;
	t *= COMB[rem][a]; rem -= a;
	t *= COMB[rem][b]; rem -= b;
	t *= COMB[rem][c]; rem -= c;
	t *= COMB[rem][d];
	return t;
}

[[nodiscard]] inline std::uint64_t rank_half4(
	std::uint64_t A, std::uint64_t B, std::uint64_t C, std::uint64_t D,
	const std::uint64_t mask18) noexcept
{
	assert((A & ~mask18) == 0);
	assert((B & ~mask18) == 0);
	assert((C & ~mask18) == 0);
	assert((D & ~mask18) == 0);
	assert((A & B) == 0 && (A & C) == 0 && (A & D) == 0);
	assert((B & C) == 0 && (B & D) == 0);
	assert((C & D) == 0);

	const int a = std::popcount(A);
	const int b = std::popcount(B);
	const int c = std::popcount(C);
	const int d = std::popcount(D);

	std::uint64_t m = mask18;
	int pop_m = HALF_N;

	std::uint64_t r = rank_patterns_colex(_pext_u64(A, m), pop_m, a);
	pop_m -= a; m ^= A;
	r *= COMB[pop_m][b];
	r += rank_patterns_colex(_pext_u64(B, m), pop_m, b);
	pop_m -= b; m ^= B;
	r *= COMB[pop_m][c];
	r += rank_patterns_colex(_pext_u64(C, m), pop_m, c);
	pop_m -= c; m ^= C;
	r *= COMB[pop_m][d];
	r += rank_patterns_colex(_pext_u64(D, m), pop_m, d);

	return r;
}

inline void unrank_half4(
	std::uint64_t number,
	int a, int b, int c, int d,
	const std::uint64_t mask18,
	std::uint64_t& A, std::uint64_t& B, std::uint64_t& C, std::uint64_t& D) noexcept
{
	assert(0 <= a && a <= 4);
	assert(0 <= b && b <= 4);
	assert(0 <= c && c <= 4);
	assert(0 <= d && d <= 4);
	assert(a + b + c + d <= HALF_N);

	const int rem1 = HALF_N - a;
	const int rem2 = rem1 - b;
	const int rem3 = rem2 - c;

	const std::uint64_t radix_b = COMB[rem1][b];
	const std::uint64_t radix_c = COMB[rem2][c];
	const std::uint64_t radix_d = COMB[rem3][d];

	[[maybe_unused]] const std::uint64_t total = COMB[HALF_N][a] * radix_b * radix_c * radix_d;
	assert(number < total);

	std::uint64_t n = number;
	const std::uint64_t r_d = n % radix_d; n /= radix_d;
	const std::uint64_t r_c = n % radix_c; n /= radix_c;
	const std::uint64_t r_b = n % radix_b; n /= radix_b;
	const std::uint64_t r_a = n;

	std::uint64_t m = mask18;
	int pop_m = HALF_N;

	{
		const std::uint64_t comp = unrank_patterns_colex(r_a, pop_m, a);
		A = _pdep_u64(comp, m);
		m ^= A; pop_m -= a;
	}
	{
		const std::uint64_t comp = unrank_patterns_colex(r_b, pop_m, b);
		B = _pdep_u64(comp, m);
		m ^= B; pop_m -= b;
	}
	{
		const std::uint64_t comp = unrank_patterns_colex(r_c, pop_m, c);
		C = _pdep_u64(comp, m);
		m ^= C; pop_m -= c;
	}
	{
		const std::uint64_t comp = unrank_patterns_colex(r_d, pop_m, d);
		D = _pdep_u64(comp, m);
	}
}

[[nodiscard]] inline int lex_cmp4(int a1, int b1, int c1, int d1, int a2, int b2, int c2, int d2) noexcept {
	if (a1 != a2) return (a1 < a2) ? -1 : 1;
	if (b1 != b2) return (b1 < b2) ? -1 : 1;
	if (c1 != c2) return (c1 < c2) ? -1 : 1;
	if (d1 != d2) return (d1 < d2) ? -1 : 1;
	return 0;
}

// -----------------------------------------------------------------------------
// Canonical domain size under mirror equivalence (Burnside; group size 2)
// -----------------------------------------------------------------------------

[[nodiscard]] inline std::uint64_t full_total_states_for_counts(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	int rem = 36;
	std::uint64_t t = 1;
	t *= COMB[rem][pop_pb]; rem -= pop_pb;
	t *= COMB[rem][pop_pr]; rem -= pop_pr;
	t *= COMB[rem][pop_ob]; rem -= pop_ob;
	t *= COMB[rem][pop_or];
	return t;
}

[[nodiscard]] inline std::uint64_t fixed_by_mirror_states(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	const bool all_even = ((pop_pb % 2) == 0) && ((pop_pr % 2) == 0) && ((pop_ob % 2) == 0) && ((pop_or % 2) == 0);
	if (!all_even) return 0;
	return ways_half_counts(pop_pb / 2, pop_pr / 2, pop_ob / 2, pop_or / 2);
}

[[nodiscard]] inline std::uint64_t canonical_total_states_for_counts(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	const std::uint64_t full = full_total_states_for_counts(pop_pb, pop_pr, pop_ob, pop_or);
	const std::uint64_t fixed = fixed_by_mirror_states(pop_pb, pop_pr, pop_ob, pop_or);
	assert(((full + fixed) & 1ULL) == 0ULL);
	return (full + fixed) / 2;
}

// -----------------------------------------------------------------------------
// CanonMeta: prefix tables for ranking among canonical splits
// -----------------------------------------------------------------------------

struct CanonMeta {
	std::uint64_t total = 0;
	std::array<std::uint64_t, 625> base{};
	std::array<std::uint64_t, 625> block{};
	std::array<std::uint64_t, 625> WL{};
	std::array<std::uint64_t, 625> WR{};
	std::array<std::uint64_t, 5> sum_a{};
	std::array<std::uint64_t, 25> sum_ab{};
	std::array<std::uint64_t, 125> sum_abc{};
};

// (pb,pr,ob,or) -> 0..255
[[nodiscard]] constexpr int linear_index(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	return ((((pop_pb - 1) * RADIX + (pop_pr - 1)) * RADIX + (pop_ob - 1)) * RADIX + (pop_or - 1));
}

inline void fill_all_canon_meta(std::array<CanonMeta, DOMAIN_TOTAL>& out) noexcept {
	for (int pb = 1; pb <= 4; ++pb) {
		for (int pr = 1; pr <= 4; ++pr) {
			for (int ob = 1; ob <= 4; ++ob) {
				for (int oc = 1; oc <= 4; ++oc) {
					const int key = linear_index(pb, pr, ob, oc);
					CanonMeta meta{};

					for (int aL = 0; aL <= pb; ++aL) {
						for (int bL = 0; bL <= pr; ++bL) {
							for (int cL = 0; cL <= ob; ++cL) {
								for (int dL = 0; dL <= oc; ++dL) {
									const int aR = pb - aL;
									const int bR = pr - bL;
									const int cR = ob - cL;
									const int dR = oc - dL;

									const int sidx = split_index5(aL, bL, cL, dL);

									const std::uint64_t WL = ways_half_counts(aL, bL, cL, dL);
									const std::uint64_t WR = ways_half_counts(aR, bR, cR, dR);

									meta.WL[sidx] = WL;
									meta.WR[sidx] = WR;

									const int cmp = lex_cmp4(aL, bL, cL, dL, aR, bR, cR, dR);

									std::uint64_t blk = 0;
									if (cmp > 0) {
										blk = 0;
									}
									else if (cmp < 0) {
										blk = WL * WR;
									}
									else {
										blk = WL * (WL + 1) / 2;
									}

									meta.block[sidx] = blk;
									meta.sum_a[aL] += blk;
									meta.sum_ab[aL * 5 + bL] += blk;
									meta.sum_abc[aL * 25 + bL * 5 + cL] += blk;
								}
							}
						}
					}

					std::uint64_t pref = 0;
					for (int aL = 0; aL <= pb; ++aL) {
						for (int bL = 0; bL <= pr; ++bL) {
							for (int cL = 0; cL <= ob; ++cL) {
								for (int dL = 0; dL <= oc; ++dL) {
									const int sidx = split_index5(aL, bL, cL, dL);
									meta.base[sidx] = pref;
									pref += meta.block[sidx];
								}
							}
						}
					}
					meta.total = pref;

					out[key] = meta;
				}
			}
		}
	}
}

inline const std::array<CanonMeta, DOMAIN_TOTAL>& all_canon_meta() noexcept {
	struct Holder {
		std::array<CanonMeta, DOMAIN_TOTAL> META{};
		Holder() noexcept { fill_all_canon_meta(META); }
	};
	static Holder h;
	return h.META;
}

// -----------------------------------------------------------------------------
// Material rank/unrank table (domain enumeration order by total pieces then lex)
// -----------------------------------------------------------------------------

struct CountRankTable {
	std::array<std::uint16_t, DOMAIN_TOTAL> id_by_tuple;
	std::array<Count4, DOMAIN_TOTAL> tuple_by_id;
	std::array<std::uint16_t, MAX_TOTAL + 2> prefix_by_total; // prefix[t] = #tuples with sum < t
};

consteval CountRankTable make_count_rank_table() {
	CountRankTable T{};
	T.id_by_tuple.fill(0xFFFF);

	std::uint16_t id = 0;
	T.prefix_by_total[0] = 0;

	for (int sum = 4; sum <= MAX_TOTAL; ++sum) {
		for (int pop_pb = 1; pop_pb <= MAX_PER_TYPE; ++pop_pb) {
			for (int pop_pr = 1; pop_pr <= MAX_PER_TYPE; ++pop_pr) {
				for (int pop_ob = 1; pop_ob <= MAX_PER_TYPE; ++pop_ob) {
					const int pop_or = sum - pop_pb - pop_pr - pop_ob;
					if (1 <= pop_or && pop_or <= MAX_PER_TYPE) {
						const int idx = linear_index(pop_pb, pop_pr, pop_ob, pop_or);
						if (T.id_by_tuple[idx] != 0xFFFF) {
							throw "compile-time failure (CountRankTable)";
						}
						T.id_by_tuple[idx] = id;
						T.tuple_by_id[id] = Count4{
							(std::uint8_t)pop_pb, (std::uint8_t)pop_pr, (std::uint8_t)pop_ob, (std::uint8_t)pop_or
						};
						++id;
					}
				}
			}
		}
		T.prefix_by_total[sum + 1] = id;
	}

	if (id != DOMAIN_TOTAL) throw "compile-time failure (CountRankTable size)";
	for (std::uint16_t v : T.id_by_tuple) {
		if (v == 0xFFFF) throw "compile-time failure (CountRankTable holes)";
	}
	if (T.prefix_by_total[MAX_TOTAL + 1] != DOMAIN_TOTAL) throw "compile-time failure (CountRankTable prefix)";

	return T;
}

inline constexpr CountRankTable COUNT_RANK = make_count_rank_table();

} // namespace

// -----------------------------------------------------------------------------
// Exported functions (definitions)
// -----------------------------------------------------------------------------

std::uint16_t rank_material_configuration(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	assert(1 <= pop_pb && pop_pb <= MAX_PER_TYPE);
	assert(1 <= pop_pr && pop_pr <= MAX_PER_TYPE);
	assert(1 <= pop_ob && pop_ob <= MAX_PER_TYPE);
	assert(1 <= pop_or && pop_or <= MAX_PER_TYPE);
	return COUNT_RANK.id_by_tuple[linear_index(pop_pb, pop_pr, pop_ob, pop_or)];
}

Count4 unrank_material_configuration(std::uint16_t id) noexcept {
	assert(id < DOMAIN_TOTAL);
	return COUNT_RANK.tuple_by_id[id];
}

std::uint16_t x_upto_total(int N) noexcept {
	assert(0 <= N && N <= MAX_TOTAL);
	return COUNT_RANK.prefix_by_total[N + 1];
}

Count4 swap_material(const Count4& c) noexcept {
	return Count4{ c.pop_ob, c.pop_or, c.pop_pb, c.pop_pr };
}

std::uint16_t material_id_of(const Count4& c) noexcept {
	return rank_material_configuration(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
}

std::uint64_t total_states_for_counts(int pop_pb, int pop_pr, int pop_ob, int pop_or) noexcept {
	assert(1 <= pop_pb && pop_pb <= 4);
	assert(1 <= pop_pr && pop_pr <= 4);
	assert(1 <= pop_ob && pop_ob <= 4);
	assert(1 <= pop_or && pop_or <= 4);
	return canonical_total_states_for_counts(pop_pb, pop_pr, pop_ob, pop_or);
}

std::uint64_t states_of(const Count4& c) noexcept {
	return total_states_for_counts(c.pop_pb, c.pop_pr, c.pop_ob, c.pop_or);
}

std::uint64_t rank_geister_perfect_information(
	std::uint64_t bb_player_blue,
	std::uint64_t bb_player_red,
	std::uint64_t bb_opponent_blue,
	std::uint64_t bb_opponent_red) noexcept
{
	assert(bb_board == (bb_board | bb_player_blue));
	assert(bb_board == (bb_board | bb_player_red));
	assert(bb_board == (bb_board | bb_opponent_blue));
	assert(bb_board == (bb_board | bb_opponent_red));
	assert((bb_player_blue & bb_player_red) == 0);
	assert((bb_player_blue & bb_opponent_blue) == 0);
	assert((bb_player_blue & bb_opponent_red) == 0);
	assert((bb_player_red & bb_opponent_blue) == 0);
	assert((bb_player_red & bb_opponent_red) == 0);
	assert((bb_opponent_blue & bb_opponent_red) == 0);

	const int pop_pb = std::popcount(bb_player_blue);
	const int pop_pr = std::popcount(bb_player_red);
	const int pop_ob = std::popcount(bb_opponent_blue);
	const int pop_or = std::popcount(bb_opponent_red);

	assert(1 <= pop_pb && pop_pb <= 4);
	assert(1 <= pop_pr && pop_pr <= 4);
	assert(1 <= pop_ob && pop_ob <= 4);
	assert(1 <= pop_or && pop_or <= 4);

	const auto& META = all_canon_meta();
	const CanonMeta& meta = META[linear_index(pop_pb, pop_pr, pop_ob, pop_or)];
	assert(meta.total == canonical_total_states_for_counts(pop_pb, pop_pr, pop_ob, pop_or));

	// Left half (A,B,C)
	const std::uint64_t pbL = bb_player_blue & bb_left;
	const std::uint64_t prL = bb_player_red & bb_left;
	const std::uint64_t obL = bb_opponent_blue & bb_left;
	const std::uint64_t orL = bb_opponent_red & bb_left;

	// Right half mapped into left coordinates via mirror
	const std::uint64_t mb_pb = mirror_lr_u64(bb_player_blue);
	const std::uint64_t mb_pr = mirror_lr_u64(bb_player_red);
	const std::uint64_t mb_ob = mirror_lr_u64(bb_opponent_blue);
	const std::uint64_t mb_or = mirror_lr_u64(bb_opponent_red);

	const std::uint64_t pbR = mb_pb & bb_left;
	const std::uint64_t prR = mb_pr & bb_left;
	const std::uint64_t obR = mb_ob & bb_left;
	const std::uint64_t orR = mb_or & bb_left;

	const int aL = std::popcount(pbL);
	const int bL = std::popcount(prL);
	const int cL = std::popcount(obL);
	const int dL = std::popcount(orL);

	const int aR = pop_pb - aL;
	const int bR = pop_pr - bL;
	const int cR = pop_ob - cL;
	const int dR = pop_or - dL;

	const std::uint64_t rankL = rank_half4(pbL, prL, obL, orL, bb_left);
	const std::uint64_t rankR = rank_half4(pbR, prR, obR, orR, bb_left);

	// canonicalize
	const int cmp0 = lex_cmp4(aL, bL, cL, dL, aR, bR, cR, dR);

	int caL = aL, cbL = bL, ccL = cL, cdL = dL;
	std::uint64_t crL = rankL;
	std::uint64_t crR = rankR;

	if (cmp0 > 0) {
		caL = aR; cbL = bR; ccL = cR; cdL = dR;
		crL = rankR;
		crR = rankL;
	}
	else if (cmp0 == 0) {
		if (crL > crR) std::swap(crL, crR);
	}

	const int caR = pop_pb - caL;
	const int cbR = pop_pr - cbL;
	const int ccR = pop_ob - ccL;
	const int cdR = pop_or - cdL;
	const int cmp = lex_cmp4(caL, cbL, ccL, cdL, caR, cbR, ccR, cdR);
	assert(cmp <= 0);

	const int sidx = split_index5(caL, cbL, ccL, cdL);
	const std::uint64_t base = meta.base[sidx];
	const std::uint64_t WL = meta.WL[sidx];
	const std::uint64_t WR = meta.WR[sidx];

	assert(crL < WL);

	if (cmp < 0) {
		assert(crR < WR);
		const std::uint64_t within = crL * WR + crR;
		const std::uint64_t out = base + within;
		assert(out < meta.total);
		return out;
	}

	assert(WL == WR);
	assert(crL <= crR);
	assert(crR < WL);

	const std::uint64_t i = crL;
	const std::uint64_t M = WL;
	const std::uint64_t within = i * M - (i * (i - 1)) / 2 + (crR - crL);
	const std::uint64_t out = base + within;
	assert(out < meta.total);
	return out;
}

void unrank_geister_perfect_information(
	std::uint64_t number,
	int pop_pb, int pop_pr, int pop_ob, int pop_or,
	std::uint64_t& bb_player_blue,
	std::uint64_t& bb_player_red,
	std::uint64_t& bb_opponent_blue,
	std::uint64_t& bb_opponent_red) noexcept
{
	assert(1 <= pop_pb && pop_pb <= 4);
	assert(1 <= pop_pr && pop_pr <= 4);
	assert(1 <= pop_ob && pop_ob <= 4);
	assert(1 <= pop_or && pop_or <= 4);

	const auto& META = all_canon_meta();
	const CanonMeta& meta = META[linear_index(pop_pb, pop_pr, pop_ob, pop_or)];
	assert(number < meta.total);

	std::uint64_t r = number;

	int aL = 0, bL = 0, cL = 0, dL = 0;

	// choose aL
	for (int a = 0; a <= pop_pb; ++a) {
		const std::uint64_t s = meta.sum_a[a];
		if (r >= s) r -= s;
		else { aL = a; break; }
	}
	// choose bL
	for (int b = 0; b <= pop_pr; ++b) {
		const std::uint64_t s = meta.sum_ab[aL * 5 + b];
		if (r >= s) r -= s;
		else { bL = b; break; }
	}
	// choose cL
	for (int c = 0; c <= pop_ob; ++c) {
		const std::uint64_t s = meta.sum_abc[aL * 25 + bL * 5 + c];
		if (r >= s) r -= s;
		else { cL = c; break; }
	}
	// choose dL
	for (int d = 0; d <= pop_or; ++d) {
		const int sidx = split_index5(aL, bL, cL, d);
		const std::uint64_t blk = meta.block[sidx];
		if (r >= blk) r -= blk;
		else { dL = d; break; }
	}

	const int aR = pop_pb - aL;
	const int bR = pop_pr - bL;
	const int cR = pop_ob - cL;
	const int dR = pop_or - dL;

	const int cmp = lex_cmp4(aL, bL, cL, dL, aR, bR, cR, dR);
	assert(cmp <= 0);

	const int sidx = split_index5(aL, bL, cL, dL);
	const std::uint64_t WL = meta.WL[sidx];
	const std::uint64_t WR = meta.WR[sidx];
	assert(WL > 0 && WR > 0);

	std::uint64_t rankL = 0, rankR = 0;

	if (cmp < 0) {
		assert(r < WL * WR);
		rankL = r / WR;
		rankR = r % WR;
	}
	else {
		assert(WL == WR);
		const std::uint64_t M = WL;
		[[maybe_unused]] const std::uint64_t total_pairs = M * (M + 1) / 2;
		assert(r < total_pairs);

		// Find largest i such that offset(i) <= r,
		// offset(i) = i*(2*M - i + 1)/2
		std::uint64_t lo = 0, hi = WL;
		while (lo + 1 < hi) {
			const std::uint64_t mid = (lo + hi) >> 1;
			const std::uint64_t off = mid * (2 * M - mid + 1) / 2;
			if (off <= r) lo = mid; else hi = mid;
		}
		rankL = lo;
		const std::uint64_t offL = rankL * (2 * M - rankL + 1) / 2;
		rankR = rankL + (r - offL);
		assert(rankR < WL);
	}

	// decode halves on bb_left coordinates
	std::uint64_t pbL = 0, prL = 0, obL = 0, orL = 0;
	std::uint64_t pbR = 0, prR = 0, obR = 0, orR = 0;

	unrank_half4(rankL, aL, bL, cL, dL, bb_left, pbL, prL, obL, orL);
	unrank_half4(rankR, aR, bR, cR, dR, bb_left, pbR, prR, obR, orR);

	// build full board: right half is mirror of bb_left-coordinate patterns
	bb_player_blue  = pbL | mirror_lr_u64(pbR);
	bb_player_red   = prL | mirror_lr_u64(prR);
	bb_opponent_blue = obL | mirror_lr_u64(obR);
	bb_opponent_red  = orL | mirror_lr_u64(orR);

	// sanity
	assert(bb_board == (bb_board | bb_player_blue));
	assert(bb_board == (bb_board | bb_player_red));
	assert(bb_board == (bb_board | bb_opponent_blue));
	assert(bb_board == (bb_board | bb_opponent_red));
	assert((bb_player_blue & bb_player_red) == 0);
	assert((bb_player_blue & bb_opponent_blue) == 0);
	assert((bb_player_blue & bb_opponent_red) == 0);
	assert((bb_player_red & bb_opponent_blue) == 0);
	assert((bb_player_red & bb_opponent_red) == 0);
	assert((bb_opponent_blue & bb_opponent_red) == 0);
	assert(pop_pb == std::popcount(bb_player_blue));
	assert(pop_pr == std::popcount(bb_player_red));
	assert(pop_ob == std::popcount(bb_opponent_blue));
	assert(pop_or == std::popcount(bb_opponent_red));
}
