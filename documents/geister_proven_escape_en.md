# Attachment for geister_proven_escape.cxx (English)

## 0. Scope of this document

This document makes explicit

- under which assumptions `geister_proven_escape.cxx` operates,
- what it computes,
- what exactly is proved sound,
- and what is intentionally **not** proved.

Here, **proven** means:

> if the function returns a non-`nullopt` `move`, that move is a genuine winning first move; in particular, the module is designed to have **no false positives**.

The converse is **not** claimed. A winning move may exist even when the function returns `std::nullopt`; false negatives are allowed.

---

## 1. Public API and the property to be proved

The exported API is:

```cpp
export [[nodiscard]] std::optional<move> proven_escape_move(
    std::uint64_t bb_my_blue,
    std::uint64_t bb_my_red,
    std::uint64_t bb_opponent_unknown,
    int pop_captured_opponent_red,
    std::uint64_t rng_seed = 0);
```

Meaning of the return value:

- `std::nullopt`
  - No escape-race first move could be certified by this sufficient condition.
  - This does **not** mean there is no win.
- `m.has_value()`
  - The returned `move m` is claimed to be an actual winning first move.
  - “Winning” means that after playing `m`, the side to move has a winning strategy against every legal opponent continuation and every hidden-information realization consistent with the current observation.

### 1.1 Final theorem to prove

> **Theorem (Soundness of `proven_escape_move`)**  
> Assume `proven_escape_move` returns a non-`nullopt` move `m`.  
> Assume further that the input observation satisfies all of the following:
> 1. It is a legal current-player observation.
> 2. The current state is non-terminal. In particular,
>    - the current player has at least one blue piece,
>    - the current player has at least one red piece,
>    - the opponent still has at least one blue and at least one red,
>    - no current-player blue is already on `A1/F1`.
>
> Then, for **every** perfect-information realization consistent with that observation, `m` is a winning move for the side to move.  
> More strongly, there exists a hidden-information-independent open-loop plan:
>
> 1. play `m`,
> 2. keep advancing that same blue runner along a certified route,
> 3. once it reaches `A1` or `F1`, perform the protocol-level escape on the next own turn,
>
> and this plan wins against every legal opponent reply.

The rest of the document proves this theorem via lemmas.

---

## 2. Conservative modeling assumptions

The module uses a deliberately conservative sufficient condition: it strengthens the opponent and weakens our own tactical options.

### 2.1 Restrictions on our side

1. **Exactly one blue runner is chosen.**  
   After the first move, only that runner matters for the certificate.

2. **No captures are used.**  
   The first move and the subsequent route only go through squares that are empty in the current observation.

3. **All currently occupied squares are treated as permanently blocked.**  
   Let

   $$
   B_0 := \texttt{bb\_my\_blue} \cup \texttt{bb\_my\_red} \cup \texttt{bb\_opponent\_unknown}.
   $$

   The route search never enters any square in `B_0`, even if that square may later become empty in the real game.  
   Hence ideas such as “move blocker A away, then pass runner B through” are intentionally ignored.

### 2.2 Strengthening the opponent

4. **Purple assumption.**  
   Every opponent on-board piece is treated as dangerous:
   - if we capture it, assume it was red (bad for us),
   - if the opponent uses it for escape, assume it was blue (bad for us).

5. **Opponent square-reachability is estimated by Manhattan distance.**  
   Blockers are ignored; the opponent is allowed the most optimistic geometric lower bound.

6. **Opponent escape-race time is also estimated by Manhattan distance.**  
   The opponent is granted the optimistic minimum time to reach `A6/F6`, plus one extra turn for the actual off-board escape.

All of these choices are opponent-favouring. Therefore, if we still win under this pessimistic model, then we also win in the real game.

---

## 3. What the implementation computes

Since every edge has unit cost, the implementation uses BFS rather than general Dijkstra. Mathematically, this is exactly **Dijkstra specialized to a unit-weight graph**, so the soundness argument is the same.

### 3.1 Opponent threat time `threat[s]`

For each board square `s`, the implementation computes

$$
\mathrm{threat}(s)
:= \min_{u \in U} \operatorname{Manhattan}(u,s),
$$

where `U` is the set of currently observed opponent piece locations.

Interpretation:

- `threat(s)=t` means: under the optimistic opponent model, some opponent piece could be on `s` by its `t`-th reply.
- Therefore, a sufficient condition for allowing our runner to be on `s` right after our `i`-th move is

$$
\mathrm{threat}(s) > i.
$$

Equality is unsafe: if we arrive on `s` on our `i`-th move and `threat(s) \le i`, then the opponent may occupy `s` on its `i`-th reply and capture the runner immediately.

### 3.2 Opponent fastest escape-completion turn `opp_escape_turn`

For each board square `u`, define

$$
\mathrm{esc}(u)
:= 1 + \min\{\operatorname{Manhattan}(u,A6),\operatorname{Manhattan}(u,F6)\}.
$$

The final `+1` is the actual off-board escape move after reaching `A6/F6`.

The implementation precomputes 8 bitmasks

$$
M_1, M_2, \dots, M_8,
$$

such that

$$
u \in M_t \iff \mathrm{esc}(u)=t.
$$

On a 6x6 board, 8 masks are enough because the largest possible value is `7 + 1 = 8`.

Then the module computes

$$
\mathrm{opp\_escape\_turn}
:= \min\{ t \in \{1,\dots,8\} : U \cap M_t \neq \emptyset \}.
$$

Operationally: it scans the masks from `t=1` upward and returns the first index that intersects the current opponent bitboard.

### 3.3 BFS certificate after fixing the first move

Fix a candidate first move `first`. The module only considers first moves that are

- moves of one of our blue pieces,
- non-capturing,
- landing on a square that is empty in the current observation.

Let `start = first.to`. From there, the module searches in the static-empty graph

$$
G_0 = (V_0,E_0),
$$

where

- $ V_0 := \text{board squares} \setminus B_0 $,
- `E_0` are the orthogonal adjacency edges.

The BFS stores `dist[v]` as

> the minimum number of our **on-board** moves from the current position to reach `v`, counting `first` as move 1.

Hence:

- `dist[start] = 1`,
- if an exit square `A1/F1` is reached with `dist = L`, then the actual protocol-level escape would occur on our next turn, i.e. on our `(L+1)`-th move from now.

A vertex `v` is only accepted at time `dist[v]=i` if

$$
\mathrm{threat}(v) > i.
$$

This guarantees that the runner is not capturable immediately after arriving at `v`.

When an exit square `e` is reached with `dist[e]=L`, the certificate only succeeds if

$$
L < \mathrm{opp\_escape\_turn}.
$$

The inequality is strict because merely reaching `A1/F1` is not yet an escape win; the actual off-board escape happens only on our next turn.

### 3.4 Which move is returned

The function tries every legal blue first move, runs the BFS certificate above, and collects the certified ones. Among them it returns

1. a move with minimum certified route length `L`,
2. and among ties, the move with minimum internal integer encoding.

For soundness, the important fact is simply:

> every returned move belongs to the set of certified candidates.

The shortest-path preference and the tie-break rule do not contribute to soundness.

---

## 4. Lemmas and proof sketches

We now split the final theorem into five formal statements.

### Lemma 1 (`threat` is a lower bound on true opponent occupation time)

For every board square `s`, the quantity `threat[s]` computed by `compute_opponent_threat_turns()` is a **lower bound** on the minimum number of opponent replies needed to occupy `s` in the real game. Therefore, if

$$
\mathrm{threat}(s) > i,
$$

then an opponent cannot capture a runner sitting on `s` immediately after our `i`-th move.

#### Proof sketch

A single opponent piece must move along a 4-neighbour path. Any such path has length at least the Manhattan distance. `threat[s]` is the minimum Manhattan distance over all currently observed opponent pieces, so the true minimum occupation time is at least `threat[s]`. Hence `threat[s] > i` implies `s` is still unreachable after the opponent’s `i`-th reply.

---

### Lemma 2 (`opp_escape_turn` is a lower bound on the true opponent escape-completion time)

The value returned by `fastest_opponent_escape_turn()` is a **lower bound** on the minimum number of opponent replies needed to actually complete an escape win.

#### Proof sketch

For any opponent piece, reaching `A6` or `F6` requires at least the Manhattan distance to the nearer exit square, and the actual off-board escape requires one additional reply. Taking the minimum over all currently observed opponent pieces is therefore an optimistic, opponent-favouring lower bound. Since actual escape is only possible with a blue piece, minimizing over **all** opponent pieces only makes the estimate even more pessimistic for us, which is safe for soundness.

---

### Lemma 3 (if the BFS succeeds, a concrete certified path exists)

If `certified_escape_length_after_first_move(first, ...)` returns `L`, then there exists a concrete route

- `s_1 = first.to`,
- `s_L ∈ {A1, F1}`,
- each `s_i` is empty in the initial observation,
- each consecutive pair is adjacent,
- `threat[s_i] > i` for all `i=1,\dots,L`,
- `L < opp_escape_turn`.

#### Proof sketch

BFS starts with `dist[start]=1` and only propagates to a neighbour `v` if `v` is statically empty and `threat[v] > dist[parent]+1`. Therefore every finite-distance vertex has a predecessor chain whose distances decrease by 1 at each step, and this chain reconstructs a real route satisfying the safety inequality at every arrival time. If an exit square is accepted with value `L`, the implementation additionally checks `L < opp_escape_turn` before returning.

---

### Lemma 4 (following a certified path is legal, safe, and outruns opponent escape)

Fix a certified path `s_1,\dots,s_L` from Lemma 3. Suppose we play `first` to `s_1`, and on every later own turn keep moving the same blue runner from `s_i` to `s_{i+1}`. Then, for every hidden-information realization and every legal opponent reply sequence,

1. every one of our runner moves is legal,
2. the runner is never captured,
3. the opponent cannot finish escape by its `L`-th reply,

and therefore we win no later than our `(L+1)`-th move by executing the protocol-level escape.

#### Proof sketch

Because `threat[s_i] > i`, the opponent cannot occupy `s_i` by the end of its `i`-th reply. In particular, the square is empty before our `i`-th move and remains uncapturable immediately after we arrive there. By induction, the runner safely reaches `s_L` and survives the opponent’s `L`-th reply. Since `L < opp_escape_turn`, Lemma 2 implies that the opponent cannot have escaped before our next turn. Thus our `(L+1)`-th move is available and is the winning off-board escape.

---

### Theorem 5 (soundness of `proven_escape_move`)

If `proven_escape_move` returns a move `m`, then `m` is a winning move.

#### Proof sketch

The outer loop only keeps blue non-capturing first moves for which `certified_escape_length_after_first_move()` succeeds. By Lemma 3, each such move has a concrete certified continuation path. By Lemma 4, that path yields a winning open-loop plan against every legal opponent response and every hidden-information realization. Therefore every returned move is winning. The implementation merely chooses one certified move among them.

---

## 5. What is **not** proved

The module proves **soundness only**. It does **not** prove any of the following:

1. **No completeness guarantee.**  
   A win may exist even when the function returns `nullopt`.

2. **Wins requiring captures are ignored.**  
   For example: clearing the gatekeeper by capture, sacrificing into a favorable exchange, or forcing a capture sequence.

3. **Wins requiring moving other friendly pieces out of the way are ignored.**  
   This is a direct consequence of the static-blocker assumption.

4. **No multi-runner coordination is considered.**

5. **No semantics are assigned to terminal inputs.**  
   In particular, calling the function when `bb_my_red == 0` is outside the intended caller contract.  
   The implementation explicitly warns and returns `nullopt` only for the special case where `escape_available(bb_my_blue)` already holds.

---

## Appendix A. Full proofs

## A.1 Definitions

### Definition A.1 (Perfect-information realization consistent with the observation)

Write the input as

$$
\mathcal O = (B,R,U,k),
$$

where

- `B = bb_my_blue`,
- `R = bb_my_red`,
- `U = bb_opponent_unknown`,
- `k = pop_captured_opponent_red`.

A perfect-information realization

$$
\mathcal P = (B,R,U_B,U_R)
$$

is **consistent** with `\mathcal O` if

1. `U_B ∩ U_R = ∅`,
2. `U_B ∪ U_R = U`,
3. `|U_R| = 4-k`,
4. `|U_B| = |U|-(4-k)`.

We additionally assume the current state is non-terminal, as required by the caller contract. In particular,

- `B ≠ ∅`,
- `R ≠ ∅`,
- `escape_available(B) = false`,
- `|U_B| ≥ 1`,
- `|U_R| ≥ 1`.

### Definition A.2 (Optimistic lower bound on opponent square occupation time)

For each board square `s`, define

$$
\tau_{\mathrm{occ}}(s)
:= \min_{u \in U} \operatorname{Manhattan}(u,s).
$$

This is exactly the implementation’s `threat[s]`.

### Definition A.3 (Optimistic lower bound on opponent escape-completion time)

For each opponent piece location `u ∈ U`, define

$$
\tau_{\mathrm{esc}}(u)
:= 1 + \min\{\operatorname{Manhattan}(u,A6), \operatorname{Manhattan}(u,F6)\}.
$$

Then define

$$
\tau_{\mathrm{esc}}
:= \min_{u \in U} \tau_{\mathrm{esc}}(u).
$$

This is exactly the implementation’s `opp_escape_turn`.

### Definition A.4 (Static-empty graph)

Let

$$
B_0 := B \cup R \cup U.
$$

Define the static-empty graph `G_0=(V_0,E_0)` by

- `V_0 = board \setminus B_0`,
- `E_0` is orthogonal adjacency.

### Definition A.5 (Certified path for a first move)

Let `first` be a legal blue non-capturing first move, and let `s_1 = first.to`. A sequence

$$
P = (s_1,s_2,\dots,s_L)
$$

is a **certified path** for `first` if

1. every `s_i ∈ V_0`,
2. `s_i` and `s_{i+1}` are adjacent in `G_0`,
3. `s_L ∈ {A1,F1}`,
4. for every `i=1,\dots,L`,
   $$
   \tau_{\mathrm{occ}}(s_i) > i,
   $$
5. 
   $$
   L < \tau_{\mathrm{esc}}.
   $$

---

## A.2 Full proof of Lemma 1

> **Lemma 1.** For any board square `s`, the true minimum number of opponent replies required to occupy `s` is at least `\tau_{\mathrm{occ}}(s)`.

### Full proof

Fix one opponent piece location `u ∈ U`. Consider any legal sequence of opponent moves that brings that piece from `u` to `s`.

Each reply moves the piece by one orthogonal step. Therefore the Manhattan distance to `s` can decrease by at most 1 per reply. Consequently, any legal route from `u` to `s` must have length at least

$$
\operatorname{Manhattan}(u,s).
$$

Blockers, interference by other pieces, and hidden-color constraints can only increase the required number of replies, never decrease it.

Hence, for every `u ∈ U`, the true minimum number of replies needed for that piece to occupy `s` is at least `\operatorname{Manhattan}(u,s)`. Taking the minimum over all observed opponent pieces yields

$$
\min_{u \in U} \operatorname{Manhattan}(u,s)
= \tau_{\mathrm{occ}}(s).
$$

Therefore the true minimum number of opponent replies needed to occupy `s` is at least `\tau_{\mathrm{occ}}(s)`. This proves the lemma. ∎

---

## A.3 Full proof of Lemma 2

> **Lemma 2.** The true minimum number of opponent replies needed to complete an escape win is at least `\tau_{\mathrm{esc}}`.

### Full proof

Assume the opponent eventually completes an escape. Let `u` be the piece used for that escape.

Before the off-board escape can occur, the piece must first reach either `A6` or `F6`. By the same geometric argument as in Lemma 1, reaching those squares requires at least the respective Manhattan distances. Hence the number of replies needed to reach one of the two exit squares is at least

$$
\min\{\operatorname{Manhattan}(u,A6), \operatorname{Manhattan}(u,F6)\}.
$$

After reaching `A6/F6`, one additional reply is required for the actual off-board escape. Therefore the total number of opponent replies needed when using piece `u` is at least

$$
1 + \min\{\operatorname{Manhattan}(u,A6), \operatorname{Manhattan}(u,F6)\}
= \tau_{\mathrm{esc}}(u).
$$

Actual escape is only possible with a blue piece, but the implementation minimizes over **all** observed opponent pieces. Therefore

$$
\tau_{\mathrm{esc}} = \min_{u \in U} \tau_{\mathrm{esc}}(u)
$$

is an opponent-favouring lower bound. Consequently, the true minimum escape-completion time is at least `\tau_{\mathrm{esc}}`. ∎

---

## A.4 Supplement to Lemma 2: why 8 masks are MECE

> **Lemma 2.1.** For every board square `u`, the value `\tau_{\mathrm{esc}}(u)` lies in `{1,2,\dots,8}`.  
> **Lemma 2.2.** The masks `M_1,\dots,M_8` form a mutually exclusive and collectively exhaustive partition of the 36 playable board squares.

### Full proof

The lower bound `\tau_{\mathrm{esc}}(u) \ge 1` is immediate from the definition.

For the upper bound, note that on the 6x6 board, the largest possible value of

$$
\min\{\operatorname{Manhattan}(u,A6), \operatorname{Manhattan}(u,F6)\}
$$

is 7. This maximum is attained, for example, at `C1` or `D1`. After adding one extra move for the actual off-board escape, the maximum becomes 8.

Thus each board square belongs to exactly one class `t ∈ {1,\dots,8}`. Therefore the masks `M_1,\dots,M_8` form a mutually exclusive and collectively exhaustive partition of the board. ∎

---

## A.5 Full proof of Lemma 3

> **Lemma 3.** If `certified_escape_length_after_first_move(first, ...)` returns `L`, then there exists a certified path for `first`.

### Full proof

Inside the BFS, the algorithm starts with `dist[start]=1`, where `start = first.to`. Before doing so, it checks that

- `start` is a board square,
- `start \notin B_0`,
- `threat[start] > 1`.

Hence the one-vertex path `(start)` already satisfies the certified conditions for length 1.

We now prove by induction on the assigned distance that every vertex `v` with finite `dist[v]=d` has a concrete path

$$
(s_1,\dots,s_d=v)
$$

such that

1. each `s_i ∈ V_0`,
2. each consecutive pair is adjacent,
3. `threat[s_i] > i` for all `i=1,\dots,d`.

**Base case `d=1`.** This is exactly the initialization above.

**Inductive step.** Suppose the claim holds for every vertex whose finite distance is at most `d-1`, and let `v` be a vertex that is first assigned `dist[v]=d` with `d\ge 2`.

The BFS can assign `dist[v]=d` only from a neighbour `p` satisfying

- `dist[p]=d-1`,
- `v ∈ V_0`,
- `threat[v] > d`.

By the inductive hypothesis, `p` already has a concrete path

$$
(s_1,\dots,s_{d-1}=p)
$$

satisfying the stated conditions. Appending `v` yields a path

$$
(s_1,\dots,s_{d-1},s_d=v)
$$

which still satisfies all three conditions.

Therefore the claim holds for every finite-distance vertex.

If the function returns `L`, this happens when an exit square `e ∈ \{A1,F1\}` is popped with `dist[e]=L`, and the function additionally checks

$$
L < \tau_{\mathrm{esc}}.
$$

Hence the reconstructed path to `e` is exactly a certified path in the sense of Definition A.5. ∎

---

## A.6 Full proof of Lemma 4

> **Lemma 4.** If `P=(s_1,\dots,s_L)` is a certified path for `first`, then the fixed plan “play `first`, then keep moving the same runner along `P`, then escape from `s_L` on the next own turn” is winning against every legal opponent continuation in every consistent realization.

### Full proof

We index time so that the runner is on `s_i` immediately after our `i`-th move. The opponent then makes its `i`-th reply.

We prove by induction on `i=1,2,\dots,L` the following claim.

> **Induction claim H(i).**
> 1. Our `i`-th move is legal.
> 2. Immediately afterwards, the runner is on `s_i`.
> 3. The opponent’s `i`-th reply does not capture the runner.

### Base case `i=1`

The outer loop of `proven_escape_move` only considers `first` if it is a legal on-board move of one of our blue pieces to a currently empty square. Therefore our first move is legal, and the runner is on `s_1` immediately afterwards.

Since `P` is certified,

$$
\tau_{\mathrm{occ}}(s_1) > 1.
$$

By Lemma 1, the opponent cannot occupy `s_1` by the end of its first reply. Therefore the runner is not captured on that reply. Hence `H(1)` holds.

### Inductive step

Assume `H(i-1)` holds for some `i\ge 2`. Then after the opponent’s `(i-1)`-st reply, the runner is still alive on `s_{i-1}`. So our `i`-th move starts from `s_{i-1}`.

We first show that `s_i` is empty at that moment. Since `P` is certified,

$$
\tau_{\mathrm{occ}}(s_i) > i.
$$

In particular, `\tau_{\mathrm{occ}}(s_i) > i-1`. By Lemma 1, the opponent cannot occupy `s_i` by the end of its `(i-1)`-st reply.

Also, `s_i ∈ V_0`, so `s_i` was not occupied by any of our pieces initially. Under the fixed plan, we never move any friendly piece except the runner itself, so no other friendly piece can move onto `s_i` either.

Therefore `s_i` is empty at the start of our `i`-th move. Since consecutive vertices of `P` are adjacent, the move `s_{i-1} -> s_i` is legal, and immediately afterwards the runner is on `s_i`.

We now show that the opponent’s `i`-th reply does not capture the runner. Again, since `P` is certified,

$$
\tau_{\mathrm{occ}}(s_i) > i.
$$

By Lemma 1, the opponent cannot occupy `s_i` by the end of its `i`-th reply. Hence the runner cannot be captured there. Therefore `H(i)` holds.

By induction, `H(i)` holds for every `i=1,\dots,L`. In particular, after our `L`-th move the runner is alive on the exit square `s_L ∈ \{A1,F1\}`, and it survives the opponent’s `L`-th reply.

Next we show that the opponent cannot have completed escape by then. Because `P` is certified,

$$
L < \tau_{\mathrm{esc}}.
$$

By Lemma 2, the true minimum opponent escape-completion time is at least `\tau_{\mathrm{esc}}`, so the opponent cannot finish escape within its first `L` replies.

Therefore the game necessarily reaches our `(L+1)`-th turn with the runner still on `A1` or `F1`. At that point the protocol-level escape is available, and executing it wins the game.

One may also ask about earlier terminal events. If the opponent captures all of our red pieces earlier, then under the project’s `geister_core` rules that is already an immediate win for us, so this cannot hurt. The two loss modes for us would be:

- all our blue pieces are gone,
- all opponent red pieces are gone.

The first cannot occur because the runner survives throughout the plan. The second cannot occur because the plan contains no captures at all.

Hence the fixed plan is winning. ∎

---

## A.7 Full proof of Theorem 5

> **Theorem 5.** If `proven_escape_move` returns a move `m`, then `m` is a winning move.

### Full proof

The outer loop of `proven_escape_move` enumerates legal on-board moves and keeps only those that

1. move one of our blue pieces,
2. are non-capturing with respect to the current observation.

For each such candidate `first`, it calls `certified_escape_length_after_first_move(first, ...)`. A candidate is accepted only if this function returns a finite value rather than `std::nullopt`.

Now suppose `proven_escape_move` returns `m`. Then `m` is one of the accepted candidates. By Lemma 3, there exists a certified path for `m`. By Lemma 4, following that certified path yields a winning plan against every legal opponent response in every perfect-information realization consistent with the observation.

Therefore `m` is a winning move.

The fact that the implementation chooses the shortest certified candidate, breaking ties by integer move encoding, only decides **which certified move** to return; it does not affect soundness. ∎

---

## A.8 Conclusion

We have shown that every non-`nullopt` move returned by `geister_proven_escape.cxx`

- remains winning for every hidden-color realization consistent with the current observation,
- remains winning against every legal opponent reply,
- is justified by a conservative model that strengthens the opponent and weakens our own tactical resources,
- and therefore is a genuine winning first move in the actual game.

Equivalently, the word **proven** in the module name is mathematically justified as:

> whenever this conservative sufficient condition certifies a move, that move is not a false positive.
