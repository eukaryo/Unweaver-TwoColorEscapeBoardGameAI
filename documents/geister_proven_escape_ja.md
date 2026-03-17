# geister_proven_escape.cxx 付属文書（日本語）

## 0. この文書の位置づけ

この文書は、`geister_proven_escape.cxx` が実装している

- 何を前提に
- 何を計算し
- 何が証明済みで
- 何は証明していないか

を明文化するための付属文書です。

ここでいう **proven** とは、「この関数が `std::optional<move>` の非 `nullopt` を返したなら、その `move` は実際に勝ち筋を持つ first move であり、偽陽性は出さない」という意味です。逆に、**完全性は主張しません**。つまり、勝ちがあるのに `nullopt` を返す偽陰性は許容します。

---

## 1. 公開 API と主張したい性質

公開 API は次です。

```cpp
export [[nodiscard]] std::optional<move> proven_escape_move(
    std::uint64_t bb_my_blue,
    std::uint64_t bb_my_red,
    std::uint64_t bb_opponent_unknown,
    int pop_captured_opponent_red,
    std::uint64_t rng_seed = 0);
```

返り値の意味は次です。

- `std::nullopt`
  - この十分条件では証明できる脱出 first move が見つからなかった。
  - 勝ちが無い、とは主張しない。
- `m.has_value()`
  - その `move m` は **実際に勝てる first move** であることを主張する。
  - ここでいう「勝てる」とは、現在手番側が `m` を初手として指した後、以後の相手応手や hidden information の実現値に依らず勝てることを意味する。

### 1.1 証明したい最終定理

この文書で示したい最終定理は次です。

> **定理（Soundness of `proven_escape_move`）**  
> `proven_escape_move` が非 `nullopt` の `move m` を返したとする。  
> さらに入力 observation が以下を満たすとする。
> 1. current player 視点の合法な observation である。
> 2. 呼び出し時点で終局ではない。特に、
>    - 自分の青は 1 枚以上ある。
>    - 自分の赤は 1 枚以上ある。
>    - 相手は少なくとも青 1 枚・赤 1 枚を残している。
>    - `A1/F1` に自分の青は乗っていない（即 escape 可ではない）。
>
> このとき、入力 observation と整合する **任意の** perfect-information 実現状態において、`m` は current player の winning move である。  
> さらに強く、hidden information に依存しない固定プラン
>
> 1. `m` を指す
> 2. その青駒 1 枚だけを、ある証明済み経路に沿って進める
> 3. 脱出口 `A1` または `F1` に着いた次の自手で protocol-level escape を行う
>
> という open-loop な方策が、相手の任意の合法応手に対して勝つ。

以下ではこの定理を補題に分けて証明する。

---

## 2. モデル化と conservative assumption

このモジュールは、「実際のゲームより相手を強く見積もる」「自分側の有利なトリックを捨てる」という保守的な sufficient condition を使う。

### 2.1 自分側の制約

1. **runner は青駒 1 枚に固定する。**  
   初手で選んだ青駒だけを走らせる。

2. **capture を使わない。**  
   初手も、それ以降の想定経路も、現在空いているマスだけを通る。

3. **現在 occupied なマスは、以後ずっと通れないものとして扱う。**  
   すなわち、初期 observation で occupied なマス
   
   $$
   B_0 := \texttt{bb\_my\_blue} \cup \texttt{bb\_my\_red} \cup \texttt{bb\_opponent\_unknown}
   $$
   
   を静的障害物とみなす。  
   したがって「駒 A をどけてから駒 B を通す」は一切考えない。

### 2.2 相手側の強化

4. **紫仮定**  
   相手盤上駒は、
   - こちらが取れば全部赤だったものとみなす（こちらに不利）
   - 相手が escape に使うなら全部青だったものとみなす（こちらに不利）

5. **相手のマス到達は Manhattan 距離で見積もる。**  
   あるマス `S` に相手が何手で到達できるかは、blocker を無視した Manhattan 距離で下から見積もる。

6. **相手の escape race も Manhattan 距離で見積もる。**  
   相手駒が `A6` または `F6` まで最短何手で着き、さらに 1 手で脱出できるかを blocker 無視で下から見積もる。

これらはすべて相手有利・こちら不利なので、このモデルで勝てるなら実際にも勝てる、というのが soundness の基本設計である。

---

## 3. 具体的な計算内容

`geister_proven_escape.cxx` の実装は、重み 1 のグラフしか扱わないので、一般の Dijkstra ではなく BFS を使っている。数学的には **unit-cost Dijkstra の特殊化** であり、soundness の中身は同じである。

### 3.1 相手のマス脅威時刻 `threat[s]`

各盤上マス `s` に対して

$$
\mathrm{threat}(s)
:= \min_{u \in U} \operatorname{Manhattan}(u,s)
$$

を計算する。ここで `U` は現在 observation 上の相手駒位置集合である。

解釈：

- `threat(s) = t` なら、「相手は最良でも自分の `t` 回目の応手で `s` に到達できる」
- よって、自分の `i` 手目に runner が `s` に居ることを許してよい十分条件は

$$
\mathrm{threat}(s) > i
$$

である。

等号ではダメである。自分が `i` 手目で `s` に着いた直後、相手の `i` 回目の応手で取られうるからである。

### 3.2 相手の最速 escape 完了ターン `opp_escape_turn`

各盤上マス `u` に対して

$$
\mathrm{esc}(u)
:= 1 + \min\{\operatorname{Manhattan}(u,A6),\operatorname{Manhattan}(u,F6)\}
$$

を定義する。最後の `+1` は、`A6/F6` に乗ったあと実際に盤外 escape する 1 手である。

実装では 8 枚のビットマスク

$$
M_1, M_2, \dots, M_8
$$

を前計算しておき、

- `u ∈ M_t`  
  $ iff \mathrm{esc}(u)=t $

となるようにしている。6x6 盤では最遠でも `7 + 1 = 8` 手なので 8 枚で足りる。

そして

$$
\mathrm{opp\_escape\_turn}
:= \min\{ t \in \{1,\dots,8\} : U \cap M_t \neq \emptyset \}
$$

とする。実装上は「最初に交差するマスク番号」を返している。

### 3.3 初手固定後の BFS 証明器

候補初手 `first` を 1 つ固定する。これは次を満たす手のみ考える。

- 自分の青駒を動かす手である
- 相手駒取りではない
- 着手先は current observation で空きマスである

`start = first.to` とする。ここから、初期空きマスだけからなる静的グラフ

$$
G_0 = (V_0, E_0)
$$

を考える。

- 頂点集合
  $$
  V_0 := \text{盤上マス} \setminus B_0
  $$
- 辺集合は 4 近傍（上下左右）

この BFS では `dist[v]` を

> 「現在局面から数えた自分の on-board move 数で、`first` を含めて `v` に着く最短手数」

として持つ。したがって

- `dist[start] = 1`
- exit square `A1/F1` に `dist = L` で着いたなら、実際の protocol-level escape はその次の自手、すなわち自分の `(L+1)` 手目に行う

という数え方になる。

BFS では、ある頂点 `v` に時刻 `dist[v]=i` で入れるのは

$$
\mathrm{threat}(v) > i
$$

の場合に限る。これにより「そのマスに着いた直後の相手応手で取られない」ことを保証する。

exit square `e` に `dist[e]=L` で到達したとき、さらに

$$
L < \mathrm{opp\_escape\_turn}
$$

が成り立つときにのみ、その初手は証明済みとみなす。

この不等式が strict なのは、こちらが exit square に着いても、実際の盤外 escape はまだ次の自手だからである。相手が `L` 回目の応手までに escape 完了できるなら race に負ける。

### 3.4 返す move

全ての合法青初手について上記 BFS 証明を試み、証明に成功したもののうち

1. `L` が最小のもの
2. `L` が同じなら `move` の内部整数表現が最小のもの

を返す。

ここで重要なのは、**soundness に必要なのは「返された手が証明済み候補集合に属している」ことだけ** であり、最短性や tie-break の取り方それ自体は soundness と無関係である、という点である。

---

## 4. 補題と proof sketch

以下では最終定理を 5 つの補題・定理に分ける。

### 補題 1（`threat` は相手到達時刻の下界である）

任意の盤上マス `s` について、`compute_opponent_threat_turns()` が計算する `threat[s]` は、実際のゲームで相手が `s` を占有できる最短応手数の**下界**である。したがって

$$
\mathrm{threat}(s) > i
$$

ならば、相手は自分の `i` 手目直後に `s` にいる runner を、その直後の相手 `i` 回目応手で取れない。

#### Proof sketch

相手駒 1 枚が `s` に到達するための合法経路長は、4 近傍移動である以上、必ず Manhattan 距離以上である。`threat[s]` は全相手駒についてその Manhattan 距離の最小値だから、実際に `s` を占有できる最短応手数も `threat[s]` 以上である。よって `threat[s] > i` なら、相手の `i` 回目応手終了時点でも `s` は占有不可能である。

---

### 補題 2（`opp_escape_turn` は相手最速 escape 完了ターンの下界である）

`fastest_opponent_escape_turn()` が返す `opp_escape_turn` は、相手が実際に escape 勝ちを完了できる最短応手数の**下界**である。

#### Proof sketch

任意の相手駒が `A6` または `F6` に着くまでの合法手数は、それぞれの Manhattan 距離以上である。さらに actual escape 完了にはそこから 1 手必要である。よって各駒ごとの `1 + min(dist to A6, dist to F6)` はその駒を escape に使う場合の下界になる。実際に escape できるのは青駒に限られるが、実装は相手全駒を候補に入れて最小値を取っているので、値はさらに相手有利・こちら不利な下界になる。

---

### 補題 3（BFS が成功したら、その初手に対応する具体的経路が存在する）

`certified_escape_length_after_first_move(first, ...)` が `L` を返したなら、

- `s_1 = first.to`
- `s_L ∈ {A1, F1}`
- 各 `s_i` は初期 observation で空きマス
- `s_i` と `s_{i+1}` は隣接
- 各 `i=1,\dots,L` について `threat[s_i] > i`
- `L < opp_escape_turn`

を満たす具体的経路 `s_1, ..., s_L` が存在する。

#### Proof sketch

BFS は `dist[start]=1` から始め、隣接空きマスへ `+1` ずつ伸ばす。あるマス `v` が `dist[v]=d` を持つのは、`d-1` の predecessor から到達しており、かつ `threat[v] > d` を満たす場合だけである。したがって、有限距離を持つ各頂点には「距離を 1 ずつ減らしていく predecessor 連鎖」が存在し、それを辿れば条件を満たす実経路が再構成できる。exit square が `L` で受理されたなら最後に `L < opp_escape_turn` も満たしている。

---

### 補題 4（補題 3 の経路に沿って走れば、runner は捕まらず、相手 escape にも先行されない）

補題 3 の経路 `s_1,\dots,s_L` を固定する。初手 `first` で `s_1` に入り、その後毎回同じ青駒を `s_i -> s_{i+1}` と進める固定プランを考える。このとき、任意の hidden information 実現状態と任意の相手応手列に対し、

1. こちらの各手は常に合法であり
2. runner は一度も取られず
3. 相手は自分の `L` 回目応手までに escape を完了できない

したがって current player は遅くとも自分の `(L+1)` 手目に escape 勝ちできる。

#### Proof sketch

`threat[s_i] > i` により、相手は `i-1` 回目応手終了時点でも `s_i` に到達できていない。よって `s_i` は自分の `i` 手目開始時に空いており、runner はそこへ合法に進める。さらに `threat[s_i] > i` により、相手はその直後の `i` 回目応手でも `s_i` を占有できず、runner を取れない。これを帰納すれば `s_L` まで安全に到達でき、さらに `threat[s_L] > L` だから `s_L` 上でも次の自手まで生存する。  
一方 `L < opp_escape_turn` と補題 2 から、相手は `L` 回目応手までに escape 完了できない。したがってこちらの `(L+1)` 手目が回り、そこで protocol-level escape を行って勝つ。

---

### 定理 5（`proven_escape_move` の soundness）

`proven_escape_move` が `move m` を返したなら、その `m` は winning move である。

#### Proof sketch

外側ループは、`certified_escape_length_after_first_move()` が成功した青初手だけを候補集合に入れる。補題 3 により、その各候補には具体的な certified path が存在する。補題 4 により、その path に沿う open-loop plan は hidden information と相手応手に依らず勝つ。したがって候補集合のどれを返しても winning move である。実装はそのうち最短のものを返しているだけなので soundness は保たれる。

---

## 5. 何は証明していないか

このモジュールが証明しているのは **soundness のみ** であり、次は証明していない。

1. **完全性は無い。**  
   勝ち筋が存在しても `nullopt` はありうる。

2. **capture を使う勝ちは拾わない。**  
   たとえば「相手を取って道をこじ開ける」「脱出口番を相打ちで消して勝つ」は、この実装では扱わない。

3. **他駒をどかして runner を通す勝ちも拾わない。**  
   初期 occupied マスを永久障害物としているため。

4. **複数 runner の協調も扱わない。**

5. **終局 observation を入力した場合の意味付けはしない。**  
   特に `bb_my_red == 0` なのに呼ぶ、といったケースは caller contract 外である。  
   `escape_available(bb_my_blue)` の場合だけは、実装は警告を出して `nullopt` を返す。

---

## Appendix A. Full proof

## A.1 定義

### 定義 A.1（Observation と整合する perfect-information 状態）

入力

$$
\mathcal O = (B,R,U,k)
$$

を

- `B = bb_my_blue`
- `R = bb_my_red`
- `U = bb_opponent_unknown`
- `k = pop_captured_opponent_red`

と書く。

perfect-information 状態

$$
\mathcal P = (B,R,U_B,U_R)
$$

が `\mathcal O` と整合するとは、

1. `U_B ∩ U_R = ∅`
2. `U_B ∪ U_R = U`
3. `|U_R| = 4-k`
4. `|U_B| = |U|-(4-k)`

が成り立つことをいう。

本書ではさらに caller contract に従い、現在局面が非終局であると仮定する。具体的には少なくとも

- `B ≠ ∅`
- `R ≠ ∅`
- `escape_available(B) = false`
- `|U_B| ≥ 1`
- `|U_R| ≥ 1`

を仮定する。

### 定義 A.2（相手マス占有時刻の optimistic lower bound）

各盤上マス `s` について

$$
\tau_{\mathrm{occ}}(s)
:= \min_{u \in U} \operatorname{Manhattan}(u,s)
$$

と定義する。実装上の `threat[s]` はこれである。

### 定義 A.3（相手 escape 完了時刻の optimistic lower bound）

各相手位置 `u ∈ U` について

$$
\tau_{\mathrm{esc}}(u)
:= 1 + \min\{\operatorname{Manhattan}(u,A6), \operatorname{Manhattan}(u,F6)\}
$$

と置き、

$$
\tau_{\mathrm{esc}}
:= \min_{u \in U} \tau_{\mathrm{esc}}(u)
$$

と定義する。実装上の `opp_escape_turn` はこれである。

### 定義 A.4（静的空きグラフ）

初期 occupied 集合を

$$
B_0 := B \cup R \cup U
$$

と置く。静的空きグラフ `G_0=(V_0,E_0)` を

- `V_0 = board \setminus B_0`
- `E_0` は 4 近傍辺

で定義する。

### 定義 A.5（`first` に対する certified path）

合法青初手 `first` の着手先を `s_1 = first.to` とする。列

$$
P = (s_1,s_2,\dots,s_L)
$$

が `first` に対する certified path であるとは、

1. 各 `s_i ∈ V_0`
2. 各 `i<L` で `s_i` と `s_{i+1}` は `G_0` で隣接
3. `s_L ∈ {A1,F1}`
4. 各 `i=1,\dots,L` で
   $$
   \tau_{\mathrm{occ}}(s_i) > i
   $$
5. 
   $$
   L < \tau_{\mathrm{esc}}
   $$

が成り立つことをいう。

---

## A.2 補題 1 の full proof

> **補題 1**  任意の盤上マス `s` について、実際のゲームで相手が `s` を占有できる最短応手数は `\tau_{\mathrm{occ}}(s)` 以上である。

### Full proof

相手位置 `u ∈ U` を 1 つ固定する。相手駒が `u` から `s` に到達する任意の合法手順を考える。

各応手では 4 近傍の 1 辺しか進めないので、1 応手で Manhattan 距離は高々 1 しか減らない。したがって `u` から `s` へ到達するには少なくとも

$$
\operatorname{Manhattan}(u,s)
$$

回の応手が必要である。blocker の存在、他駒との干渉、hidden color の制約はすべて「必要応手数を増やす方向」にしか作用しないので、この下界は常に有効である。

よって、任意の `u ∈ U` から `s` を占有するのに必要な応手数は `\operatorname{Manhattan}(u,s)` 以上である。したがって、**どの相手駒かを最適に選んでも** 必要応手数は

$$
\min_{u \in U} \operatorname{Manhattan}(u,s)
= \tau_{\mathrm{occ}}(s)
$$

以上である。

これで補題は示された。∎

---

## A.3 補題 2 の full proof

> **補題 2**  相手が実際に escape を完了できる最短応手数は `\tau_{\mathrm{esc}}` 以上である。

### Full proof

実際に相手が escape を完了したとする。そのとき escape に使われた相手駒を `u` とおく。

まず、その駒が `A6` または `F6` のどちらかへ到達しなければ protocol-level escape はできない。`u` がそのいずれかに到達するための合法経路長は、補題 1 と同じ理由で、それぞれの Manhattan 距離以上である。したがって最寄り脱出口マスに到達するまでに少なくとも

$$
\min\{\operatorname{Manhattan}(u,A6), \operatorname{Manhattan}(u,F6)\}
$$

応手を要する。

さらに、`A6/F6` に乗っただけではまだ勝ちではなく、実際の盤外 escape に 1 応手必要である。したがって駒 `u` を使った escape 完了応手数は少なくとも

$$
1 + \min\{\operatorname{Manhattan}(u,A6), \operatorname{Manhattan}(u,F6)\}
= \tau_{\mathrm{esc}}(u)
$$

である。

実際に escape に使えるのは青駒だけだが、`\tau_{\mathrm{esc}}` は `U` 上の**全駒**で最小値を取っているので、

$$
\tau_{\mathrm{esc}} = \min_{u \in U} \tau_{\mathrm{esc}}(u)
$$

は「相手にとって最も有利に見積もった」下界である。よって、実際の escape 完了最短応手数は `\tau_{\mathrm{esc}}` 以上である。∎

---

## A.4 補題 2 の補足：8 枚マスクが MECE であること

> **補題 2.1**  各盤上マス `u` に対し `\tau_{\mathrm{esc}}(u)` は 1 以上 8 以下である。  
> **補題 2.2**  マスク `M_1,\dots,M_8` は盤上マス全体の MECE 分割をなす。

### Full proof

`\tau_{\mathrm{esc}}(u) \ge 1` は定義から自明である。上界については、6x6 盤で `A6/F6` のいずれかへの最小 Manhattan 距離の最大値は 7 である。実際、`C1` や `D1` から最寄りの `A6/F6` までの距離は 7 であり、これが最大である。したがって

$$
1 \le \tau_{\mathrm{esc}}(u) \le 8.
$$

よって各盤上マスは一意にちょうど 1 つの `M_t` に属する。したがって `M_1,\dots,M_8` は盤上マス全体の MECE 分割である。∎

---

## A.5 補題 3 の full proof

> **補題 3**  `certified_escape_length_after_first_move(first, ...)` が `L` を返したなら、`first` に対する certified path が存在する。

### Full proof

BFS 内部で `dist[start]=1` と置かれる。`start` は `first.to` であり、実装は

- `start` が盤上マスであること
- `start ∉ B_0` であること
- `threat[start] > 1` であること

を確認してから探索を始める。したがって長さ 1 の部分経路 `(start)` は certified 条件を満たす。

次に、BFS によってある頂点 `v` が `dist[v]=d` で初めて確定したとき、以下を帰納法で示す。

> **帰納主張**  `v` に対して、長さ `d` の部分経路
> $$
> (s_1,\dots,s_d=v)
> $$
> が存在し、
> 1. 各 `s_i ∈ V_0`
> 2. 各隣接条件が成り立つ
> 3. 各 `i` で `threat[s_i] > i`
>
> を満たす。

基底 `d=1` は上で示した。

帰納段階を示す。`dist[v]=d` (`d\ge 2`) が設定されたのは、ある隣接頂点 `p` について

- `dist[p]=d-1`
- `v ∈ V_0`
- `threat[v] > d`

が成り立ったときだけである。帰納法の仮定により `p` までの長さ `d-1` の certified 部分経路が存在する。それに `v` を 1 頂点付け足せば、長さ `d` の経路が得られ、上の 1–3 を保つ。

よって有限距離 `dist[v]=d` を持つ全頂点について上の帰納主張が成り立つ。

BFS が `L` を返したのは、ある exit square `e ∈ {A1,F1}` が `dist[e]=L` で取り出されたときであり、返却条件としてさらに

$$
L < \tau_{\mathrm{esc}}
$$

が確認されている。したがって、上の帰納主張から `e` に至る長さ `L` の経路が存在し、かつ escape race 条件も満たす。これは定義 A.5 の certified path そのものである。∎

---

## A.6 補題 4 の full proof

> **補題 4**  `first` に対する certified path `P=(s_1,\dots,s_L)` が存在するとき、current player が
> 1. 初手 `first` を指し、
> 2. 以後は毎回同じ青駒を `s_i -> s_{i+1}` と進め、
> 3. `s_L` 上の次の自手で escape する
>
> という固定プランを採用すると、任意の整合実現状態と任意の相手応手列に対して勝つ。

### Full proof

まず、時系列を明確にする。`s_i` に runner がいるのは「自分の `i` 手目直後」である。相手はその直後に `i` 回目の応手を行う。

以下、`i=1,2,\dots,L` について帰納法で次を示す。

> **帰納主張 H(i)**
> 1. 自分の `i` 手目は合法に実行できる。
> 2. その直後、runner は `s_i` にいる。
> 3. 相手の `i` 回目応手でも runner は取られない。

### 基底 `i=1`

`first` は実装の外側ループで「自分の青駒を現在空きマスへ動かす合法 on-board move」として選別されている。したがって自分の 1 手目 `first` は合法であり、直後に runner は `s_1` にいる。

さらに certified path の条件より

$$
\tau_{\mathrm{occ}}(s_1) > 1.
$$

補題 1 により、相手は自分の 1 回目応手終了時点でも `s_1` を占有できない。したがって相手の 1 回目応手で runner は取られない。よって `H(1)` が成り立つ。

### 帰納段階

`H(i-1)` が成り立つと仮定し、`H(i)` (`i\ge 2`) を示す。

帰納法の仮定により、相手の `(i-1)` 回目応手後にも runner は `s_{i-1}` 上で生存している。したがって自分の `i` 手目開始時に runner は `s_{i-1}` にいる。

次に `s_i` が空いていることを示す。certified path の条件より

$$
\tau_{\mathrm{occ}}(s_i) > i.
$$

特に `\tau_{\mathrm{occ}}(s_i) > i-1` である。補題 1 により、相手は `(i-1)` 回目応手終了時点でも `s_i` を占有できない。  
また `s_i ∈ V_0` だから、初期時点で `s_i` に自分駒は無く、我々の固定プランでは runner 以外の自駒は一切動かさないので、`s_i` に自分の他駒が入ることもない。よって自分の `i` 手目開始時に `s_i` は空いている。

さらに `s_{i-1}` と `s_i` は certified path の定義より隣接している。したがって自分の `i` 手目として `s_{i-1} -> s_i` を合法に実行でき、直後に runner は `s_i` にいる。

最後に、相手 `i` 回目応手で runner が取られないことを示す。再び

$$
\tau_{\mathrm{occ}}(s_i) > i
$$

と補題 1 より、相手は `i` 回目応手終了時点でも `s_i` を占有できない。したがってその応手で runner を取れない。よって `H(i)` が成り立つ。

以上で全ての `i=1,\dots,L` について `H(i)` が示された。特に、自分の `L` 手目直後に runner は exit square `s_L ∈ \{A1,F1\}` におり、相手の `L` 回目応手でも取られない。

次に race 条件を示す。certified path の条件より

$$
L < \tau_{\mathrm{esc}}.
$$

補題 2 により、相手が実際に escape を完了できる最短応手数は `\tau_{\mathrm{esc}}` 以上なので、相手は `L` 回目応手までに escape 完了できない。

したがって相手の `L` 回目応手後、局面は still alive であり、自分の `(L+1)` 手目が回ってくる。その時点で runner は `A1` または `F1` にいるので protocol-level escape が可能であり、それを実行して current player は勝つ。

なお、その前に相手が自分の赤をすべて取ってしまった場合、`geister_core` のルールではそれは current player の即勝ちであるから、いずれにせよ負けにはならない。  
また current player が途中で負ける可能性は

- 自分の青が全滅する
- 相手の赤を全滅させる

のいずれかだが、前者は runner が生存するので起こらず、後者はこの固定プランが一切 capture を含まないので起こらない。

よって current player は必ず勝つ。∎

---

## A.7 定理 5 の full proof

> **定理 5**  `proven_escape_move` が `move m` を返したなら、その `m` は winning move である。

### Full proof

`proven_escape_move` の外側ループは、生成した合法 on-board move のうち、

1. 自分青駒を動かす手
2. 相手駒取りではない手

だけを候補に残す。各候補 `first` について `certified_escape_length_after_first_move(first,...)` を呼び、その返り値が `std::nullopt` でないときに限って、その `first` を「証明済み候補」として採用する。

いま `proven_escape_move` が `m` を返したとする。すると `m` は証明済み候補の 1 つである。ゆえに補題 3 により、`m` に対する certified path が存在する。さらに補題 4 により、その certified path に沿う固定プランは、任意の整合実現状態と任意の相手応手列に対して勝つ。

したがって `m` は winning move である。

実装は証明済み候補の中から `L` 最小、同点なら整数表現最小を返しているが、これは「証明済み候補集合の中からどれを選ぶか」という選択規則にすぎず、soundness を損なわない。∎

---

## A.8 まとめ

以上により、`geister_proven_escape.cxx` が返す非 `nullopt` の `move` は、

- 相手 hidden color をどのように実現しても
- 相手がどう応手しても
- current observation で見えている occupied マスを固定障害物とする保守モデルに照らしてもなお十分安全であり
- 実ゲームでも実際に勝てる first move

であることが示された。

言い換えると、このモジュール名の **proven** は、「この conservative sufficient condition のもとで non-nullopt を返した first move は偽陽性ではない」という意味で数学的に正当化されている。
