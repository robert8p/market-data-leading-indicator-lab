# Contrarian Blank-Canvas Research Methodology v2

## Purpose

This methodology broadens the empirical search space without turning **contrarian** into a trading prior.

Contrarian means: deliberately investigate structures that conventional screening pipelines often discard or average away. It does **not** mean prefer reversal, short consensus, choose surprising signs, or reward a result because it contradicts conventional wisdom.

A candidate is preferred only when the data supports it more strongly after bias controls, external replication, robustness tests and realistic economics.

**Surprise is not a score.**

## Core principles

1. Start from observable data and mechanical representations before narratives or indicators.
2. Treat sign, direction, horizon, session, market and payoff shape as unknown until evidence supports them.
3. Search conventional and unconventional structures in parallel. Contrarian families receive search coverage, not preferential promotion.
4. Preserve negative results. A failed unusual idea is an asset because it prevents rediscovery and repeated multiple-testing leakage.
5. Never repair a failed untouched holdout.
6. Separate predictive information from economic implementation. A predictor may be scientifically real and still not be tradeable.
7. Measure incremental information. A novel-looking variable that is merely a proxy for an established predictor is not a new edge.
8. Keep dependence explicit. Trade/event observations sharing a day, session, asset or source event are not automatically independent.

## Search families

Every broad research cycle should allocate search budget across the following families without assuming any family must contain an edge:

- **Conditional instability** — apparently unstable relationships that may become stable under an independently defined state.
- **Low-frequency / high-value events** — rare events where frequency is low but conditional economic magnitude may be large.
- **Asymmetric response** — positive and negative shocks, tails or disagreements may have different downstream effects.
- **Unusual lag** — predictive information may peak away from the shortest or most intuitive lag.
- **Regime sign reversal** — a relationship may legitimately reverse across regimes, but the reversal itself must replicate as an interaction.
- **Aggregation-hidden effects** — opposing cohort effects may cancel in market-wide averages.
- **Cross-sectional effects** — breadth, dispersion, ranks, leaders/laggards and subgroup disagreement may contain information absent from market means.
- **Nonlinear thresholds** — effects may be thresholded, U-shaped or tail-local rather than linear.
- **Sequence dependence** — the same current state may imply different outcomes depending on the immediately preceding state.
- **Session-specific effects** — information transmission may differ across predefined market-session blocks.
- **Weak-variable interactions** — individually weak variables may jointly matter, subject to strict complexity limits.

## Research firewall and chronological splits

For the current two-year historical crypto/equity programme, the default frozen chronology is:

- **Discovery:** 2024-07-01 through 2025-06-30
- **Validation:** 2025-07-01 through 2026-04-30
- **Final untouched holdout:** 2026-05-01 through 2026-06-28

A faster current-period screen may be used to nominate structures, but once a candidate is formulated from that screen, those same current rows do not count as pristine external validation. Historical untouched periods must provide the first external replication.

The final holdout must not be queried for candidate target outcomes until:

- the predictor definition is frozen;
- the lag and target horizon are frozen;
- the conditioning family is frozen;
- the parameter neighbourhood to be checked is defined;
- the multiple-testing family is declared;
- the economic implementation, when applicable, is sufficiently specified to prevent outcome-driven repair.

If a candidate fails the final holdout, it is rejected. The holdout cannot become a new development set.

## Conditional-rescue lane

An unstable-looking candidate may receive a bounded conditional-rescue attempt. This exists to avoid prematurely rejecting genuine conditional structure while preventing unlimited post-hoc slicing.

Rules:

- Allowed conditioning families are fixed in advance: time/session, volatility state, liquidity state, breadth state, prior direction, shock magnitude and cross-sectional dispersion.
- Maximum conditioning dimensions: **2**.
- Maximum thresholds per numeric conditioner: **3**.
- Numeric thresholds come from discovery data only or from an independently frozen prior threshold.
- Minimum usable sample: **30 events per evaluated cell** unless a rare-event family has a separately preregistered small-sample test.
- The conditioned relationship must validate before the final holdout is opened.
- A regime reversal must be demonstrated as a replicating interaction, not by independently cherry-picking two subgroups.
- If the mapping from regime to effect changes across external periods, the rescue fails.
- A failed rescue is written to the rejection ledger; no further slicing is allowed unless a materially new dataset or independently motivated variable justifies a new fingerprint.

## Low-frequency-event lane

Rare events are not rejected merely for low frequency. They are assessed with methods appropriate to sparse data:

- event definition frozen before outcomes are inspected;
- neighbouring tail thresholds tested for continuity rather than exact-threshold luck;
- exact event counts and calendar clustering reported;
- leave-one-event and leave-one-session-out sensitivity;
- bootstrap/permutation inference at the appropriate dependence unit;
- tail payoff and adverse-excursion distributions reported;
- capacity/liquidity and realistic execution considered before economic promotion.

Low frequency is a reason to demand stronger replication, not a reason to discard automatically or waive statistical discipline.

## Multiple-testing control

Every test belongs to a declared family. Examples include all lags for one predictor/target search, all session blocks, all threshold variants, or all pairwise interactions in a screen.

Required controls:

- record the approximate number of hypotheses screened;
- use Benjamini-Hochberg FDR where the family is large and p-values/permutation p-values are available;
- use family-wise permutation/max-statistic tests when practical for highly dependent grids;
- do not reset the multiple-testing count when a candidate receives a new name;
- treat parameter neighbours as robustness checks, not fresh independent discoveries;
- preserve screened near-misses in the ledger so they remain inside the original multiplicity family if revisited.

A strong nominal p-value discovered after hundreds of searches is not strong evidence by itself.

## Mandatory candidate tests

A candidate cannot be promoted without facing the following, to the extent supported by the available data:

1. **Chronological validation** on data not used to formulate the rule.
2. **Untouched final holdout** after the complete rule is frozen.
3. **Multiple-testing accounting** and FDR/family-wise control where practical.
4. **Placebo tests** such as wrong assets, wrong signs, wrong sessions or mechanically similar noncausal predictors.
5. **Time permutation / circular shift** that destroys causal timing while preserving relevant marginal structure.
6. **Parameter perturbation** around lags, horizons, thresholds and state definitions.
7. **Neighbouring-threshold analysis** to reject isolated optimal cut-offs.
8. **Regime stability** across volatility, liquidity, session, direction and major market periods.
9. **Outlier sensitivity** including removal of the best event, best several events, best day/session and influential assets where relevant.
10. **Dependence-aware inference** such as day/session cluster bootstrap when observations overlap.
11. **Execution-cost stress** only after predictability survives: spread, commissions/fees, slippage, funding/borrow, market impact and liquidity constraints as applicable.
12. **Signal degradation / delay stress** when execution latency could erase the information advantage.

## Parameter-neighbour rule

A parameter is credible when a neighbourhood around it retains the qualitative effect. A candidate is suspicious when only one narrow value works.

Neighbouring values are evaluated after the primary definition is frozen. They cannot be used to replace a failed primary rule and then call the new value validated. If a neighbour becomes the new candidate, it starts a new development cycle and requires a new untouched holdout.

## Unusual-lag protocol

Lag searches are particularly vulnerable to data mining and shared-regime confounds.

- Search a fixed lag grid as one hypothesis family.
- Include lag 0 as an explicit baseline.
- Adjust for source and target time-of-day effects where intraday seasonality is material.
- Compare the candidate lag and lag 0 on matched samples.
- Use neighbouring and placebo lags.
- Reject a purported lead if the historical relationship simply tracks lag 0 or a persistent shared volatility regime.
- A large lagged correlation is not a leading indicator unless lag specificity replicates.

## Aggregation and cross-sectional protocol

When a cross-sectional or subgroup variable appears useful:

- compare it against the broad market mean/range baseline;
- test incremental value using residualisation, nested models or equivalent out-of-sample comparisons;
- freeze subgroup definitions mechanically (for example fixed liquidity quartiles from lagged information);
- avoid contemporaneous membership information that would not have been known at decision time;
- distinguish a genuinely incremental cross-sectional signal from a proxy for overall volatility or market direction.

## Interaction protocol

Interactions receive a limited complexity budget:

- pairwise interactions first;
- higher-order interactions are not allowed simply because pairwise tests failed;
- parent variables and interaction family size are recorded;
- hierarchical tests are preferred where applicable;
- conditioning and interaction dimensions count toward the same complexity budget when they form one rule.

## Placebo and permutation standards

Placebos should preserve nuisance structure while destroying the hypothesised information path. Depending on the candidate, use:

- within-day circular time shifts;
- session-preserving predictor permutations;
- source-day permutations with target-day structure intact;
- wrong-lag or future-to-past placebo lags;
- mechanically similar but causally irrelevant assets/cohorts.

Permutation should be conducted at a level that does not create artificial independence.

## Economic translation

Predictive information and tradeable edge are separate stages.

Do not optimise entry/exit mechanics until the information relationship has survived external validation. Once a trading implementation is proposed, freeze:

- observable trigger;
- exact information available at decision time;
- entry rule and timing;
- instrument and direction;
- sizing assumption;
- exit and maximum holding period;
- invalidation/stop where relevant;
- spread, commissions, slippage and impact;
- liquidity/capacity and borrow/funding constraints where applicable.

Then test the fixed implementation independently. Do not repair an economic implementation after observing its untouched holdout.

## Rejection and near-miss ledger

Every explored idea receives a deterministic `search_fingerprint` in `research.contrarian_search_ledger`.

Statuses are semantically distinct:

- `registered` — planned, not yet fairly tested.
- `historical_validation` — candidate frozen and under external testing.
- `parked` — tested enough to preserve as a near-miss but not currently promoted; revisit only under the recorded condition and original multiplicity family.
- `rejected` — failed a material scientific/economic gate; do not rediscover under a new name.
- family-level statuses may record that top candidates were rejected while lower-ranked preregistered near-misses remain eligible for bounded follow-up.

Rejections must record the failure class and reason, such as:

- no external replication;
- conditional-rescue failure;
- regime mapping instability;
- lag specificity failure;
- incremental effect explained by an existing predictor;
- underpowered subgroup;
- outlier dependence;
- threshold fragility;
- execution costs overwhelm gross edge;
- unavailable-at-decision-time information;
- unrealistic fills or same-bar path leakage.

`can_revisit=true` requires a concrete `revisit_condition`; otherwise a rejected idea stays closed.

## Promotion standard

A surprising result earns no special credit for being surprising. Promotion requires that the predictor:

- exists before the target outcome;
- replicates outside its formulation sample;
- survives appropriate multiplicity controls;
- survives placebo/permutation tests;
- remains qualitatively stable under parameter neighbours and relevant regimes;
- is not explained by an already-known predictor unless the combination adds out-of-sample value;
- survives outlier/dependence checks;
- and, for a trading claim, remains economically positive after realistic execution constraints.

If those gates fail, record the failure and continue searching.

## Research stopping rule

The programme stops only when either:

- **Outcome A:** at least one genuinely robust, executable edge survives the complete process; or
- **Outcome B:** the meaningful search space supported by available data has been exhausted, with near-misses, rejected families, missing datasets and next experiments documented.

No edge should be manufactured to satisfy the mission.