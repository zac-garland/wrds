"""
Weighted cluster salience scoring for earnings-call transcript text.

Rebuilt from scratch to cover ~85-90% of financially meaningful content
across all US public companies. 26 clusters vs original 12, with ~3x
more terms, restructured to eliminate false positives and reduce
generic-word dilution.

Design principles
-----------------
- Universal clusters (guidance, margins, macro...) always active
- Sector clusters (banking, healthcare, energy, retail...) always scored —
  they score near-zero for irrelevant firms so no need to gate them
- Generic high-frequency words (cost, price, volume) kept at low weights 0.4–0.6
  so they only matter when no better term fires
- Multi-word terms always sorted longest-first (handled in _cluster_term_list)
  so "net interest margin" matches before "margin"
- mgmt_signal cluster contains NEGATIVE weights for boilerplate phrases
  that dilute signal across the rest of the call

Usage
-----
    from salience_dictionary import score_sentence, highlight_call_html, write_highlight_html
    score, breakdown = score_sentence("We are raising full-year guidance.", year=2023)
    html = highlight_call_html(turns_df, year=2023)
    write_highlight_html("out.html", turns_df, year=2023)
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from html import escape
from pathlib import Path
from typing import Any

# ── Palette — one color per cluster (stable insertion-order mapping) ──────────
_PALETTE = (
    "#c9a227",  # guidance          gold
    "#f59e42",  # earnings          amber
    "#34d399",  # revenue_growth    green
    "#2dd4bf",  # margin            teal
    "#60a5fa",  # cost_structure    blue
    "#a78bfa",  # capital_alloc     violet
    "#e879f9",  # balance_sheet     fuchsia
    "#f472b6",  # consumer_demand   pink
    "#fb923c",  # pricing_strategy  orange
    "#4ade80",  # customer_metrics  lime
    "#38bdf8",  # product_innov     sky
    "#818cf8",  # geo_expansion     indigo
    "#fbbf24",  # partnerships      yellow
    "#f87171",  # competition       red
    "#94a3b8",  # macro_env         slate
    "#a3e635",  # supply_chain      yellow-green
    "#c084fc",  # workforce         purple
    "#fda4af",  # regulatory        rose
    "#67e8f9",  # technology        cyan
    "#86efac",  # esg               light-green
    "#d1d5db",  # mgmt_signal       gray
    "#fcd34d",  # banking_credit    light-amber
    "#6ee7b7",  # healthcare        emerald
    "#fdba74",  # energy            light-orange
    "#cbd5e1",  # real_estate       light-slate
    "#fca5a5",  # retail            light-red
)


def cluster_colors() -> dict[str, str]:
    keys = list(SALIENCE_DICT.keys())
    return {k: _PALETTE[i % len(_PALETTE)] for i, k in enumerate(keys)}


def _hex_to_rgb(h: str) -> tuple[int, int, int]:
    h = h.strip().lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _split_sentences(text: str) -> list[str]:
    return re.split(r"(?<=[.!?])\s+(?=[A-Z\"'(\[])", text)


# =============================================================================
# SALIENCE DICTIONARY
# =============================================================================
# Each cluster:  { "cluster_weight": float, "terms": { phrase: weight } }
#
# Cluster weights:
#   3.0   highest importance — guidance, direct earnings signals
#   2.5   high — macro drivers, capital decisions, risk events
#   2.2   medium-high — growth, margins, competition, technology
#   2.0   medium — consumer, pricing, partnerships, geography
#   1.8   medium-low — operational, workforce, ESG
#   1.5   sector-specific (low base, only fires in relevant sectors)
#
# Term weights:
#   1.0   highly specific, always high-signal
#   0.8   specific and meaningful
#   0.6   meaningful but somewhat generic
#   0.4   generic — only worth scoring if nothing better fires
#   <0    boilerplate penalty (mgmt_signal only)
# =============================================================================

SALIENCE_DICT: dict[str, dict[str, Any]] = {

    # ── 1. GUIDANCE & FORWARD OUTLOOK ─────────────────────────────────────────
    # The single highest-value cluster. Any change to guidance is material.
    "guidance": {
        "cluster_weight": 3.0,
        "terms": {
            # Explicit guidance language
            "full-year guidance":           1.0,
            "annual guidance":              1.0,
            "guidance range":               1.0,
            "guidance":                     0.9,
            "full year":                    0.7,
            "full-year":                    0.7,
            # Directional changes — most important
            "raising guidance":             1.0,
            "lowering guidance":            1.0,
            "cutting guidance":             1.0,
            "withdrawing guidance":         1.0,
            "suspending guidance":          1.0,
            "raising our":                  0.6,
            "lowering our":                 0.6,
            "narrowing":                    0.7,
            "raising":                      0.5,
            # Affirmation language
            "reaffirm":                     0.8,
            "reiterate":                    0.7,
            "on track":                     0.7,
            "remain on track":              0.9,
            "maintained":                   0.5,
            # Outlook and visibility
            "outlook":                      0.9,
            "forecast":                     0.9,
            "visibility":                   0.7,
            "conviction":                   0.7,
            "trajectory":                   0.6,
            # Forward language
            "expect":                       0.5,
            "anticipate":                   0.5,
            "project":                      0.4,
            "next quarter":                 0.6,
            "next fiscal year":             0.7,
            "second half":                  0.6,
            "remainder of the year":        0.8,
            "going forward":                0.5,
        },
    },

    # ── 2. EARNINGS PERFORMANCE ───────────────────────────────────────────────
    # Actual vs expected results. Beat/miss language is highest signal.
    "earnings_performance": {
        "cluster_weight": 3.0,
        "terms": {
            # Beat / miss
            "beat expectations":            1.0,
            "exceeded expectations":        1.0,
            "below expectations":           1.0,
            "missed expectations":          1.0,
            "ahead of consensus":           1.0,
            "fell short":                   1.0,
            "shortfall":                    1.0,
            "disappointing":                0.9,
            "outperformed":                 0.8,
            "underperformed":               0.9,
            "record results":               0.9,
            "record quarter":               0.9,
            "record revenue":               0.9,
            # EPS / earnings metrics
            "earnings per share":           0.9,
            "diluted eps":                  0.9,
            "eps":                          0.7,
            "net income":                   0.7,
            "operating income":             0.7,
            "adjusted earnings":            0.8,
            "non-gaap":                     0.7,
            # Surprise language
            "stronger than expected":       0.9,
            "weaker than expected":         0.9,
            "better than anticipated":      0.8,
            "ahead of":                     0.6,
            "exceeded":                     0.7,
            "missed":                       0.8,
        },
    },

    # ── 3. REVENUE & GROWTH DRIVERS ───────────────────────────────────────────
    "revenue_growth": {
        "cluster_weight": 2.5,
        "terms": {
            # High-signal growth metrics
            "organic growth":               1.0,
            "organic revenue":              1.0,
            "constant currency":            0.9,
            "currency-neutral":             0.9,
            "same-store sales":             1.0,
            "comparable sales":             1.0,
            "comparable store":             1.0,
            "net revenue":                  0.8,
            # Market position
            "market share":                 1.0,
            "share gains":                  1.0,
            "share loss":                   1.0,
            "gaining share":                0.9,
            "losing share":                 0.9,
            # Growth levers
            "volume":                       0.6,
            "price":                        0.4,
            "mix":                          0.4,
            "cross-sell":                   0.9,
            "upsell":                       0.8,
            "attach rate":                  0.8,
            "take rate":                    0.8,
            "monetization":                 0.8,
            # Forward-looking growth
            "total addressable market":     0.9,
            "tam":                          0.7,
            "addressable market":           0.8,
            "growth runway":                0.8,
            "growth algorithm":             0.9,
            # Bookings / backlog
            "backlog":                      0.9,
            "order intake":                 0.9,
            "bookings":                     0.9,
            "pipeline":                     0.6,
            "contracted revenue":           0.9,
            "remaining performance obligation": 1.0,
            "rpo":                          0.8,
            # SaaS / recurring
            "annual recurring revenue":     1.0,
            "arr":                          0.8,
            "net revenue retention":        1.0,
            "nrr":                          0.8,
            "dollar-based net retention":   1.0,
        },
    },

    # ── 4. MARGIN & PROFITABILITY ──────────────────────────────────────────────
    "margin_profitability": {
        "cluster_weight": 2.8,
        "terms": {
            # Margin types — most important
            "gross margin":                 1.0,
            "operating margin":             1.0,
            "net margin":                   0.9,
            "ebitda margin":                1.0,
            "contribution margin":          0.9,
            "margin expansion":             1.0,
            "margin compression":           1.0,
            "margin improvement":           0.9,
            "margin pressure":              1.0,
            "margin headwind":              1.0,
            "margin":                       0.6,
            # Profitability metrics
            "ebitda":                       0.9,
            "adjusted ebitda":              0.9,
            "operating profit":             0.8,
            "return on equity":             0.9,
            "return on assets":             0.8,
            "return on invested capital":   0.9,
            "roic":                         0.9,
            "roe":                          0.8,
            "profitability":                0.7,
            # Leverage / efficiency
            "operating leverage":           0.9,
            "negative operating leverage":  1.0,
            "fixed cost leverage":          0.9,
            "incremental margin":           0.9,
            # Pricing dynamics
            "pricing power":                0.9,
            "price realization":            0.9,
            "price-cost":                   1.0,
            "price cost":                   1.0,
        },
    },

    # ── 5. COST STRUCTURE & EFFICIENCY ────────────────────────────────────────
    "cost_structure": {
        "cluster_weight": 2.3,
        "terms": {
            # Restructuring (highest signal)
            "restructuring charge":         1.0,
            "restructuring":                0.9,
            "workforce reduction":          1.0,
            "reduction in force":           1.0,
            "rif":                          0.8,
            "layoff":                       1.0,
            "headcount reduction":          1.0,
            "rightsizing":                  0.9,
            "footprint reduction":          0.9,
            # Input costs
            "input cost":                   1.0,
            "raw material":                 0.9,
            "commodity cost":               0.9,
            "labor cost":                   0.8,
            "wage inflation":               1.0,
            "cost inflation":               0.9,
            "freight cost":                 0.8,
            "energy cost":                  0.7,
            # Efficiency programs
            "cost savings":                 0.9,
            "cost reduction":               0.9,
            "efficiency program":           0.9,
            "productivity initiative":      0.9,
            "structural savings":           0.9,
            "run rate savings":             1.0,
            # Investment spending
            "research and development":     0.7,
            "r&d":                          0.7,
            "selling general and administrative": 0.8,
            "sg&a":                         0.8,
            "opex":                         0.7,
            "headcount":                    0.7,
            "cost":                         0.4,
        },
    },

    # ── 6. CAPITAL ALLOCATION ─────────────────────────────────────────────────
    "capital_allocation": {
        "cluster_weight": 2.5,
        "terms": {
            # Shareholder returns
            "share repurchase":             1.0,
            "buyback program":              1.0,
            "stock repurchase":             1.0,
            "buyback":                      0.9,
            "repurchase":                   0.8,
            "special dividend":             1.0,
            "dividend increase":            1.0,
            "dividend cut":                 1.0,
            "dividend":                     0.7,
            # M&A
            "acquisition":                  1.0,
            "merger":                       1.0,
            "divestiture":                  1.0,
            "spinoff":                      1.0,
            "spin-off":                     1.0,
            "joint venture":                0.9,
            "strategic transaction":        0.9,
            "purchase price":               0.7,
            # Capex
            "capital expenditure":          0.9,
            "capex":                        0.9,
            "capital allocation":           0.9,
            "capital return":               0.9,
            "capital deployment":           0.8,
            # Cash generation
            "free cash flow":               1.0,
            "fcf":                          0.9,
            "cash conversion":              0.9,
            "cash generation":              0.8,
            "cash flow from operations":    0.9,
        },
    },

    # ── 7. BALANCE SHEET & LIQUIDITY ──────────────────────────────────────────
    "balance_sheet": {
        "cluster_weight": 2.3,
        "terms": {
            # Distress signals (highest weight)
            "going concern":                1.0,
            "covenant breach":              1.0,
            "covenant violation":           1.0,
            "material weakness":            1.0,
            "restatement":                  1.0,
            "liquidity crisis":             1.0,
            # Leverage
            "net debt":                     0.9,
            "gross debt":                   0.8,
            "leverage ratio":               0.9,
            "debt-to-ebitda":               0.9,
            "leverage":                     0.6,
            "deleverage":                   0.8,
            "refinancing":                  0.9,
            "maturity":                     0.7,
            # Liquidity
            "liquidity":                    0.8,
            "cash position":                0.8,
            "cash on hand":                 0.8,
            "revolver":                     0.8,
            "credit facility":              0.8,
            "covenant":                     0.8,
            # Impairments
            "impairment":                   1.0,
            "write-down":                   1.0,
            "write-off":                    1.0,
            "goodwill impairment":          1.0,
            "asset impairment":             1.0,
            "balance sheet":                0.5,
        },
    },

    # ── 8. CONSUMER & DEMAND ENVIRONMENT ──────────────────────────────────────
    # Critical for consumer-facing companies but also for B2B reading end-market demand.
    "consumer_demand": {
        "cluster_weight": 2.2,
        "terms": {
            # Consumer health signals
            "consumer confidence":          1.0,
            "consumer sentiment":           1.0,
            "consumer health":              1.0,
            "consumer spending":            1.0,
            "consumer behavior":            0.9,
            "consumer demand":              0.9,
            "consumer":                     0.4,
            # Spending patterns
            "spending trends":              0.9,
            "customer spending":            0.9,
            "spending slowdown":            1.0,
            "spending deceleration":        1.0,
            "discretionary spending":       1.0,
            "essential spending":           0.8,
            "trade-down":                   1.0,
            "trading down":                 1.0,
            "trade-up":                     0.9,
            "premiumization":               1.0,
            "affordable":                   0.6,
            # Demand signals
            "demand environment":           0.9,
            "demand softness":              1.0,
            "demand weakness":              1.0,
            "demand acceleration":          0.9,
            "pent-up demand":               1.0,
            "end market":                   0.7,
            "end demand":                   0.8,
            # Traffic / engagement
            "foot traffic":                 1.0,
            "store traffic":                1.0,
            "transaction volume":           0.9,
            "active users":                 0.9,
            "monthly active users":         1.0,
            "daily active users":           1.0,
            "engagement":                   0.6,
        },
    },

    # ── 9. PRICING STRATEGY ───────────────────────────────────────────────────
    "pricing_strategy": {
        "cluster_weight": 2.2,
        "terms": {
            # Price actions
            "price increase":               1.0,
            "price decrease":               1.0,
            "price cut":                    1.0,
            "price reduction":              1.0,
            "price hike":                   0.9,
            "price action":                 0.9,
            "pricing actions":              0.9,
            "pricing initiative":           0.9,
            # Competitive pricing
            "pricing environment":          1.0,
            "pricing pressure":             1.0,
            "irrational pricing":           1.0,
            "price competition":            0.9,
            "price wars":                   1.0,
            "price war":                    1.0,
            "commoditization":              1.0,
            "commoditize":                  1.0,
            # Value / premium
            "value proposition":            0.8,
            "premium pricing":              0.9,
            "price realization":            0.9,
            "surcharge":                    0.8,
            "price elasticity":             1.0,
            "list price":                   0.7,
            "net price":                    0.7,
            "discounting":                  0.9,
            "promotional activity":         0.8,
        },
    },

    # ── 10. CUSTOMER METRICS ──────────────────────────────────────────────────
    "customer_metrics": {
        "cluster_weight": 2.0,
        "terms": {
            # Acquisition
            "customer acquisition":         1.0,
            "customer acquisition cost":    1.0,
            "cac":                          0.9,
            "new customer":                 0.7,
            "net new customer":             0.9,
            "customer additions":           0.8,
            # Retention / churn
            "churn rate":                   1.0,
            "customer churn":               1.0,
            "churn":                        0.8,
            "attrition":                    0.9,
            "retention rate":               0.9,
            "retention":                    0.7,
            "renewal rate":                 0.9,
            "logo retention":               1.0,
            # Lifetime value
            "lifetime value":               1.0,
            "ltv":                          0.9,
            "ltv to cac":                   1.0,
            "payback period":               0.9,
            # Satisfaction / NPS
            "net promoter score":           1.0,
            "nps":                          0.8,
            "customer satisfaction":        0.8,
            "csat":                         0.8,
            # Scale metrics
            "customer base":                0.6,
            "total customers":              0.6,
            "active customers":             0.7,
            "average revenue per user":     0.9,
            "arpu":                         0.9,
            "average order value":          0.9,
            "aov":                          0.8,
            # Fraud & security (important for payments/fintech)
            "fraud":                        1.0,
            "fraud losses":                 1.0,
            "fraud rate":                   1.0,
            "authorization rate":           1.0,
        },
    },

    # ── 11. PRODUCT & INNOVATION ──────────────────────────────────────────────
    "product_innovation": {
        "cluster_weight": 2.0,
        "terms": {
            # Product launches
            "product launch":               1.0,
            "new product":                  0.8,
            "product introduction":         0.9,
            "product roadmap":              0.9,
            "product portfolio":            0.7,
            "go-to-market":                 0.8,
            # Innovation signals
            "innovation":                   0.7,
            "innovate":                     0.6,
            "breakthrough":                 0.9,
            "next generation":              0.8,
            "next-generation":              0.8,
            "disruptive":                   0.8,
            "patent":                       0.8,
            "intellectual property":        0.8,
            # Digital / tech products
            "digital product":              0.8,
            "digital platform":             0.8,
            "digital capabilities":         0.8,
            "digital transformation":       0.9,
            "digitization":                 0.8,
            "digitalize":                   0.7,
            # Investment in future
            "invest in":                    0.4,
            "investing in":                 0.4,
            "r&d investment":               0.9,
            "research investment":          0.8,
        },
    },

    # ── 12. GEOGRAPHIC EXPANSION & INTERNATIONAL ──────────────────────────────
    "geographic_expansion": {
        "cluster_weight": 2.0,
        "terms": {
            # New markets
            "market entry":                 1.0,
            "new market":                   0.8,
            "geographic expansion":         1.0,
            "international expansion":      1.0,
            "global expansion":             0.9,
            # International performance
            "international":                0.5,
            "cross-border":                 1.0,
            "emerging markets":             0.9,
            "developed markets":            0.7,
            "europe":                       0.4,
            "asia pacific":                 0.5,
            "latin america":                0.5,
            "china":                        0.7,
            "india":                        0.5,
            # Localization
            "local market":                 0.6,
            "market penetration":           0.8,
            # FX impact (distinct from macro FX)
            "foreign exchange impact":      0.9,
            "currency headwind":            0.9,
            "currency tailwind":            0.8,
            "fx impact":                    0.8,
            "constant currency growth":     0.9,
        },
    },

    # ── 13. PARTNERSHIPS, M&A & ECOSYSTEM ────────────────────────────────────
    "partnerships_ecosystem": {
        "cluster_weight": 2.2,
        "terms": {
            # Partnerships
            "strategic partnership":        1.0,
            "strategic alliance":           1.0,
            "partnership":                  0.7,
            "alliance":                     0.6,
            "collaboration":                0.5,
            "co-development":               0.9,
            # Ecosystem
            "ecosystem":                    0.9,
            "platform ecosystem":           1.0,
            "network effect":               1.0,
            "flywheel":                     1.0,
            "two-sided":                    0.9,
            "marketplace":                  0.7,
            # Distribution
            "channel partner":              0.9,
            "channel":                      0.5,
            "reseller":                     0.7,
            "distribution partner":         0.9,
            "go-to-market partner":         0.9,
            "value-added reseller":         0.9,
            # Integration
            "integration":                  0.6,
            "api":                          0.6,
            "embedded":                     0.6,
            # Fintech / payments specific
            "issuer":                       0.8,
            "acquirer":                     0.8,
            "merchant":                     0.7,
            "network":                      0.5,
            "contactless":                  0.9,
            "digital wallet":               1.0,
            "tap to pay":                   0.9,
        },
    },

    # ── 14. COMPETITION & MARKET DYNAMICS ────────────────────────────────────
    "competition": {
        "cluster_weight": 2.2,
        "terms": {
            # Direct competition
            "competitive environment":      1.0,
            "competitive intensity":        1.0,
            "competitive landscape":        0.9,
            "competitive pressure":         1.0,
            "competitive threat":           1.0,
            "competitive dynamics":         0.9,
            "competitor":                   0.7,
            "competitive":                  0.5,
            # Market position
            "market share":                 1.0,
            "gaining share":                0.9,
            "losing share":                 0.9,
            "new entrant":                  0.9,
            "incumbent":                    0.7,
            "moat":                         0.9,
            "competitive moat":             1.0,
            "switching cost":               1.0,
            "switching costs":              1.0,
            # Disruption
            "disruption":                   0.8,
            "disruptor":                    0.9,
            "displacement":                 0.9,
            "substitute":                   0.7,
            "differentiation":              0.8,
            # Pricing competition
            "undercutting":                 1.0,
            "price matching":               0.9,
            "fintech":                      0.8,
            "intensifying":                 0.8,
            "competitive intensity":        1.0,
        },
    },

    # ── 15. MACRO & EXTERNAL ENVIRONMENT ─────────────────────────────────────
    "macro_environment": {
        "cluster_weight": 2.5,
        "terms": {
            # Direction signals
            "headwind":                     1.0,
            "tailwind":                     0.9,
            "macro environment":            0.9,
            "macroeconomic":                0.8,
            "economic uncertainty":         1.0,
            "economic slowdown":            1.0,
            # Inflation / rates
            "inflation":                    1.0,
            "deflation":                    0.9,
            "disinflation":                 0.9,
            "interest rate":                1.0,
            "rate environment":             0.9,
            "rate hike":                    1.0,
            "rate cut":                     1.0,
            "fed":                          0.6,
            "federal reserve":              0.8,
            "monetary policy":              0.9,
            # Recession
            "recession":                    1.0,
            "recessionary":                 1.0,
            "soft landing":                 0.9,
            "hard landing":                 1.0,
            "economic contraction":         0.9,
            # Trade / geopolitics
            "tariff":                       1.0,
            "trade war":                    1.0,
            "geopolitical":                 0.9,
            "sanctions":                    1.0,
            # Supply chain
            "supply chain":                 1.0,
            "supply disruption":            1.0,
            "supply constraint":            0.9,
            # FX (macro level)
            "foreign exchange":             0.8,
            "currency":                     0.6,
            "fx headwind":                  0.9,
            "fx tailwind":                  0.8,
        },
    },

    # ── 16. SUPPLY CHAIN & OPERATIONS ────────────────────────────────────────
    "supply_chain_ops": {
        "cluster_weight": 1.8,
        "terms": {
            # Inventory
            "inventory level":              1.0,
            "inventory build":              1.0,
            "inventory drawdown":           1.0,
            "inventory normalization":      1.0,
            "channel inventory":            1.0,
            "inventory":                    0.6,
            "destocking":                   1.0,
            "restocking":                   0.9,
            "inventory correction":         1.0,
            # Logistics
            "freight":                      0.8,
            "logistics":                    0.7,
            "fulfillment":                  0.7,
            "shipping":                     0.6,
            "lead time":                    0.8,
            # Capacity
            "capacity":                     0.6,
            "capacity constraint":          0.9,
            "capacity expansion":           0.8,
            "utilization":                  0.8,
            "throughput":                   0.7,
            # Sourcing
            "sourcing":                     0.7,
            "supplier":                     0.7,
            "vendor":                       0.6,
            "sole source":                  0.9,
            "dual source":                  0.8,
        },
    },

    # ── 17. WORKFORCE & TALENT ────────────────────────────────────────────────
    "workforce_talent": {
        "cluster_weight": 1.8,
        "terms": {
            # Hiring signals
            "hiring":                       0.7,
            "talent acquisition":           0.8,
            "headcount growth":             0.8,
            "headcount":                    0.6,
            "attrition":                    0.8,
            "turnover":                     0.7,
            # Leadership
            "ceo":                          0.7,
            "cfo":                          0.7,
            "chief executive":              0.7,
            "leadership transition":        1.0,
            "management change":            1.0,
            "succession":                   0.9,
            "new ceo":                      1.0,
            "new cfo":                      1.0,
            # Culture / morale
            "employee engagement":          0.8,
            "culture":                      0.5,
            "return to office":             0.9,
            # Cost (labor)
            "labor":                        0.6,
            "wage":                         0.7,
            "compensation":                 0.6,
            "benefits":                     0.5,
            "stock-based compensation":     0.8,
        },
    },

    # ── 18. REGULATORY, LEGAL & COMPLIANCE ───────────────────────────────────
    "regulatory_legal": {
        "cluster_weight": 2.5,
        "terms": {
            # Investigations / enforcement (very high signal)
            "investigation":                1.0,
            "sec investigation":            1.0,
            "doj investigation":            1.0,
            "regulatory investigation":     1.0,
            "subpoena":                     1.0,
            "grand jury":                   1.0,
            # Litigation
            "litigation":                   1.0,
            "lawsuit":                      0.9,
            "class action":                 1.0,
            "settlement":                   0.9,
            "judgment":                     0.8,
            "legal proceeding":             0.9,
            # Regulatory risk
            "regulatory":                   0.7,
            "compliance":                   0.6,
            "antitrust":                    1.0,
            "monopoly":                     0.9,
            "consent order":                1.0,
            "consent decree":               1.0,
            "cease and desist":             1.0,
            # Accounting integrity
            "restatement":                  1.0,
            "material weakness":            1.0,
            "internal control":             0.8,
            "audit":                        0.6,
            # Policy / legislation
            "legislation":                  0.7,
            "regulation":                   0.6,
            "policy change":                0.7,
            "rule change":                  0.7,
        },
    },

    # ── 19. TECHNOLOGY & DIGITAL ─────────────────────────────────────────────
    # Time-varying: cluster_weight boosted post-2020 in score_sentence
    "technology_digital": {
        "cluster_weight": 2.3,
        "terms": {
            # AI / ML (highest weight post-2022)
            "generative ai":                1.0,
            "artificial intelligence":      1.0,
            "large language model":         1.0,
            "machine learning":             0.9,
            "llm":                          0.9,
            "agentic":                      1.0,
            "ai-powered":                   1.0,
            "ai-driven":                    0.9,
            "ai":                           0.7,
            "copilot":                      0.8,
            # Cloud
            "cloud migration":              0.9,
            "cloud revenue":                0.9,
            "cloud":                        0.7,
            "saas":                         0.8,
            "software as a service":        0.8,
            "hyperscaler":                  0.9,
            # Data / analytics
            "data analytics":               0.8,
            "data platform":                0.8,
            "data monetization":            0.9,
            # Security
            "cybersecurity":                1.0,
            "ransomware":                   1.0,
            "data breach":                  1.0,
            "cyber attack":                 1.0,
            "zero trust":                   0.9,
            # Semiconductors / infra
            "semiconductor":                0.8,
            "gpu":                          0.9,
            "data center":                  0.8,
            "chip":                         0.7,
            # Automation
            "automation":                   0.7,
            "robotic process automation":   0.9,
            "rpa":                          0.8,
        },
    },

    # ── 20. ESG & SUSTAINABILITY ──────────────────────────────────────────────
    "esg_sustainability": {
        "cluster_weight": 1.8,
        "terms": {
            # Climate / environment
            "net zero":                     1.0,
            "carbon neutral":               1.0,
            "carbon emissions":             0.9,
            "greenhouse gas":               0.9,
            "scope 1":                      0.9,
            "scope 2":                      0.9,
            "scope 3":                      0.9,
            "sustainability":               0.7,
            "energy transition":            0.9,
            "renewable energy":             0.8,
            "electrification":              0.9,
            "ev":                           0.8,
            "battery":                      0.7,
            # Social
            "diversity":                    0.6,
            "inclusion":                    0.6,
            "dei":                          0.7,
            # Governance
            "esg":                          0.8,
            "board composition":            0.8,
            "shareholder rights":           0.9,
            "say on pay":                   0.9,
            "proxy":                        0.6,
        },
    },

    # ── 21. MANAGEMENT SIGNALS (incl. boilerplate penalties) ─────────────────
    "mgmt_signal": {
        "cluster_weight": 2.0,
        "terms": {
            # Evasion / avoidance — high positive weight (we WANT to surface these)
            "not in a position to":         1.0,
            "not going to get into":        1.0,
            "decline to comment":           1.0,
            "too early to say":             1.0,
            "too early to tell":            1.0,
            "it depends":                   0.9,
            "we will see":                  0.7,
            "monitor closely":              0.7,
            # Strong commitment language
            "committed to":                 0.7,
            "we are confident":             0.8,
            "highly confident":             0.9,
            "firmly committed":             0.9,
            # Surprise language
            "frankly":                      0.8,
            "candidly":                     0.8,
            "disappointed":                 0.9,
            "disappointing":                0.9,
            "clearly":                      0.4,
            # Negative surprise
            "below our expectations":       1.0,
            "below expectations":           1.0,
            "missed our target":            1.0,
            "fell short":                   1.0,
            # Boilerplate PENALTIES
            "thank you for the question":   -2.0,
            "great question":               -2.0,
            "good question":                -1.5,
            "as i mentioned earlier":       -0.8,
            "as i said":                    -0.5,
            "as we have said":              -0.5,
            "i think what you are asking":  -0.5,
            "let me take that":             -0.5,
            "operator":                     -2.0,
        },
    },

    # ── 22. BANKING & CREDIT (financial sector) ───────────────────────────────
    "banking_credit": {
        "cluster_weight": 1.5,
        "terms": {
            # Credit quality (highest signal)
            "credit loss":                  1.0,
            "credit losses":                1.0,
            "allowance for credit losses":  1.0,
            "net charge-off":               1.0,
            "charge-off":                   1.0,
            "delinquency":                  1.0,
            "non-performing":               1.0,
            "npl":                          1.0,
            "provision":                    0.9,
            # NIM / spreads
            "net interest margin":          1.0,
            "nim":                          1.0,
            "net interest income":          0.9,
            "nii":                          0.8,
            "credit spread":                0.9,
            "swap spread":                  0.8,
            # Deposits / funding
            "deposit":                      0.7,
            "deposit outflow":              1.0,
            "deposit growth":               0.9,
            "funding cost":                 0.9,
            "cost of funds":                0.9,
            # Capital adequacy
            "capital ratio":                0.9,
            "cet1":                         0.9,
            "tier 1":                       0.9,
            "stress test":                  0.9,
            "basel":                        0.8,
            # Loans
            "loan growth":                  0.9,
            "loan demand":                  0.8,
            "credit card":                  0.7,
            "mortgage":                     0.7,
        },
    },

    # ── 23. HEALTHCARE & BIOTECH ──────────────────────────────────────────────
    "healthcare_biotech": {
        "cluster_weight": 1.5,
        "terms": {
            # Regulatory / pipeline (highest signal)
            "fda approval":                 1.0,
            "fda":                          0.9,
            "clinical trial":               1.0,
            "phase 3":                      1.0,
            "phase 2":                      0.9,
            "phase 1":                      0.8,
            "readout":                      0.9,
            "data readout":                 1.0,
            "endpoint":                     0.9,
            "primary endpoint":             1.0,
            # Competitive / IP
            "patent cliff":                 1.0,
            "patent expiry":                1.0,
            "biosimilar":                   1.0,
            "generic competition":          1.0,
            "loss of exclusivity":          1.0,
            # Commercial
            "formulary":                    1.0,
            "reimbursement":                1.0,
            "payer":                        0.8,
            "co-pay":                       0.8,
            "prior authorization":          0.9,
            # Pipeline
            "pipeline":                     0.7,
            "indication":                   0.7,
            "label expansion":              1.0,
            "new indication":               1.0,
        },
    },

    # ── 24. ENERGY & COMMODITIES ──────────────────────────────────────────────
    "energy_commodities": {
        "cluster_weight": 1.5,
        "terms": {
            # Production
            "production":                   0.7,
            "oil production":               0.9,
            "gas production":               0.8,
            "barrels per day":              1.0,
            "boe":                          0.9,
            "bopd":                         0.9,
            # Prices
            "oil price":                    0.9,
            "natural gas price":            0.9,
            "commodity price":              0.9,
            "wti":                          0.9,
            "brent":                        0.9,
            # Refining
            "refining margin":              1.0,
            "crack spread":                 1.0,
            "downstream":                   0.7,
            "upstream":                     0.7,
            # Reserves / assets
            "proved reserves":              0.9,
            "reserve replacement":          0.9,
            "lng":                          0.9,
            "natural gas":                  0.7,
            # Rig / operational
            "rig count":                    0.9,
            "completion":                   0.6,
            "fracking":                     0.8,
        },
    },

    # ── 25. REAL ESTATE & REITs ───────────────────────────────────────────────
    "real_estate": {
        "cluster_weight": 1.5,
        "terms": {
            # Core metrics
            "funds from operations":        1.0,
            "ffo":                          1.0,
            "adjusted ffo":                 1.0,
            "net operating income":         1.0,
            "noi":                          0.9,
            "same-store noi":               1.0,
            # Occupancy
            "occupancy rate":               1.0,
            "occupancy":                    0.8,
            "leasing activity":             0.9,
            "lease expiration":             0.9,
            "lease renewal":                0.9,
            "rent growth":                  0.9,
            "cap rate":                     0.9,
            # Property
            "property acquisition":         0.9,
            "disposition":                  0.8,
            "development pipeline":         0.9,
        },
    },

    # ── 26. RETAIL & CONSUMER GOODS ──────────────────────────────────────────
    "retail_consumer": {
        "cluster_weight": 1.5,
        "terms": {
            # Core retail metrics
            "same-store sales":             1.0,
            "comparable store sales":       1.0,
            "comps":                        0.9,
            "comp sales":                   0.9,
            "basket size":                  0.9,
            "average ticket":               0.9,
            "transaction count":            0.9,
            # Inventory (retail specific)
            "inventory shrink":             1.0,
            "shrinkage":                    1.0,
            "out-of-stock":                 1.0,
            "in-stock":                     0.8,
            "planogram":                    0.8,
            # Category / brand
            "brand loyalty":                0.8,
            "private label":                0.8,
            "national brand":               0.7,
            "category management":          0.8,
            # E-commerce
            "e-commerce":                   0.8,
            "online sales":                 0.8,
            "omnichannel":                  0.9,
            "digital sales":                0.8,
            "penetration":                  0.6,
        },
    },
}


# =============================================================================
# SCORING ENGINE (unchanged from original — validated against test cases)
# =============================================================================

def _cluster_term_list(cluster: dict[str, Any]) -> list[tuple[str, float]]:
    """Return (term, weight) pairs sorted longest-first to prevent partial matches."""
    pairs = [(str(k), float(v)) for k, v in cluster["terms"].items()]
    pairs.sort(key=lambda x: len(x[0]), reverse=True)
    return pairs


def _match_cluster_terms_no_overlap(
    text_lower: str, term_weights: list[tuple[str, float]]
) -> float:
    """Sum term weights for non-overlapping whole-word matches; longest terms first."""
    n = len(text_lower)
    covered = [False] * n
    raw_score = 0.0
    for term, w in term_weights:
        if not term:
            continue
        pat = re.compile(r"\b" + re.escape(term) + r"\b")
        for m in pat.finditer(text_lower):
            pos, end = m.start(), m.end()
            if not any(covered[pos:end]):
                for i in range(pos, end):
                    covered[i] = True
                raw_score += w
    return raw_score


def score_sentence(
    sentence: str,
    year: int | None = None,
    sector: str | None = None,
    custom_weights: dict[str, float] | None = None,
) -> tuple[float, dict[str, float]]:
    """
    Returns (total_score, cluster_breakdown).

    Parameters
    ----------
    sentence      : raw sentence text
    year          : call year — boosts technology_digital post-2020, emerging themes
    sector        : cluster name to upweight by 1.3× (e.g. 'banking_credit')
    custom_weights: override per-cluster weights {cluster_name: weight}
    """
    text = sentence.lower()
    scores: dict[str, float] = {}
    weights = custom_weights or {}

    for cluster_name, cluster in SALIENCE_DICT.items():
        base_w = float(weights.get(cluster_name, cluster["cluster_weight"]))

        # Time-varying boosts
        if cluster_name == "technology_digital" and year is not None and year >= 2020:
            base_w *= 1.0 + 0.1 * min(year - 2020, 5)   # +10% per year, capped at +50%

        if cluster_name == "esg_sustainability" and year is not None and year >= 2019:
            base_w *= 1.2

        # Sector boost
        if sector and cluster_name == sector:
            base_w *= 1.3

        term_weights = _cluster_term_list(cluster)
        cluster_raw = _match_cluster_terms_no_overlap(text, term_weights)
        scores[cluster_name] = round(cluster_raw * base_w, 4)

    total = round(sum(scores.values()), 4)
    return total, scores


def highlight_transcript(
    turns_df: Any,
    year: int | None = None,
    sector: str | None = None,
    top_n: int = 30,
    custom_weights: dict[str, float] | None = None,
    min_words: int = 8,
) -> list[dict[str, Any]]:
    """
    Return top_n highest-salience sentences across all turns with cluster breakdown.

    turns_df must have a ``componenttext`` column. ``role`` and ``section`` are
    used for labeling if present.
    """
    results: list[dict[str, Any]] = []

    for _, row in turns_df.iterrows():
        text = str(row.get("componenttext", "") or "")
        for sent in _split_sentences(text):
            words = sent.split()
            if len(words) < min_words:
                continue
            score, breakdown = score_sentence(
                sent, year=year, sector=sector, custom_weights=custom_weights
            )
            if score > 0:
                top_cluster = max(breakdown, key=breakdown.get)
                results.append(
                    {
                        "sentence":       sent.strip()[:2000],
                        "score":          score,
                        "role":           row.get("role", ""),
                        "section":        row.get("section", ""),
                        "top_cluster":    top_cluster,
                        "cluster_scores": breakdown,
                    }
                )

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_n]


def temporal_delta(
    call_t: list[dict[str, Any]],
    call_t_minus_1: list[dict[str, Any]],
    cluster: str,
) -> dict[str, Any]:
    """Compare summed cluster scores between two highlighted outputs (QoQ delta)."""
    t_sents  = [r for r in call_t          if r.get("top_cluster") == cluster]
    t1_sents = [r for r in call_t_minus_1  if r.get("top_cluster") == cluster]
    t_score  = sum(r["score"] for r in t_sents)
    t1_score = sum(r["score"] for r in t1_sents)
    return {
        "score_delta":      round(t_score - t1_score, 4),
        "intensity_change": "increased" if t_score > t1_score else "decreased",
        "current_top":      t_sents[:5],
        "prior_top":        t1_sents[:5],
    }


# =============================================================================
# HTML RENDERER (visual highlighter)
# =============================================================================

def latest_call_turns(
    df: Any,
    *,
    ticker: str | None = None,
    transcriptid: int | None = None,
    date_col: str = "call_date",
    id_col: str = "transcriptid",
    order_col: str = "componentorder",
) -> Any:
    """Return all turns for a single call ordered by componentorder."""
    import pandas as pd

    if df is None or len(df) == 0:
        return df
    d = df
    if ticker is not None and "ticker" in d.columns:
        d = d[d["ticker"] == ticker]
    if d.empty:
        return d
    if transcriptid is not None:
        sub = d[d[id_col] == transcriptid]
    elif date_col in d.columns:
        dates = pd.to_datetime(d[date_col], errors="coerce")
        best  = dates.max()
        cand  = d[dates == best]
        tid   = int(cand[id_col].max())
        sub   = d[d[id_col] == tid]
    else:
        tid = int(d[id_col].max())
        sub = d[d[id_col] == tid]
    if order_col in sub.columns:
        sub = sub.sort_values(order_col, kind="mergesort")
    return sub


def _speaker_label(row: Any) -> str:
    for key in ("transcriptpersonname", "speakername", "speaker_name"):
        v = row.get(key) if hasattr(row, "get") else None
        if v is None and hasattr(row, "index") and key in row.index:
            v = row[key]
        if v is not None and str(v).strip():
            return str(v).strip()
    return "Speaker"


def _tooltip_breakdown(breakdown: dict[str, float], score: float, top_cluster: str) -> str:
    parts  = [f"total={score:.3f}", f"top={top_cluster}"]
    ranked = sorted(
        ((k, v) for k, v in breakdown.items() if v > 0), key=lambda x: -x[1]
    )[:8]
    parts.extend(f"{k}: {v:.3f}" for k, v in ranked)
    return escape(" | ".join(parts), quote=True)


def highlight_call_html(
    turns_df: Any,
    *,
    year: int | None = None,
    sector: str | None = None,
    custom_weights: dict[str, float] | None = None,
    min_words: int = 8,
    title: str | None = None,
    subtitle: str | None = None,
    order_col: str = "componentorder",
) -> str:
    """
    Render a full transcript as HTML with per-sentence color highlights.

    Layout: **floating topic legend** (fixed left), **full transcript** (left column),
    **topic filter panel** (right column). Clicking a legend entry lists every sentence
    where that cluster is the **dominant** match (same rule as highlight color).

    Opacity scales with salience vs. the strongest sentence on the call; hover for
    the full cluster breakdown.
    """
    cmap = cluster_colors()
    rows: list[Any] = []
    if hasattr(turns_df, "columns") and order_col in turns_df.columns:
        rows = list(turns_df.sort_values(order_col, kind="mergesort").iterrows())
    else:
        rows = list(turns_df.iterrows())

    # First pass — find max score for alpha scaling
    max_sc = 0.0
    for _, row in rows:
        text0 = str(row.get("componenttext", "") or "")
        for sent in _split_sentences(text0):
            raw = sent.strip()
            if not raw or len(raw.split()) < min_words:
                continue
            sc, _ = score_sentence(raw, year=year, sector=sector,
                                   custom_weights=custom_weights)
            if sc > max_sc:
                max_sc = sc
    denom = max_sc if max_sc > 1e-9 else 1.0

    # Second pass — render transcript + collect per-cluster hits (dominant topic per sentence)
    hits_by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
    turn_blocks: list[str] = []
    for _, row in rows:
        text    = str(row.get("componenttext", "") or "")
        role    = escape(str(row.get("role", "") or ""))
        section = escape(str(row.get("section", "") or ""))
        name    = escape(_speaker_label(row))
        meta_bits  = [x for x in (name, role, section) if x]
        meta_html  = " · ".join(meta_bits) if meta_bits else name
        plain_meta = " · ".join(
            str(x).strip()
            for x in (
                _speaker_label(row),
                row.get("role", "") or "",
                row.get("section", "") or "",
            )
            if str(x).strip()
        ) or "Speaker"

        styled_spans: list[str] = []
        for sent in _split_sentences(text):
            raw = sent.strip()
            if not raw:
                continue
            if len(raw.split()) < min_words:
                styled_spans.append(f'<span class="sent-plain">{escape(raw)}</span>')
                continue
            sc, br = score_sentence(raw, year=year, sector=sector,
                                    custom_weights=custom_weights)
            if sc <= 0:
                styled_spans.append(f'<span class="sent-plain">{escape(raw)}</span>')
                continue
            top_c     = max(br, key=br.get)
            rgb       = _hex_to_rgb(cmap.get(top_c, "#94a3b8"))
            intensity = min(1.0, sc / denom)
            alpha     = 0.1 + 0.75 * intensity
            border_a  = min(0.95, 0.35 + 0.55 * intensity)
            tip       = _tooltip_breakdown(br, sc, top_c)
            dc        = escape(top_c, quote=True)
            styled_spans.append(
                f'<span class="sent-hi" data-cluster="{dc}" title="{tip}" '
                f'style="background: rgba({rgb[0]},{rgb[1]},{rgb[2]},{alpha:.3f}); '
                f"border-bottom: 2px solid rgba({rgb[0]},{rgb[1]},{rgb[2]},{border_a:.3f});\">"
                f"{escape(raw)}</span>"
            )
            hits_by_cluster[top_c].append(
                {"meta": plain_meta, "text": raw, "score": round(float(sc), 4)}
            )

        body = " ".join(styled_spans) if styled_spans else escape(text)
        turn_blocks.append(
            f'<article class="turn"><header class="turn-meta">{meta_html}</header>'
            f'<div class="turn-text">{body}</div></article>'
        )

    hits_payload: dict[str, list[dict[str, Any]]] = {k: list(v) for k, v in hits_by_cluster.items()}
    for k in cmap:
        hits_payload.setdefault(k, [])
    for _lst in hits_payload.values():
        _lst.sort(key=lambda x: -float(x["score"]))

    app_data = {"hits": hits_payload, "colors": dict(cmap)}
    json_safe = json.dumps(app_data, ensure_ascii=True).replace("<", "\\u003c")

    legend_items = "".join(
        f'<li><button type="button" class="legend-btn" data-cluster="{escape(k, quote=True)}" '
        f'aria-pressed="false">'
        f'<span class="lg" style="background:{h};" aria-hidden="true"></span>'
        f'<span class="lg-label">{escape(k.replace("_", " "))}</span></button></li>'
        for k, h in cmap.items()
    )

    first = rows[0][1] if rows else None
    if title is None and first is not None:
        tkr = str(first.get("ticker", "") or "").strip()
        co  = str(first.get("companyname", "") or "").strip()
        title = f"{co} ({tkr})" if co and tkr else (co or tkr or "Earnings call")
    elif title is None:
        title = "Earnings call"
    if subtitle is None and first is not None:
        tid     = first.get("transcriptid", "")
        cd      = first.get("call_date", "")
        yr_hint = year if year is not None else ""
        subtitle = f"transcriptid={tid}  ·  call_date={cd}  ·  score_year={yr_hint}"
    elif subtitle is None:
        subtitle = ""

    doc_title = escape(str(title))
    sub_html  = f'<p class="sub">{escape(subtitle)}</p>' if subtitle else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{doc_title}</title>
<style>
:root {{
  --bg:     #0f1118;
  --card:   #161a24;
  --text:   #e8e8f0;
  --muted:  #8b8fa8;
  --border: #2a3142;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  font-family: "Segoe UI", system-ui, sans-serif;
  background: var(--bg); color: var(--text);
  line-height: 1.6; font-size: 15px;
}}
.wrap {{
  max-width: none;
  margin: 0;
  padding: 1.25rem 1.5rem 0.5rem;
  padding-left: calc(12.75rem + 1.5rem);
  padding-right: 1.5rem;
}}
h1 {{ font-size: 1.35rem; font-weight: 600; margin: 0 0 0.25rem; }}
.sub {{ color: var(--muted); font-size: 0.88rem; margin: 0 0 1rem; }}
.note {{ font-size: 0.78rem; color: var(--muted); margin: 0 0 1rem; max-width: 48rem; }}

/* Floating topic legend */
.legend-float {{
  position: fixed;
  left: 0.75rem;
  top: 50%;
  transform: translateY(-50%);
  z-index: 100;
  width: 11.75rem;
  max-height: min(88vh, 900px);
  overflow-x: hidden;
  overflow-y: auto;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 0.55rem 0.35rem 0.65rem;
  box-shadow: 0 12px 40px rgba(0,0,0,0.55);
}}
.legend-float .legend-title {{
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: var(--muted);
  padding: 0.2rem 0.5rem 0.45rem;
  font-weight: 600;
}}
.legend-float .legend-inner {{
  list-style: none;
  margin: 0;
  padding: 0;
}}
.legend-float .legend-inner li {{ margin: 0; }}
.legend-float .legend-btn {{
  display: flex;
  align-items: center;
  gap: 0.4rem;
  width: 100%;
  text-align: left;
  background: transparent;
  border: none;
  color: var(--text);
  cursor: pointer;
  padding: 0.32rem 0.45rem;
  border-radius: 8px;
  font-size: 0.72rem;
  line-height: 1.25;
}}
.legend-float .legend-btn:hover {{
  background: rgba(255,255,255,0.07);
}}
.legend-float .legend-btn.active {{
  background: rgba(100, 120, 200, 0.18);
  outline: 1px solid rgba(100, 120, 200, 0.35);
}}
.legend-float .lg {{
  width: 0.65rem;
  height: 0.65rem;
  border-radius: 3px;
  flex-shrink: 0;
}}
.legend-float .lg-label {{
  word-break: break-word;
}}

/* Two columns: transcript | topic hits */
.main-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.25rem;
  align-items: start;
  padding: 0 1.5rem 2.5rem;
  padding-left: calc(12.75rem + 1.5rem);
  min-height: 50vh;
}}
.col-left {{
  min-width: 0;
}}
.col-right {{
  position: sticky;
  top: 0.75rem;
  max-height: calc(100vh - 1.5rem);
  overflow-y: auto;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 0.9rem 1rem 1.1rem;
}}
.panel-head {{
  font-size: 1rem;
  font-weight: 600;
  margin: 0 0 0.35rem;
  color: var(--text);
}}
.panel-sub {{
  font-size: 0.78rem;
  color: var(--muted);
  margin: 0 0 0.85rem;
}}
.panel-empty {{
  font-size: 0.85rem;
  color: var(--muted);
  margin: 0;
}}
.hit-card {{
  margin-bottom: 0.85rem;
  padding: 0.65rem 0.75rem;
  background: rgba(0,0,0,0.2);
  border-radius: 8px;
  border-left: 3px solid var(--accent, #64748b);
}}
.hit-meta {{
  font-size: 0.72rem;
  color: var(--muted);
  font-weight: 600;
  margin-bottom: 0.35rem;
}}
.hit-score {{
  font-size: 0.7rem;
  color: var(--muted);
  display: block;
  margin-bottom: 0.35rem;
}}
.hit-text {{
  font-size: 0.88rem;
  line-height: 1.5;
  color: var(--text);
}}
.turn {{
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 0.85rem 1rem;
  margin-bottom: 0.85rem;
}}
.turn-meta {{
  font-size: 0.8rem;
  color: var(--muted);
  margin-bottom: 0.5rem;
  font-weight: 600;
  letter-spacing: 0.02em;
}}
.turn-text {{ font-size: 0.94rem; }}
.sent-plain {{ }}
.sent-hi {{ padding: 0.05em 0.15em; border-radius: 3px; cursor: help; }}

@media (max-width: 1100px) {{
  .main-grid {{
    grid-template-columns: 1fr;
    padding-left: 1.25rem;
  }}
  .legend-float {{
    position: relative;
    left: auto;
    top: auto;
    transform: none;
    width: auto;
    max-height: 14rem;
    margin: 0 1.25rem 1rem;
  }}
  .wrap {{
    padding-left: 1.25rem;
  }}
}}
</style>
</head>
<body>
<script type="application/json" id="salience-app-data">{json_safe}</script>

<nav class="legend-float" aria-label="Topic clusters">
  <div class="legend-title">Topics</div>
  <ul class="legend-inner">{legend_items}</ul>
</nav>

<div class="wrap">
  <h1>{doc_title}</h1>
  {sub_html}
  <p class="note">Left: full call. Use the floating legend to pick a topic — the right panel lists every sentence where that topic is <strong>dominant</strong> (same color as in the transcript). Hover sentences for the full cluster breakdown.</p>
</div>

<div class="main-grid">
  <main class="col-left" aria-label="Full transcript">
    {''.join(turn_blocks)}
  </main>
  <aside class="col-right" id="topic-aside" aria-label="Filtered by topic">
    <h2 class="panel-head" id="panel-head">Topic filter</h2>
    <p class="panel-sub" id="panel-sub"></p>
    <p class="panel-empty" id="panel-empty">Click a topic in the legend to list matching sentences.</p>
    <div id="panel-list"></div>
  </aside>
</div>

<script>
(function () {{
  const dataEl = document.getElementById("salience-app-data");
  if (!dataEl) return;
  let APP;
  try {{
    APP = JSON.parse(dataEl.textContent);
  }} catch (e) {{
    return;
  }}
  const HITS = APP.hits || {{}};
  const COLORS = APP.colors || {{}};
  const headEl = document.getElementById("panel-head");
  const subEl = document.getElementById("panel-sub");
  const emptyEl = document.getElementById("panel-empty");
  const listEl = document.getElementById("panel-list");
  const buttons = document.querySelectorAll(".legend-float .legend-btn");

  function labelCluster(key) {{
    return String(key || "").replace(/_/g, " ");
  }}

  function setActive(cluster) {{
    buttons.forEach(function (b) {{
      const on = b.getAttribute("data-cluster") === cluster;
      b.classList.toggle("active", on);
      b.setAttribute("aria-pressed", on ? "true" : "false");
    }});
  }}

  function showTopic(cluster) {{
    setActive(cluster);
    const hits = HITS[cluster] || [];
    headEl.textContent = labelCluster(cluster);
    subEl.textContent = hits.length
      ? hits.length + " sentence" + (hits.length === 1 ? "" : "s") + " (dominant topic)"
      : "";

    listEl.innerHTML = "";
    if (!hits.length) {{
      emptyEl.style.display = "block";
      emptyEl.textContent = "No sentences where this topic is the dominant match.";
      return;
    }}
    emptyEl.style.display = "none";
    const accent = COLORS[cluster] || "#64748b";
    hits.forEach(function (h) {{
      const card = document.createElement("div");
      card.className = "hit-card";
      card.style.setProperty("--accent", accent);
      card.style.borderLeftColor = accent;
      const meta = document.createElement("div");
      meta.className = "hit-meta";
      meta.textContent = h.meta || "";
      const sc = document.createElement("span");
      sc.className = "hit-score";
      sc.textContent = "Salience score: " + Number(h.score).toFixed(2);
      const txt = document.createElement("div");
      txt.className = "hit-text";
      txt.textContent = h.text || "";
      card.appendChild(meta);
      card.appendChild(sc);
      card.appendChild(txt);
      listEl.appendChild(card);
    }});
  }}

  buttons.forEach(function (btn) {{
    btn.addEventListener("click", function () {{
      const c = btn.getAttribute("data-cluster");
      if (c) showTopic(c);
    }});
  }});
}})();
</script>
</body>
</html>
"""


def write_highlight_html(
    path: str | Path,
    turns_df: Any,
    **kwargs: Any,
) -> None:
    """Write :func:`highlight_call_html` output to ``path`` (UTF-8)."""
    Path(path).write_text(highlight_call_html(turns_df, **kwargs), encoding="utf-8")