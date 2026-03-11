import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Meridian MMM – Plan", layout="wide")

DEFAULT_LOCAL_XLSX = "mmm_data_download.xlsx"
BUDGET_GRID_PREFIX = "budget_opt_grid"


# ---------- Utils ----------
def _norm_cols(df: pd.DataFrame) -> Dict[str, str]:
    m = {}
    for c in df.columns:
        nc = re.sub(r"[^a-z0-9]+", "_", str(c).strip().lower()).strip("_")
        m[nc] = c
    return m


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    m = _norm_cols(df)
    for cand in candidates:
        if cand in m:
            return m[cand]
    return None


def parse_pct_list(s: str) -> List[float]:
    parts = [p.strip() for p in str(s).split(",") if p.strip()]
    out = []
    for p in parts:
        p = p.replace("%", "").strip()
        try:
            out.append(float(p) / 100.0)
        except Exception:
            continue
    return out


def parse_channel_list(user_text: str, channels: List[str]) -> List[str]:
    if not user_text:
        return []
    tokens = [t.strip() for t in user_text.split(",") if t.strip()]
    if not tokens:
        return []
    ch_lower = {c.lower(): c for c in channels}
    chosen = []
    for tok in tokens:
        tl = tok.lower()
        if tl in ch_lower:
            chosen.append(ch_lower[tl])
            continue
        matches = [c for c in channels if tl in c.lower()]
        if len(matches) == 1:
            chosen.append(matches[0])
        elif len(matches) > 1:
            chosen.append(sorted(matches, key=len)[0])
    seen = set()
    out = []
    for c in chosen:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def fmt_money(x: float, prefix: str = "IDR ") -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    x = float(x)
    absx = abs(x)
    if absx >= 1_000_000_000:
        return f"{prefix}{x / 1_000_000_000:.1f}B"
    if absx >= 1_000_000:
        return f"{prefix}{x / 1_000_000:.1f}M"
    if absx >= 1_000:
        return f"{prefix}{x / 1_000:.0f}K"
    return f"{prefix}{x:,.0f}"


def fmt_num(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{float(x):,.0f}"


# ---------- Load local Excel (or uploaded) ----------
@st.cache_data(ttl=300, show_spinner=False)
def load_workbook(file_bytes: Optional[bytes], local_path: str):
    if file_bytes is None:
        xls = pd.ExcelFile(local_path, engine="openpyxl")
    else:
        xls = pd.ExcelFile(file_bytes, engine="openpyxl")
    sheet_names = xls.sheet_names

    def read_sheet(name: str) -> pd.DataFrame:
        df = pd.read_excel(xls, sheet_name=name)
        for c in df.columns:
            s = df[c]
            if s.dtype == object:
                sn = pd.to_numeric(s, errors="coerce")
                if sn.notna().mean() >= 0.7:
                    df[c] = sn
        return df

    if "budget_opt_specs" not in sheet_names:
        raise ValueError("Missing required sheet: budget_opt_specs")
    specs = read_sheet("budget_opt_specs")
    results = (
        read_sheet("budget_opt_results")
        if "budget_opt_results" in sheet_names
        else None
    )

    grid_sheet = None
    for n in sheet_names:
        if str(n).startswith(BUDGET_GRID_PREFIX):
            grid_sheet = n
            break
    if grid_sheet is None:
        raise ValueError(
            "Missing required sheet: budget_opt_grid_* (e.g. budget_opt_grid_poc-mmm_ALL)"
        )
    grid = read_sheet(grid_sheet)

    return sheet_names, specs, results, grid, grid_sheet


# ---------- MMM parsing ----------
def prep_groups(specs: pd.DataFrame) -> pd.DataFrame:
    group_col = pick_col(specs, ["group_id", "groupid", "group"])
    if group_col is None:
        return pd.DataFrame()
    cols = [group_col]
    for cand in [
        ["analysis_period", "analysisperiod"],
        ["date_interval_start", "date_interval_start_date", "date_interval_start_time"],
        ["date_interval_end", "date_interval_end_date", "date_interval_end_time"],
        ["objective"],
        ["scenario_type"],
    ]:
        c = pick_col(specs, cand)
        if c and c not in cols:
            cols.append(c)
    g = specs.groupby(group_col, dropna=False).first().reset_index()
    return g[cols].copy()


def constraints_for_group(
    specs: pd.DataFrame, group_id: str
) -> Tuple[pd.DataFrame, str, str, str, str]:
    group_col = pick_col(specs, ["group_id", "groupid", "group"])
    ch_col = pick_col(specs, ["channel", "media_channel", "paid_channel"])
    init_col = pick_col(
        specs,
        [
            "initial_channel_spend",
            "initial_spend",
            "initial_spend_amount",
            "initial_channel_budget",
            "non_optimized_spend",
        ],
    )
    min_col = pick_col(specs, ["channel_spend_min", "min_spend", "spend_min"])
    max_col = pick_col(specs, ["channel_spend_max", "max_spend", "spend_max"])
    if not all([group_col, ch_col, init_col, min_col, max_col]):
        raise ValueError(
            "budget_opt_specs missing required columns for Plan (group/channel/initial/min/max)."
        )
    df = specs[specs[group_col].astype(str) == str(group_id)].copy()
    for col in [init_col, min_col, max_col]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[ch_col])
    df[ch_col] = df[ch_col].astype(str)
    return df, ch_col, init_col, min_col, max_col


def response_curves_from_grid(
    grid: pd.DataFrame, group_id: str
) -> Dict[str, pd.DataFrame]:
    group_col = pick_col(grid, ["group_id", "groupid", "group"])
    ch_col = pick_col(grid, ["channel", "media_channel", "paid_channel"])
    spend_col = pick_col(grid, ["spend", "channel_spend"])
    inc_col = pick_col(
        grid,
        ["incremental_outcome", "incremental_kpi", "incremental_sales", "incremental"],
    )
    if not all([group_col, ch_col, spend_col, inc_col]):
        raise ValueError(
            "budget_opt_grid_* missing required columns (group/channel/spend/incremental)."
        )
    gdf = grid[grid[group_col].astype(str) == str(group_id)].copy()
    gdf[spend_col] = pd.to_numeric(gdf[spend_col], errors="coerce")
    gdf[inc_col] = pd.to_numeric(gdf[inc_col], errors="coerce")
    gdf = gdf.dropna(subset=[ch_col, spend_col, inc_col])
    lookup: Dict[str, pd.DataFrame] = {}
    for ch, d in gdf.groupby(ch_col):
        d = d.sort_values(spend_col).drop_duplicates(spend_col)
        lookup[str(ch)] = (
            d[[spend_col, inc_col]]
            .rename(columns={spend_col: "Spend", inc_col: "Incremental"})
            .reset_index(drop=True)
        )
    return lookup


def interp_incremental(curve: pd.DataFrame, spend: float) -> float:
    x = curve["Spend"].to_numpy(dtype=float)
    y = curve["Incremental"].to_numpy(dtype=float)
    if len(x) == 0:
        return 0.0
    s = float(np.clip(spend, x.min(), x.max()))
    return float(np.interp(s, x, y))


# ---------- Allocation helpers ----------
def rebalance_to_total(
    alloc: Dict[str, float],
    mins: Dict[str, float],
    maxs: Dict[str, float],
    total: float,
) -> Dict[str, float]:
    channels = list(alloc.keys())
    cur = {ch: float(np.clip(alloc[ch], mins[ch], maxs[ch])) for ch in channels}
    for _ in range(30):
        diff = float(total) - sum(cur.values())
        if abs(diff) < 1e-6:
            break
        if diff > 0:
            headroom = {ch: maxs[ch] - cur[ch] for ch in channels}
        else:
            headroom = {ch: cur[ch] - mins[ch] for ch in channels}
        avail = {ch: hr for ch, hr in headroom.items() if hr > 1e-9}
        if not avail:
            break
        hr_sum = sum(avail.values())
        for ch, hr in avail.items():
            cur[ch] += diff * (hr / hr_sum)
        cur = {ch: float(np.clip(cur[ch], mins[ch], maxs[ch])) for ch in channels}
    return cur


def non_optimized_at_budget(
    init: Dict[str, float],
    mins: Dict[str, float],
    maxs: Dict[str, float],
    budget: float,
) -> Dict[str, float]:
    s0 = sum(init.values())
    factor = (budget / s0) if s0 > 0 else 1.0
    alloc = {ch: init[ch] * factor for ch in init.keys()}
    return rebalance_to_total(alloc, mins, maxs, budget)


def discrete_optimize(
    curves: Dict[str, pd.DataFrame],
    mins: Dict[str, float],
    maxs: Dict[str, float],
    total_budget: float,
) -> Dict[str, float]:
    channels = list(curves.keys())
    grids = {}
    for ch in channels:
        d = curves[ch].copy()
        d = d[(d["Spend"] >= mins[ch]) & (d["Spend"] <= maxs[ch])].sort_values("Spend")
        if d.empty:
            grids[ch] = pd.DataFrame(
                {
                    "Spend": [mins[ch], maxs[ch]],
                    "Incremental": [
                        interp_incremental(curves[ch], mins[ch]),
                        interp_incremental(curves[ch], maxs[ch]),
                    ],
                }
            )
        else:
            grids[ch] = d.reset_index(drop=True)
        if grids[ch]["Spend"].iloc[0] > mins[ch] + 1e-9:
            grids[ch] = pd.concat(
                [
                    pd.DataFrame(
                        {
                            "Spend": [mins[ch]],
                            "Incremental": [interp_incremental(curves[ch], mins[ch])],
                        }
                    ),
                    grids[ch],
                ],
                ignore_index=True,
            )
        if grids[ch]["Spend"].iloc[-1] < maxs[ch] - 1e-9:
            grids[ch] = pd.concat(
                [
                    grids[ch],
                    pd.DataFrame(
                        {
                            "Spend": [maxs[ch]],
                            "Incremental": [interp_incremental(curves[ch], maxs[ch])],
                        }
                    ),
                ],
                ignore_index=True,
            )

    idx = {ch: 0 for ch in channels}
    alloc = {ch: float(grids[ch]["Spend"].iloc[0]) for ch in channels}
    remaining = float(total_budget) - sum(alloc.values())
    if remaining < 0:
        return alloc

    for _ in range(200000):
        best = None
        best_slope = -1e18
        for ch in channels:
            i = idx[ch]
            g = grids[ch]
            if i >= len(g) - 1:
                continue
            s0, y0 = float(g["Spend"].iloc[i]), float(g["Incremental"].iloc[i])
            s1, y1 = float(g["Spend"].iloc[i + 1]), float(g["Incremental"].iloc[i + 1])
            ds = s1 - s0
            dy = y1 - y0
            if ds <= 0:
                continue
            if ds <= remaining + 1e-9:
                slope = dy / ds
                if slope > best_slope:
                    best_slope = slope
                    best = ch
        if best is None:
            break
        ch = best
        i = idx[ch]
        g = grids[ch]
        s_next = float(g["Spend"].iloc[i + 1])
        ds = s_next - float(g["Spend"].iloc[i])
        idx[ch] += 1
        alloc[ch] = s_next
        remaining -= ds
        if remaining <= 1e-6:
            break
    return alloc


def total_outcome(curves: Dict[str, pd.DataFrame], alloc: Dict[str, float]) -> float:
    return float(sum(interp_incremental(curves[ch], alloc[ch]) for ch in alloc.keys()))


# ---------- UI ----------
st.title("Marketing Mix Modelling - Buddget Optimization Engine")
# st.caption(
#     "Focus on Plan only. Data source: local Excel (`mmm_data_download.xlsx`) or uploaded Excel."
# )

with st.sidebar:
    st.header("Data")
    local_path = st.text_input("Local file path", value=DEFAULT_LOCAL_XLSX)
    uploaded = st.file_uploader("Or upload Excel", type=["xlsx", "xls"])
    prefix = st.text_input("Currency prefix", value="IDR ")

try:
    file_bytes = uploaded.getvalue() if uploaded is not None else None
    sheet_names, specs, results, grid, grid_sheet = load_workbook(
        file_bytes, local_path
    )
except Exception as e:
    st.error(str(e))
    st.stop()

# st.success(f"Loaded workbook. Using grid sheet: `{grid_sheet}`")

groups = prep_groups(specs)
if groups.empty:
    st.error("Cannot find Group ID in budget_opt_specs.")
    st.stop()

group_id_col = groups.columns[0]
ap_col = pick_col(groups, ["analysis_period", "analysisperiod"])
s_col = pick_col(
    groups,
    ["date_interval_start", "date_interval_start_date", "date_interval_start_time"],
)
e_col = pick_col(
    groups, ["date_interval_end", "date_interval_end_date", "date_interval_end_time"]
)

labels = []
label_to_gid = {}
for _, row in groups.iterrows():
    gid = str(row[group_id_col])
    ap = str(row.get(ap_col, "ALL")) if ap_col else "ALL"
    s = str(row.get(s_col, "")) if s_col else ""
    e = str(row.get(e_col, "")) if e_col else ""
    label = f"{ap} : {s} => {e}".strip()
    labels.append(label)
    label_to_gid[label] = gid

with st.sidebar:
    st.header("Plan")
    sel_label = st.selectbox("Date range", labels, index=0)
    selected_group = label_to_gid[sel_label]
    revenue_per_kpi = st.number_input(
        "Revenue per KPI (optional)", min_value=0.0, value=1.0, step=1.0
    )

con_df, ch_col, init_col, min_col, max_col = constraints_for_group(
    specs, selected_group
)
channels = con_df[ch_col].astype(str).tolist()
init_spend0 = {
    ch: float(con_df.loc[con_df[ch_col] == ch, init_col].iloc[0]) for ch in channels
}
min_spend0 = {
    ch: float(con_df.loc[con_df[ch_col] == ch, min_col].iloc[0]) for ch in channels
}
max_spend0 = {
    ch: float(con_df.loc[con_df[ch_col] == ch, max_col].iloc[0]) for ch in channels
}
init_total0 = float(np.nansum(list(init_spend0.values())))
curves = response_curves_from_grid(grid, selected_group)

st.subheader("1. Enter the amount you plan to spend")
planned_budget = st.number_input(
    "Initial budget", min_value=0.0, value=float(init_total0), step=1.0
)

st.subheader("2. Is your budget fixed or flexible?")
constraint_type = st.selectbox(
    "Budget constraint type",
    [
        "Fixed: Maximize ROI at my set budget",
        "Flexible: Target Total ROI",
        "Flexible: Target Marginal ROI (mROI)",
    ],
    index=0,
)

target_value = None
if constraint_type.startswith("Flexible"):
    st.markdown(
        "**[Flexible plans only]** If flexible, what ROI or marginal ROI do you want to achieve?"
    )
    if "Total ROI" in constraint_type:
        target_value = st.number_input(
            "Flexible budget constraint value (Target Total ROI)",
            min_value=0.0,
            value=1.0,
            step=0.05,
        )
    else:
        target_value = st.number_input(
            "Flexible budget constraint value (Target Marginal ROI)",
            min_value=0.0,
            value=0.01,
            step=0.001,
        )

st.subheader(
    "3. For each channel, enter how much less and more you're willing to spend as percentages"
)
bounded_txt = st.text_input("Spend bounded channels", value=", ".join(channels))
lower_txt = st.text_input("Spend lower bound ratios", value="-100%")
upper_txt = st.text_input("Spend upper bound ratios", value="100%")

bounded = parse_channel_list(bounded_txt, channels) or channels[:]
lowers = parse_pct_list(lower_txt)
uppers = parse_pct_list(upper_txt)
if len(lowers) == 1 and len(bounded) > 1:
    lowers = lowers * len(bounded)
if len(uppers) == 1 and len(bounded) > 1:
    uppers = uppers * len(bounded)

min_spend = dict(min_spend0)
max_spend = dict(max_spend0)
lb_ratio = {ch: None for ch in channels}
ub_ratio = {ch: None for ch in channels}
for i, ch in enumerate(bounded):
    lr = lowers[i] if i < len(lowers) else (-1.0)
    ur = uppers[i] if i < len(uppers) else (1.0)
    base = init_spend0[ch]
    min_spend[ch] = max(0.0, base * (1.0 + lr))
    max_spend[ch] = max(0.0, base * (1.0 + ur))
    lb_ratio[ch] = lr
    ub_ratio[ch] = ur

# st.subheader("4. Review settings")
review = pd.DataFrame(
    {
        "Channel": channels,
        "Non-optimized spend": [init_spend0[ch] for ch in channels],
        "Spend lower bound ratio": [
            f"{(lb_ratio[ch] * 100):.0f}%" if lb_ratio[ch] is not None else ""
            for ch in channels
        ],
        "Spend upper bound ratio": [
            f"{(ub_ratio[ch] * 100):.0f}%" if ub_ratio[ch] is not None else ""
            for ch in channels
        ],
        "Spend lower bound": [min_spend[ch] for ch in channels],
        "Spend upper bound": [max_spend[ch] for ch in channels],
    }
)
# st.dataframe(review, use_container_width=True, hide_index=True)

st.divider()
if st.button("Allocate & Optimize budget"):
    # Fixed only (simple + stable). Flexible kept as-is but can be disabled if you want.
    if constraint_type.startswith("Fixed"):
        budget_used = float(planned_budget)
        opt_alloc = discrete_optimize(curves, min_spend, max_spend, budget_used)
    else:
        # Best-effort flexible search
        def total_roi_at(b):
            a = discrete_optimize(curves, min_spend, max_spend, b)
            y = total_outcome(curves, a) * float(revenue_per_kpi)
            r = (y / b) if b > 0 else np.nan
            return r, a

        def total_mroi_at(b, delta=0.01):
            b1 = min(b * (1 + delta), sum(max_spend.values()))
            a0 = discrete_optimize(curves, min_spend, max_spend, b)
            a1 = discrete_optimize(curves, min_spend, max_spend, b1)
            y0 = total_outcome(curves, a0)
            y1 = total_outcome(curves, a1)
            return (y1 - y0) / (b1 - b) if (b1 - b) > 0 else np.nan, a0

        lo = float(sum(min_spend.values()))
        hi = float(sum(max_spend.values()))
        best_b = lo
        if "Total ROI" in constraint_type:
            target = float(target_value)
            opt_alloc = discrete_optimize(curves, min_spend, max_spend, lo)
            for _ in range(30):
                mid = (lo + hi) / 2
                r_mid, a_mid = total_roi_at(mid)
                if np.isnan(r_mid):
                    hi = mid
                elif r_mid >= target:
                    best_b = mid
                    hi = mid
                    opt_alloc = a_mid
                else:
                    lo = mid
            budget_used = best_b
        else:
            target = float(target_value)
            opt_alloc = discrete_optimize(curves, min_spend, max_spend, lo)
            for _ in range(30):
                mid = (lo + hi) / 2
                m_mid, a_mid = total_mroi_at(mid)
                if np.isnan(m_mid):
                    hi = mid
                elif m_mid >= target:
                    best_b = mid
                    lo = mid
                    opt_alloc = a_mid
                else:
                    hi = mid
            budget_used = best_b

    opt_total_outcome = total_outcome(curves, opt_alloc)
    opt_total_roi = (
        (opt_total_outcome * float(revenue_per_kpi)) / budget_used
        if budget_used > 0
        else np.nan
    )

    non_alloc = non_optimized_at_budget(
        init_spend0, min_spend, max_spend, float(budget_used)
    )
    non_total_outcome = total_outcome(curves, non_alloc)
    non_total_roi = (
        (non_total_outcome * float(revenue_per_kpi)) / float(budget_used)
        if budget_used > 0
        else np.nan
    )

    rows = []
    for ch in channels:
        ob = float(opt_alloc.get(ch, 0.0))
        nb = float(non_alloc.get(ch, 0.0))
        oy = interp_incremental(curves[ch], ob)
        ny = interp_incremental(curves[ch], nb)
        rows.append(
            {
                "Channel": ch,
                "Optimized budget": ob,
                "Non-optimized budget": nb,
                "Optimized outcome": oy,
                "Non-optimized outcome": ny,
                "Δ budget": ob - nb,
            }
        )
    per = pd.DataFrame(rows).sort_values("Optimized budget", ascending=False)

    st.header("Optimization overview")
    c1, c2, c3 = st.columns(3)
    c1.metric("Optimized total budget", fmt_money(budget_used, prefix=prefix))
    c2.metric(
        "Optimized total ROI",
        f"{opt_total_roi:.3f}" if not np.isnan(opt_total_roi) else "-",
        delta=f"{(opt_total_roi - non_total_roi):.3f}"
        if (not np.isnan(opt_total_roi) and not np.isnan(non_total_roi))
        else None,
    )
    c3.metric(
        "Optimized total outcome",
        fmt_num(opt_total_outcome),
        delta=fmt_num(opt_total_outcome - non_total_outcome),
    )

    t = per[
        [
            "Channel",
            "Optimized budget",
            "Non-optimized budget",
            "Optimized outcome",
            "Non-optimized outcome",
        ]
    ].copy()
    grand = pd.DataFrame(
        [
            {
                "Channel": "Grand total",
                "Optimized budget": float(budget_used),
                "Non-optimized budget": float(budget_used),
                "Optimized outcome": float(opt_total_outcome),
                "Non-optimized outcome": float(non_total_outcome),
            }
        ]
    )
    st.dataframe(
        pd.concat([t, grand], ignore_index=True),
        use_container_width=True,
        hide_index=True,
    )

    st.header("Optimized budget allocation")
    st.subheader("Top channels by budget")
    top = per.head(5)
    cols = st.columns(min(5, len(top)))
    for i, (_, r) in enumerate(top.iterrows()):
        cols[i].metric(
            r["Channel"],
            fmt_money(r["Optimized budget"], prefix=prefix),
            delta=fmt_money(r["Δ budget"], prefix=prefix),
        )

    st.subheader("Change in optimized budget for each channel")
    dbar = per.copy()
    dbar["Direction"] = np.where(dbar["Δ budget"] >= 0, "Increase", "Decrease")
    fig1 = px.bar(dbar, x="Channel", y="Δ budget", color="Direction")
    fig1.update_layout(height=420, yaxis_title="Δ budget", xaxis_title="")
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Optimized budget allocation")
    fig2 = px.pie(per, names="Channel", values="Optimized budget", hole=0.6)
    fig2.update_layout(height=420, legend_title_text="")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Budget per channel")
    bdf = per[["Channel", "Non-optimized budget", "Optimized budget"]].melt(
        id_vars="Channel", var_name="Type", value_name="Budget"
    )
    fig3 = px.bar(bdf, x="Channel", y="Budget", color="Type", barmode="group")
    fig3.update_layout(height=420, yaxis_title="Budget", xaxis_title="")
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Incremental outcome per channel")
    odf = per[["Channel", "Non-optimized outcome", "Optimized outcome"]].melt(
        id_vars="Channel", var_name="Type", value_name="Outcome"
    )
    fig4 = px.bar(odf, x="Channel", y="Outcome", color="Type", barmode="group")
    fig4.update_layout(height=420, yaxis_title="Outcome", xaxis_title="")
    st.plotly_chart(fig4, use_container_width=True)
