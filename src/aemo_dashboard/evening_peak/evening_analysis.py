"""
Core analysis functions for NEM Evening Peak Fuel Mix & Price comparison.

Adapted from analysis_code/evening_analysis.py for use as a dashboard tab.
Data source: DuckDB (tables: scada30, rooftop30, prices30, demand30, duid_mapping).
"""

import os
import time
import functools

import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime, timedelta

import panel as pn


DB_PATH = os.getenv(
    "AEMO_DUCKDB_PATH",
    "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb",
)

FLEXOKI = {
    "background": "#FFFCF0",
    "foreground": "#100F0F",
    "text": "#403E3C",
    "muted": "#6F6E69",
    "blue": "#205EA6",
    "orange": "#BC5215",
    "cyan": "#24837B",
    "magenta": "#A02F6F",
    "green": "#66800B",
    "red": "#D14D41",
    "yellow": "#AD8301",
    "purple": "#5E409D",
}

FUEL_ORDER = ["Net Imports", "Coal", "Gas", "Hydro", "Wind", "Solar", "Rooftop Solar", "Battery", "Other"]
FUEL_COLORS = {
    "Net Imports": FLEXOKI["green"],
    "Coal": "#6F6E69",
    "Gas": FLEXOKI["orange"],
    "Hydro": FLEXOKI["blue"],
    "Wind": FLEXOKI["cyan"],
    "Solar": FLEXOKI["yellow"],
    "Rooftop Solar": "#B4A06E",
    "Battery": FLEXOKI["magenta"],
    "Other": FLEXOKI["purple"],
}

FUEL_MAPPING = {
    "Coal": "Coal",
    "CCGT": "Gas",
    "OCGT": "Gas",
    "Gas other": "Gas",
    "Water": "Hydro",
    "Wind": "Wind",
    "Solar": "Solar",
    "Battery Storage": "Battery",
    "Biomass": "Other",
    "Other": "Other",
}


def _query(sql, max_retries=3, retry_delay=0.2):
    last_err = None
    for attempt in range(max_retries):
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            df = con.execute(sql).df()
            con.close()
            return df
        except duckdb.IOException as e:
            last_err = e
            time.sleep(retry_delay * (attempt + 1))
    raise last_err


@functools.lru_cache(maxsize=1)
def _load_gen_info():
    """Load DUID→fuel/region mappings + battery DUIDs. Lazy & cached to avoid startup DB hits."""
    mapping = _query("SELECT duid, fuel, region FROM duid_mapping")

    duid_to_fuel = dict(zip(mapping["duid"], mapping["fuel"]))
    duid_to_region = dict(zip(mapping["duid"], mapping["region"]))
    battery_duids = set(mapping[mapping["fuel"] == "Battery Storage"]["duid"].tolist())

    all_scada_duids = _query("SELECT DISTINCT duid FROM scada30")["duid"].tolist()

    battery_patterns = [
        "BESS", "BAT", "HPR", "VBB", "ERB", "TIB", "RANG", "MREH", "WALG", "WDB",
        "DALNTH", "GANN", "LBB", "LDBE", "LIMBE", "LVES", "MANNUM", "QBYN", "RESS",
        "RIVN", "SMTH", "SNB", "TARB", "TB2B", "TEMP", "ULPB", "WAND", "WTAHB",
        "BLYTHB", "BUNG", "CAP", "ADP", "BALB", "BHB", "BULB", "CBWW", "BOWWB",
        "HVWW", "KEPB", "PIBE", "GREEN", "BRND", "ORAB", "CGB", "KESS",
    ]

    for duid in all_scada_duids:
        if duid.endswith("L1") or duid.endswith("L"):
            continue
        if duid.endswith("WF") or duid.endswith("SF") or duid.endswith("PV1"):
            continue
        if any(pat in duid.upper() for pat in battery_patterns):
            if duid not in duid_to_fuel:
                duid_to_fuel[duid] = "Battery Storage"
                battery_duids.add(duid)

    return duid_to_fuel, duid_to_region, battery_duids


@pn.cache(max_items=24, policy="LRU", ttl=600)
def get_evening_data(start_date: str, end_date: str, region: str = "NEM"):
    """
    Get evening peak (17:00-22:00) generation + price data.

    Returns:
        avg_by_time: DataFrame of average generation by time-of-day
        avg_price_by_time: Series of demand-weighted prices by time-of-day
        stats: dict with total/battery/rooftop/net_imports/price/fuel_averages
    """
    duid_to_fuel, duid_to_region, _battery_duids = _load_gen_info()

    if region == "NEM":
        region_duids = None
    else:
        region_duids = [duid for duid, reg in duid_to_region.items() if reg == region]

    scada_df = _query(f"""
        SELECT
            settlementdate,
            duid,
            CASE WHEN scadavalue > 0 THEN scadavalue ELSE 0 END as generation
        FROM scada30
        WHERE settlementdate >= '{start_date}'
        AND settlementdate < '{end_date}'
        AND EXTRACT(HOUR FROM settlementdate) >= 17
        AND EXTRACT(HOUR FROM settlementdate) < 22
    """)

    if region_duids is not None:
        scada_df = scada_df[scada_df["duid"].isin(region_duids)]

    scada_df["fuel_raw"] = scada_df["duid"].map(duid_to_fuel).fillna("Other")
    scada_df["fuel"] = scada_df["fuel_raw"].map(FUEL_MAPPING).fillna("Other")
    scada_df["region"] = scada_df["duid"].map(duid_to_region)

    fuel_by_time = scada_df.groupby(["settlementdate", "fuel"])["generation"].sum().unstack(fill_value=0)

    for fuel in FUEL_ORDER:
        if fuel not in fuel_by_time.columns and fuel not in ("Rooftop Solar", "Net Imports"):
            fuel_by_time[fuel] = 0

    if region == "NEM":
        rooftop_region_filter = "AND regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')"
    else:
        rooftop_region_filter = f"AND regionid = '{region}'"

    rooftop_df = _query(f"""
        SELECT
            settlementdate,
            SUM(power) as rooftop_mw
        FROM rooftop30
        WHERE settlementdate >= '{start_date}'
        AND settlementdate < '{end_date}'
        AND EXTRACT(HOUR FROM settlementdate) >= 17
        AND EXTRACT(HOUR FROM settlementdate) < 22
        {rooftop_region_filter}
        GROUP BY settlementdate
    """)
    rooftop_df = rooftop_df.set_index("settlementdate")

    fuel_by_time = fuel_by_time.join(rooftop_df["rooftop_mw"].rename("Rooftop Solar"), how="left")
    fuel_by_time["Rooftop Solar"] = fuel_by_time["Rooftop Solar"].fillna(0)

    if region == "NEM":
        fuel_by_time["Net Imports"] = 0.0
    else:
        demand_df = _query(f"""
            SELECT settlementdate, demand
            FROM demand30
            WHERE settlementdate >= '{start_date}'
            AND settlementdate < '{end_date}'
            AND EXTRACT(HOUR FROM settlementdate) >= 17
            AND EXTRACT(HOUR FROM settlementdate) < 22
            AND regionid = '{region}'
        """)
        demand_df = demand_df.set_index("settlementdate")
        fuel_by_time = fuel_by_time.join(demand_df["demand"], how="left")
        fuel_by_time["demand"] = fuel_by_time["demand"].fillna(0)
        gen_cols = [c for c in FUEL_ORDER if c != "Net Imports" and c in fuel_by_time.columns]
        fuel_by_time["Net Imports"] = fuel_by_time["demand"] - fuel_by_time[gen_cols].sum(axis=1)
        fuel_by_time.drop(columns=["demand"], inplace=True)

    if region == "NEM":
        prices_df = _query(f"""
            SELECT
                p.settlementdate,
                SUM(p.rrp * d.demand) / NULLIF(SUM(d.demand), 0) AS weighted_price
            FROM prices30 p
            JOIN demand30 d
              ON p.settlementdate = d.settlementdate
             AND p.regionid = d.regionid
            WHERE p.settlementdate >= '{start_date}'
            AND p.settlementdate < '{end_date}'
            AND EXTRACT(HOUR FROM p.settlementdate) >= 17
            AND EXTRACT(HOUR FROM p.settlementdate) < 22
            AND p.regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')
            GROUP BY p.settlementdate
            ORDER BY p.settlementdate
        """)
        weighted_prices = prices_df.set_index("settlementdate")
    else:
        prices_df = _query(f"""
            SELECT
                settlementdate,
                rrp AS weighted_price
            FROM prices30
            WHERE settlementdate >= '{start_date}'
            AND settlementdate < '{end_date}'
            AND EXTRACT(HOUR FROM settlementdate) >= 17
            AND EXTRACT(HOUR FROM settlementdate) < 22
            AND regionid = '{region}'
            ORDER BY settlementdate
        """)
        weighted_prices = prices_df.set_index("settlementdate")

    fuel_by_time["time"] = fuel_by_time.index.strftime("%H:%M")
    avg_by_time = fuel_by_time.groupby("time").mean()

    weighted_prices["time"] = weighted_prices.index.strftime("%H:%M")
    avg_price_by_time = weighted_prices.groupby("time")["weighted_price"].mean()

    fuel_averages = {}
    for fuel in FUEL_ORDER:
        if fuel in avg_by_time.columns:
            fuel_averages[fuel] = avg_by_time[fuel].mean()
        else:
            fuel_averages[fuel] = 0

    total_gen = sum(fuel_averages.values())
    avg_price = avg_price_by_time.mean()

    return avg_by_time, avg_price_by_time, {
        "total": total_gen,
        "battery": fuel_averages.get("Battery", 0),
        "rooftop": fuel_averages.get("Rooftop Solar", 0),
        "net_imports": fuel_averages.get("Net Imports", 0),
        "price": avg_price,
        "fuel_averages": fuel_averages,
    }


def create_comparison_figure(this_year_data, this_year_prices, this_year_stats,
                             last_year_data, last_year_prices, last_year_stats,
                             region: str, period_days: int,
                             this_year_start: str, this_year_end: str,
                             last_year_start: str, last_year_end: str):
    """4-panel matplotlib figure comparing current evening peak window to PCP."""
    max_price = max(last_year_prices.max(), this_year_prices.max()) * 1.2

    ty_start_fmt = datetime.strptime(this_year_start, "%Y-%m-%d").strftime("%d %b %Y")
    ty_end_fmt = (datetime.strptime(this_year_end, "%Y-%m-%d") - timedelta(days=1)).strftime("%d %b %Y")
    ly_start_fmt = datetime.strptime(last_year_start, "%Y-%m-%d").strftime("%d %b %Y")
    ly_end_fmt = (datetime.strptime(last_year_end, "%Y-%m-%d") - timedelta(days=1)).strftime("%d %b %Y")

    fig = plt.figure(figsize=(16, 12))
    fig.patch.set_facecolor(FLEXOKI["background"])

    gs = fig.add_gridspec(2, 2, height_ratios=[1.1, 0.9], wspace=0.25, hspace=0.35)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax_price = fig.add_subplot(gs[1, 0])
    ax3 = fig.add_subplot(gs[1, 1])

    def plot_panel(ax, data, stats, title, show_y_label=True, show_battery_arrow=False):
        ax.set_facecolor(FLEXOKI["background"])

        times = data.index.tolist()
        x = range(len(times))
        bottom = np.zeros(len(times))
        battery_top = None
        battery_bottom = None
        has_negative_imports = False

        for fuel in FUEL_ORDER:
            if fuel not in data.columns:
                continue
            values = data[fuel].values

            if fuel == "Net Imports":
                if np.allclose(values, 0):
                    continue
                pos = np.maximum(values, 0)
                neg = np.minimum(values, 0)
                if pos.any():
                    ax.fill_between(x, bottom, bottom + pos,
                                    label=fuel, color=FUEL_COLORS[fuel], alpha=0.85)
                    bottom += pos
                if neg.any():
                    has_negative_imports = True
                    lbl = fuel if not pos.any() else None
                    ax.fill_between(x, neg, 0, label=lbl,
                                    color=FUEL_COLORS[fuel], alpha=0.4)
            else:
                ax.fill_between(x, bottom, bottom + values,
                                label=fuel, color=FUEL_COLORS[fuel], alpha=0.85)
                if fuel == "Battery":
                    battery_bottom = bottom.copy()
                    battery_top = bottom + values
                bottom += values

        if has_negative_imports:
            ax.axhline(y=0, color=FLEXOKI["muted"], linewidth=0.8, alpha=0.6)

        ax.set_xlabel("Time of Day", color=FLEXOKI["text"], fontsize=12)
        if show_y_label:
            ax.set_ylabel("Generation (GW)", color=FLEXOKI["text"], fontsize=12)
        ax.set_title(title, color=FLEXOKI["foreground"], fontsize=16, fontweight="bold", pad=10)

        ax.set_xticks(range(0, len(times), 2))
        ax.set_xticklabels([times[i] for i in range(0, len(times), 2)], color=FLEXOKI["text"], fontsize=11)
        ax.tick_params(axis="y", colors=FLEXOKI["text"], labelsize=11)

        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x/1000:.0f}"))

        if show_battery_arrow and battery_top is not None and stats["battery"] > 100:
            arrow_idx = len(times) - 2
            arrow_y = (battery_bottom[arrow_idx] + battery_top[arrow_idx]) / 2
            total_gen = stats["total"]
            text_offset_y = total_gen * 0.15
            ax.annotate(f"Battery\n{stats['battery']/1000:.1f} GW",
                        xy=(arrow_idx, arrow_y),
                        xytext=(len(times) + 0.5, battery_top[arrow_idx] + text_offset_y * 0.3),
                        fontsize=10, fontweight="bold", color=FLEXOKI["magenta"],
                        arrowprops=dict(arrowstyle="->", color=FLEXOKI["magenta"], lw=2),
                        ha="left", annotation_clip=False)

        ax.grid(False)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.tick_params(length=0)

    ly_title = f"PCP: {ly_start_fmt} - {ly_end_fmt}"
    ty_title = f"Current: {ty_start_fmt} - {ty_end_fmt}"

    plot_panel(ax1, last_year_data, last_year_stats, ly_title, show_y_label=True, show_battery_arrow=False)
    plot_panel(ax2, this_year_data, this_year_stats, ty_title, show_y_label=False, show_battery_arrow=True)

    ax_price.set_facecolor(FLEXOKI["background"])

    times = last_year_data.index.tolist()
    price_values_ly = [last_year_prices.get(t, np.nan) for t in times]
    price_values_ty = [this_year_prices.get(t, np.nan) for t in times]

    ax_price.plot(range(len(times)), price_values_ly, "-", color=FLEXOKI["blue"],
                  linewidth=3, solid_capstyle="round", label="PCP")
    ax_price.plot(range(len(times)), price_values_ty, "-", color=FLEXOKI["orange"],
                  linewidth=3, solid_capstyle="round", label="Current")

    ax_price.set_xlabel("Time of Day", color=FLEXOKI["text"], fontsize=12)
    ax_price.set_ylabel("Price ($/MWh)", color=FLEXOKI["text"], fontsize=12)
    ax_price.set_title("Evening Price Comparison", color=FLEXOKI["foreground"], fontsize=14, fontweight="bold")

    ax_price.set_xticks(range(0, len(times), 2))
    ax_price.set_xticklabels([times[i] for i in range(0, len(times), 2)], color=FLEXOKI["text"], fontsize=11)
    ax_price.tick_params(axis="y", colors=FLEXOKI["text"], labelsize=11, length=0)
    ax_price.tick_params(axis="x", length=0)
    ax_price.set_ylim(0, max_price)

    ax_price.grid(False)
    for spine in ax_price.spines.values():
        spine.set_visible(False)

    ax_price.axhline(y=last_year_stats["price"], color=FLEXOKI["blue"], linestyle="--", alpha=0.5, linewidth=1.5)
    ax_price.axhline(y=this_year_stats["price"], color=FLEXOKI["orange"], linestyle="--", alpha=0.5, linewidth=1.5)

    ax_price.text(len(times) - 1, last_year_stats["price"] + 12, f"Avg ${last_year_stats['price']:.0f}",
                  va="bottom", ha="right", color=FLEXOKI["blue"], fontsize=10, fontweight="bold")
    ax_price.text(len(times) - 1, this_year_stats["price"] - 12, f"Avg ${this_year_stats['price']:.0f}",
                  va="top", ha="right", color=FLEXOKI["orange"], fontsize=10, fontweight="bold")

    ax_price.legend(loc="upper left", facecolor=FLEXOKI["background"], edgecolor="none",
                    labelcolor=FLEXOKI["foreground"], fontsize=11, framealpha=0.8)

    ax3.set_facecolor(FLEXOKI["background"])

    waterfall_fuels = [f for f in FUEL_ORDER if f not in ["Other", "Rooftop Solar"] and f != "Net Imports"]
    waterfall_fuels = ["Net Imports"] + waterfall_fuels

    changes_gw = {}
    for fuel in waterfall_fuels:
        ly_val = last_year_stats["fuel_averages"].get(fuel, 0) / 1000
        ty_val = this_year_stats["fuel_averages"].get(fuel, 0) / 1000
        changes_gw[fuel] = ty_val - ly_val

    waterfall_labels = ["PCP"] + waterfall_fuels + ["Current"]
    waterfall_values_gw = [last_year_stats["total"] / 1000]
    cumulative = last_year_stats["total"] / 1000

    for fuel in waterfall_fuels:
        waterfall_values_gw.append(changes_gw[fuel])
        cumulative += changes_gw[fuel]

    waterfall_values_gw.append(this_year_stats["total"] / 1000)

    bar_positions = range(len(waterfall_labels))
    bar_colors = []
    bar_bottoms = []

    TOTAL_BAR_COLOR = FLEXOKI["muted"]
    running_total = 0

    for i, (label, val) in enumerate(zip(waterfall_labels, waterfall_values_gw)):
        if label == "PCP":
            bar_colors.append(TOTAL_BAR_COLOR)
            bar_bottoms.append(0)
            running_total = val
        elif label == "Current":
            bar_colors.append(TOTAL_BAR_COLOR)
            bar_bottoms.append(0)
        else:
            if val >= 0:
                bar_colors.append(FLEXOKI["cyan"])
                bar_bottoms.append(running_total)
            else:
                bar_colors.append(FLEXOKI["red"])
                bar_bottoms.append(running_total + val)
            running_total += val

    min_visible_height = 0.15
    bar_heights = []
    for i, val in enumerate(waterfall_values_gw):
        if waterfall_labels[i] in ["PCP", "Current"]:
            bar_heights.append(abs(val))
        else:
            bar_heights.append(max(abs(val), min_visible_height) if val != 0 else 0)

    bars = ax3.bar(bar_positions, bar_heights,
                   bottom=bar_bottoms, color=bar_colors, edgecolor=FLEXOKI["background"], linewidth=1)

    for i, (bar, val) in enumerate(zip(bars, waterfall_values_gw)):
        height = bar.get_height()
        if waterfall_labels[i] in ["PCP", "Current"]:
            y_pos = bar.get_y() + height + 0.15
            label = f"{val:.1f}"
            ax3.text(i, y_pos, label, ha="center", va="bottom", color=FLEXOKI["foreground"],
                     fontsize=10, fontweight="bold")
        else:
            if abs(val) >= 0.01:
                if val >= 0:
                    y_pos = bar.get_y() + height + 0.15
                    label = f"+{val:.1f}"
                    va = "bottom"
                else:
                    y_pos = bar.get_y() - 0.15
                    label = f"{val:.1f}"
                    va = "top"
                ax3.text(i, y_pos, label, ha="center", va=va, color=FLEXOKI["foreground"],
                         fontsize=9, fontweight="bold")

    waterfall_labels_short = ["PCP", "Net Imp", "Coal", "Gas", "Hydro", "Wind", "Solar", "Battery", "Current"]
    ax3.set_xticks(bar_positions)
    ax3.set_xticklabels(waterfall_labels_short, rotation=0, ha="center", color=FLEXOKI["text"], fontsize=10)
    ax3.set_ylabel("Generation (GW)", color=FLEXOKI["text"], fontsize=12)
    ax3.set_title("Year-over-Year Change by Fuel Type", color=FLEXOKI["foreground"], fontsize=14, fontweight="bold")
    ax3.tick_params(axis="y", colors=FLEXOKI["text"], labelsize=11)

    all_values = [last_year_stats["total"] / 1000, this_year_stats["total"] / 1000]
    y_min = min(all_values) * 0.85
    y_max = max(all_values) * 1.15
    ax3.set_ylim(bottom=y_min, top=y_max)

    ax3.grid(False)
    for spine in ax3.spines.values():
        spine.set_visible(False)
    ax3.tick_params(length=0)

    region_label = region if region != "NEM" else "NEM"
    ty_end_short = (datetime.strptime(this_year_end, "%Y-%m-%d") - timedelta(days=1)).strftime("%d %b %Y")
    fig.suptitle(f"{region_label} Evening Peak (17:00-22:00) - {period_days} Days to {ty_end_short}",
                 color=FLEXOKI["foreground"], fontsize=18, fontweight="bold", y=0.98)

    handles, labels = ax1.get_legend_handles_labels()
    handles_reversed = handles[::-1]
    labels_reversed = labels[::-1]

    fig.legend(handles_reversed, labels_reversed, loc="upper center", ncol=9,
               facecolor=FLEXOKI["background"], edgecolor="none", labelcolor=FLEXOKI["text"],
               bbox_to_anchor=(0.5, 0.48), fontsize=11, framealpha=0)

    fig.text(0.98, 0.02, "Design: ITK, Data: AEMO", color=FLEXOKI["muted"], fontsize=10,
             ha="right", style="italic")

    fig.subplots_adjust(left=0.06, right=0.98, top=0.92, bottom=0.06)

    return fig


def get_latest_data_date():
    """Get the most recent settlementdate in scada30 as a date object."""
    result = _query("SELECT MAX(settlementdate) as max_date FROM scada30")
    max_date = result["max_date"].iloc[0]
    return pd.to_datetime(max_date).date() if max_date is not None else datetime.now().date()
