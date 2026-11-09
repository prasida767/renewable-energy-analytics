"""Great Britain electricity-generation analytics dashboard backed by Databricks Gold tables.

Run from the repository root:
    PYTHONPATH=src python -m streamlit run app.py
"""

from datetime import datetime, time, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st

from energy_pipeline.databricks_reader import read_gold_table


LOGGER = logging.getLogger(__name__)
FUEL_COLUMNS = {
    "WIND": "wind_mw",
    "CCGT": "gas_ccgt_mw",
    "NUCLEAR": "nuclear_mw",
}
REQUIRED_DAILY = {
    "settlement_date", "fuel_type", "period_count", "average_mw",
    "peak_mw", "total_energy_mwh",
}
REQUIRED_TRENDS = {"settlement_date", "settlement_period", *FUEL_COLUMNS.values()}

st.set_page_config(
    page_title="GB Electricity | Generation Analytics",
    page_icon="⚡",
    layout="wide",
)


@st.cache_data(ttl=600, show_spinner="Loading Databricks Gold data…")
def load_gold_data():
    """Load small, pre-aggregated reporting tables (10-minute cache)."""
    return (
        read_gold_table("fuelhh_gold_daily_fuel"),
        read_gold_table("fuelhh_gold_period_trends"),
    )


def prepare_data(daily: pd.DataFrame, trends: pd.DataFrame):
    """Normalise types after SQL retrieval; keep calculations in Gold."""
    missing_daily = REQUIRED_DAILY - set(daily.columns)
    missing_trends = REQUIRED_TRENDS - set(trends.columns)
    if missing_daily or missing_trends:
        raise ValueError(
            f"Unexpected Gold schemas. Missing daily: {sorted(missing_daily)}; "
            f"missing trends: {sorted(missing_trends)}"
        )

    daily = daily.copy()
    trends = trends.copy()
    for frame in (daily, trends):
        frame["settlement_date"] = pd.to_datetime(
            frame["settlement_date"], errors="raise"
        ).dt.date
    for name in ("period_count", "average_mw", "peak_mw", "total_energy_mwh"):
        daily[name] = pd.to_numeric(daily[name], errors="coerce")
    daily["fuel_type"] = daily["fuel_type"].astype("string")
    trends["settlement_period"] = pd.to_numeric(
        trends["settlement_period"], errors="coerce"
    )
    for name in FUEL_COLUMNS.values():
        trends[name] = pd.to_numeric(trends[name], errors="coerce")
    return daily, trends


def expected_period_count(settlement_date):
    """GB settlement day length: typically 48, or 46/50 around clock changes."""
    london = ZoneInfo("Europe/London")
    start = datetime.combine(settlement_date, time.min, tzinfo=london)
    end = datetime.combine(settlement_date + timedelta(days=1), time.min, tzinfo=london)
    elapsed = end.astimezone(timezone.utc) - start.astimezone(timezone.utc)
    return round(elapsed.total_seconds() / 1800)


def fmt_mwh(value):
    if pd.isna(value):
        return "N/A"
    return f"{value:,.0f} MWh"


st.title("⚡ Great Britain Electricity Generation Analytics")
st.caption(
    "A portfolio analytics app using Elexon FUELHH → Python ingestion → "
    "Databricks Bronze/Silver/Gold → SQL → Streamlit"
)

try:
    daily_df, trends_df = prepare_data(*load_gold_data())
except Exception:
    LOGGER.exception("Unable to load or prepare Gold data")
    st.error("Could not read the Databricks Gold tables. Check your .env, "
             "SQL warehouse, authentication, and Gold table schemas.")
    st.stop()

if daily_df.empty or trends_df.empty:
    st.warning("No Gold reporting data is available yet.")
    st.stop()

available_dates = sorted(
    set(daily_df["settlement_date"]) & set(trends_df["settlement_date"])
)
if not available_dates:
    st.warning("The two Gold tables do not contain any common settlement dates.")
    st.stop()

with st.sidebar:
    st.header("Explore the data")
    selected_date = st.selectbox(
        "Settlement date", available_dates, index=len(available_dates) - 1,
        format_func=lambda d: d.strftime("%d %b %Y"),
    )
    previous_dates = [d for d in available_dates if d < selected_date]
    previous_date = previous_dates[-1] if previous_dates else None
    st.caption(f"{len(available_dates)} available settlement date(s)")
    st.divider()
    selected_fuels = st.multiselect(
        "Compare half-hourly power",
        options=list(FUEL_COLUMNS),
        default=list(FUEL_COLUMNS),
    )
    st.divider()
    if st.button("Refresh Databricks results", width="stretch"):
        load_gold_data.clear()
        st.rerun()

current_daily = daily_df[daily_df["settlement_date"] == selected_date].copy()
current_trends = trends_df[trends_df["settlement_date"] == selected_date].copy()
previous_daily = (
    daily_df[daily_df["settlement_date"] == previous_date].copy()
    if previous_date is not None else None
)


def fuel_total(frame, fuel):
    rows = frame.loc[frame["fuel_type"] == fuel, "total_energy_mwh"]
    if rows.empty or rows.isna().all():
        return None
    return float(rows.sum())


wind_current = fuel_total(current_daily, "WIND")
wind_previous = fuel_total(previous_daily, "WIND") if previous_daily is not None else None
wind_delta = None
if wind_current is not None and wind_previous not in (None, 0):
    wind_delta = f"{(wind_current / wind_previous - 1) * 100:+.1f}%"

expected_periods = expected_period_count(selected_date)
observed_periods = int(current_trends["settlement_period"].nunique())
fuel_count = int(current_daily["fuel_type"].nunique())

st.subheader(f"Snapshot · {selected_date:%d %B %Y}")
if previous_date:
    st.caption(f"Comparison baseline: previous available date, {previous_date:%d %B %Y}")
else:
    st.caption("No earlier date is available for comparisons yet.")

k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Reported wind energy", fmt_mwh(wind_current),
    delta=wind_delta, delta_color="normal",
    help="Sum of reported WIND half-hourly average MW × 0.5 hours.",
)
k2.metric("Fuel categories", f"{fuel_count}")
k3.metric("Settlement periods", f"{observed_periods} / {expected_periods}")
k4.metric("Settlement dates loaded", f"{len(available_dates)}")

if observed_periods != expected_periods:
    st.warning(
        f"This day has {observed_periods} periods in the trend table; "
        f"the GB settlement calendar implies {expected_periods}. "
        "Review source completeness before drawing conclusions."
    )

st.divider()
left, right = st.columns([1.2, 1], gap="large")
with left:
    st.subheader("Wind energy across available dates")
    wind_history = daily_df[daily_df["fuel_type"] == "WIND"].sort_values("settlement_date")
    if not wind_history.empty:
        wind_fig = px.line(
            wind_history, x="settlement_date", y="total_energy_mwh",
            markers=True,
            labels={"settlement_date": "Settlement date", "total_energy_mwh": "Wind energy (MWh)"},
        )
        wind_fig.update_traces(line_width=3, marker_size=9)
        wind_fig.update_layout(margin=dict(l=10, r=10, t=15, b=5), height=350)
        st.plotly_chart(wind_fig, width="stretch")
        if len(wind_history) < 7:
            st.caption("Only a few dates are loaded; longer-term trends will become clearer after backfilling more data.")
    else:
        st.info("No WIND measurements are present in the loaded Gold data.")

with right:
    st.subheader("Reported energy by fuel category")
    breakdown = current_daily.sort_values("total_energy_mwh", ascending=True)
    bar_fig = px.bar(
        breakdown, x="total_energy_mwh", y="fuel_type", orientation="h",
        labels={"total_energy_mwh": "Energy equivalent (MWh)", "fuel_type": "Fuel"},
    )
    bar_fig.update_layout(margin=dict(l=10, r=10, t=15, b=5), height=520)
    st.plotly_chart(bar_fig, width="stretch")

st.subheader("Half-hourly power by settlement period")
if selected_fuels:
    trend_cols = [FUEL_COLUMNS[fuel] for fuel in selected_fuels]
    long_trends = current_trends.melt(
        id_vars=["settlement_period"], value_vars=trend_cols,
        var_name="source", value_name="average_mw",
    )
    legend_names = {column: fuel for fuel, column in FUEL_COLUMNS.items()}
    long_trends["fuel_type"] = long_trends["source"].map(legend_names)
    fig = px.line(
        long_trends.sort_values("settlement_period"),
        x="settlement_period", y="average_mw", color="fuel_type",
        labels={"settlement_period": "Settlement period", "average_mw": "Average power (MW)",
                "fuel_type": "Fuel category"},
    )
    fig.update_layout(margin=dict(l=10, r=10, t=15, b=5), height=370)
    st.plotly_chart(fig, width="stretch")
else:
    st.info("Select at least one fuel category in the sidebar to see period trends.")

st.subheader("Compare reported fuel energy between dates")
if previous_daily is not None:
    current = current_daily[["fuel_type", "total_energy_mwh"]].rename(
        columns={"total_energy_mwh": "selected_mwh"}
    )
    prev = previous_daily[["fuel_type", "total_energy_mwh"]].rename(
        columns={"total_energy_mwh": "previous_mwh"}
    )
    comparison = current.merge(prev, on="fuel_type", how="outer", validate="one_to_one")
    comparison["change_mwh"] = comparison["selected_mwh"] - comparison["previous_mwh"]
    comparison["change_pct"] = (
        comparison["change_mwh"] / comparison["previous_mwh"].replace(0, float("nan")) * 100
    )
    comparison = comparison.sort_values("selected_mwh", ascending=False)
    st.dataframe(
        comparison,
        width="stretch", hide_index=True,
        column_config={
            "fuel_type": "Fuel",
            "selected_mwh": st.column_config.NumberColumn(f"{selected_date:%d %b} (MWh)", format="%.0f"),
            "previous_mwh": st.column_config.NumberColumn(f"{previous_date:%d %b} (MWh)", format="%.0f"),
            "change_mwh": st.column_config.NumberColumn("Change (MWh)", format="%.0f"),
            "change_pct": st.column_config.NumberColumn("Change (%)", format="%.1f"),
        },
    )
else:
    st.info("Load an earlier settlement date to enable this comparison.")

with st.expander("Data quality, methodology & limitations"):
    st.write(
        "Gold is derived from validated Silver records. Each FUELHH generation value "
        "is an average MW figure for a 30-minute period; the daily energy equivalent "
        "is calculated as MW × 0.5 hours, then summed across periods."
    )
    st.write(
        "Elexon FUELHH covers transmission-metered generation and includes "
        "interconnector import categories and pumped storage. Embedded generation, "
        "including some wind and solar, is not fully represented. Consequently, these "
        "charts are not complete UK generation totals or asset-level performance metrics."
    )
    st.write(
        "Period counts are checked against the GB settlement calendar (usually 48, "
        "with 46 or 50 on clock-change days). The current pipeline uses full table "
        "refreshes and does not retain immutable source revisions."
    )

st.download_button(
    "Download selected day's fuel metrics (CSV)",
    data=current_daily.to_csv(index=False).encode("utf-8"),
    file_name=f"fuel-metrics-{selected_date.isoformat()}.csv",
    mime="text/csv",
)
st.caption("Data source: Elexon BMRS FUELHH. Dashboard reads Databricks Gold tables; it does not re-run the ETL pipeline.")
