import pandas as pd
import streamlit as st

from communication.spark_service import (
    get_district_energy_state_df,
    get_measurements_df,
    get_sliding_window_balance_df,
    get_usage_records_df,
)

st.set_page_config(page_title="Monitoring", layout="wide")
st.title("Monitoring")

tab_measurements, tab_billing, tab_analytics = st.tabs(
    ["Measurements", "Billing / Usage records", "Analytics"]
)

with tab_measurements:
    st.subheader("MeasurementReported events")

    max_m = st.number_input(
        "Max messages to load", min_value=1, max_value=2000, value=500, key="max_m"
    )
    if st.button("Refresh measurements", use_container_width=True):
        with st.spinner("Reading from Kafka via Spark..."):
            st.session_state["measurements_df"] = get_measurements_df(int(max_m))

    df_m = st.session_state.get("measurements_df")
    if df_m is not None and not df_m.empty:
        st.dataframe(df_m, use_container_width=True, height=500)
        st.caption(f"{len(df_m)} event(s) loaded.")
    else:
        st.info("No measurement events found. Click Refresh to load.")

with tab_billing:
    st.subheader("UsageRecordCreated events")

    max_u = st.number_input(
        "Max messages to load", min_value=1, max_value=2000, value=500, key="max_u"
    )
    if st.button("Refresh usage records", use_container_width=True):
        with st.spinner("Reading from Kafka via Spark..."):
            st.session_state["usage_df"] = get_usage_records_df(int(max_u))

    df_u = st.session_state.get("usage_df")
    if df_u is not None and not df_u.empty:
        st.dataframe(df_u, use_container_width=True, height=500)
        st.caption(f"{len(df_u)} record(s) loaded.")
    else:
        st.info("No usage records found. Click Refresh to load.")

with tab_analytics:
    st.subheader("Streaming Analytics")

    st.markdown("### Per-district energy state")

    max_ana = st.number_input(
        "Max measurement events to process",
        min_value=100,
        max_value=10_000,
        value=2000,
        step=100,
        key="max_ana",
    )

    if st.button("Compute district energy state", use_container_width=True):
        with st.spinner("Running Spark aggregation on measurement-events..."):
            st.session_state["district_energy_df"] = get_district_energy_state_df(
                int(max_ana)
            )

    df_energy: pd.DataFrame | None = st.session_state.get("district_energy_df")

    if df_energy is not None and not df_energy.empty:
        st.dataframe(df_energy, use_container_width=True)

        chart_cols = ["total_produced", "total_consumed", "state_of_charge"]
        chart_df = df_energy.set_index("districtId")[chart_cols]

        st.markdown("#### Energy breakdown per district")
        st.bar_chart(chart_df, color=["#4CAF50", "#F44336", "#2196F3"])

        net_df = df_energy.set_index("districtId")[["net_balance"]]
        st.markdown("#### Net energy balance per district (produced - consumed)")
        st.bar_chart(net_df, color=["#FF9800"])
    else:
        st.info("No data yet — click Compute district energy state above to load.")

    st.divider()

    st.markdown("### Sliding-window average district energy balance")

    window_minutes = st.slider(
        "Window size (minutes)",
        min_value=4,
        max_value=60,
        value=10,
        step=1,
        key="window_minutes",
    )

    if st.button("Compute sliding-window balance", use_container_width=True):
        with st.spinner(f"Running Spark sliding-window aggregation ({window_minutes}-minute window) on usage-records..."):
            st.session_state["sliding_df"] = get_sliding_window_balance_df(
                window_minutes=int(window_minutes)
            )

    df_sliding: pd.DataFrame | None = st.session_state.get("sliding_df")

    if df_sliding is not None and not df_sliding.empty:
        st.dataframe(df_sliding, use_container_width=True)

        pivoted = df_sliding.pivot_table(
            index="window_start",
            columns="districtId",
            values="avg_net_balance",
            aggfunc="mean",
        )
        pivoted.index = pd.to_datetime(pivoted.index)
        pivoted = pivoted.sort_index()

        st.markdown(f"#### Average net balance over {window_minutes}-minute sliding windows")
        st.line_chart(pivoted)
    else:
        st.info("No data yet — click Compute sliding-window balance above to load.")
