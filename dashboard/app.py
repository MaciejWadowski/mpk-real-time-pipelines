from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from queries import (
    get_all_routes,
    get_date_range,
    get_kpi,
    get_last_update,
    get_map_stops,
    get_ranking_lines,
    get_ranking_stops,
    get_route_shapes,
    get_time_heatmap,
    get_trend,
    get_weather_correlation,
)

st.set_page_config(
    page_title="MPK Kraków — Opóźnienia",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── helpers ───────────────────────────────────────────────────────────────────

_DOW_LABELS = {0: "Niedz", 1: "Pon", 2: "Wt", 3: "Śr", 4: "Czw", 5: "Pt", 6: "Sob"}
_DOW_ORDER = [1, 2, 3, 4, 5, 6, 0]  # Mon first

_WMO_CATEGORY = {
    range(0, 1): "Słonecznie",
    range(1, 4): "Pochmurno",
    range(45, 50): "Mgła",
    range(50, 68): "Deszcz",
    range(70, 78): "Śnieg",
    range(80, 83): "Przelotne opady",
    range(85, 87): "Śnieg przelotny",
    range(95, 100): "Burza",
}


def _categorize_wmo(code) -> str:
    if pd.isna(code):
        return "Nieznane"
    c = int(code)
    for r, label in _WMO_CATEGORY.items():
        if c in r:
            return label
    return "Inne"


def _delay_color(delay: float, max_d: float = 10.0) -> list[int]:
    ratio = min(max(float(delay), 0.0) / max_d, 1.0)
    return [int(220 * ratio), int(180 * (1 - ratio)), 0, 200]


def _add_colors(df: pd.DataFrame, col: str = "avg_delay") -> pd.DataFrame:
    df = df.copy()
    df["color"] = df[col].apply(_delay_color)
    return df


# ── sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("MPK Kraków")
    try:
        last_update = get_last_update()
        st.caption(f"Dane z: **{last_update.strftime('%d.%m.%Y %H:%M')}**")
    except Exception:
        st.caption("Brak połączenia ze Snowflake")

    st.divider()
    st.header("Filtry")

    modes = st.multiselect(
        "Tryb pojazdu",
        options=["A", "M", "T"],
        default=["A", "M", "T"],
        help="A = autobus podmiejski, M = autobus miejski, T = tramwaj",
    )

    try:
        all_routes = get_all_routes()
    except Exception:
        all_routes = []

    line_options = ["Wszystkie"] + all_routes
    selected_line_label = st.selectbox("Linia", line_options)
    line: str | None = None if selected_line_label == "Wszystkie" else selected_line_label

    try:
        _, default_end = get_date_range()
    except Exception:
        from datetime import date
        default_end = date.today()

    from datetime import timedelta
    default_start = default_end - timedelta(days=90)

    start_date = st.date_input("Od", value=default_start)
    end_date = st.date_input("Do", value=default_end)

    hour_min, hour_max = st.slider("Zakres godzin", 0, 23, (0, 23))

if not modes:
    st.warning("Wybierz co najmniej jeden tryb pojazdu w panelu bocznym.")
    st.stop()

modes_t = tuple(sorted(modes))
start_str = start_date.strftime("%Y%m%d")
end_str = end_date.strftime("%Y%m%d")

# ── KPI row ───────────────────────────────────────────────────────────────────

st.title("Dashboard opóźnień MPK Kraków")

try:
    kpi = get_kpi(modes_t, line, start_str, end_str, hour_min, hour_max)
    c1, c2, c3 = st.columns(3)
    c1.metric("% kursów na czas", f"{kpi['pct_on_time']:.1f}%")
    c2.metric(
        "Najgorsza linia",
        f"Linia {kpi['worst_line']}",
        f"śr. {kpi['worst_delay']:.1f} min opóźnienia",
        delta_color="inverse",
    )
    c3.metric("Zmierzonych przejazdów", f"{kpi['n_obs']:,}".replace(",", " "))
except Exception as e:
    st.error(f"Błąd połączenia ze Snowflake: {e}")
    st.stop()

st.divider()

# ── tabs ──────────────────────────────────────────────────────────────────────

tab_map, tab_heatmap, tab_trends, tab_ranking, tab_weather = st.tabs(
    ["Mapa", "Heatmapa czasu", "Trendy", "Ranking", "Korelacja pogody"]
)

# ══ TAB 1: MAPA ══════════════════════════════════════════════════════════════

with tab_map:
    col_ctrl, col_map = st.columns([1, 5])

    with col_ctrl:
        st.subheader("Warstwy")
        show_stops = st.checkbox("Przystanki", value=True)
        show_heat = st.checkbox("Heatmapa", value=False)
        show_routes = st.checkbox(
            "Trasy linii",
            value=False,
            help="Pierwsze załadowanie: ok. 30–60 s. Dane cachowane na 24 h.",
        )
        st.divider()
        st.caption("Kolor: zielony = punktualnie, czerwony = duże opóźnienia (≥10 min)")

    layers: list = []
    tooltip_config: dict = {}

    if show_stops or show_heat:
        with st.spinner("Ładowanie danych przystanków..."):
            stop_df = get_map_stops(modes_t, line, start_str, end_str, hour_min, hour_max)

        if stop_df.empty:
            with col_map:
                st.info("Brak danych dla wybranych filtrów.")
        else:
            stop_df = _add_colors(stop_df)
            # Radius: log-scale of observations, capped for visual balance
            stop_df["radius"] = (np.log1p(stop_df["n_obs"]) * 30).clip(40, 300)

            if show_stops:
                layers.append(
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=stop_df,
                        get_position=["lon", "lat"],
                        get_color="color",
                        get_radius="radius",
                        pickable=True,
                        auto_highlight=True,
                    )
                )
                tooltip_config = {
                    "html": (
                        "<b>{STOP_NAME}</b><br/>"
                        "Śr. opóźnienie: <b>{avg_delay} min</b><br/>"
                        "% na czas: {pct_on_time}%<br/>"
                        "Obserwacje: {n_obs}"
                    ),
                    "style": {
                        "backgroundColor": "#1a1a2e",
                        "color": "white",
                        "padding": "8px",
                        "borderRadius": "4px",
                        "fontSize": "13px",
                    },
                }

            if show_heat:
                layers.append(
                    pdk.Layer(
                        "HeatmapLayer",
                        data=stop_df,
                        get_position=["lon", "lat"],
                        get_weight="avg_delay",
                        radius_pixels=60,
                        intensity=1,
                        threshold=0.03,
                    )
                )

    if show_routes:
        with st.spinner("Ładowanie geometrii tras (może zająć ok. 30 s przy pierwszym ładowaniu)..."):
            shapes_raw = get_route_shapes(modes_t)

        if not shapes_raw.empty:
            # Build per-route avg_delay for coloring
            line_avg: dict[str, float] = {}
            if not stop_df.empty if (show_stops or show_heat) else True:
                try:
                    rank_df = get_ranking_lines(modes_t, line, start_str, end_str, hour_min, hour_max)
                    line_avg = dict(zip(rank_df["route_short_name"], rank_df["avg_delay"].astype(float)))
                except Exception:
                    pass

            # Group shape points into path arrays
            def _make_path(grp: pd.DataFrame) -> list:
                return grp.sort_values("shape_pt_sequence")[["shape_pt_lon", "shape_pt_lat"]].values.tolist()

            paths_df = (
                shapes_raw
                .groupby(["route_short_name", "mode"])
                .apply(_make_path, include_groups=False)
                .reset_index(name="path")
            )
            paths_df["avg_delay"] = paths_df["route_short_name"].map(line_avg).fillna(2.5)
            paths_df = _add_colors(paths_df)

            layers.append(
                pdk.Layer(
                    "PathLayer",
                    data=paths_df,
                    get_path="path",
                    get_color="color",
                    get_width=4,
                    width_min_pixels=1,
                    pickable=True,
                    auto_highlight=True,
                )
            )
            if not tooltip_config:
                tooltip_config = {
                    "html": "<b>Linia {route_short_name}</b> ({mode})<br/>Śr. opóźnienie: {avg_delay} min",
                    "style": {"backgroundColor": "#1a1a2e", "color": "white", "padding": "8px"},
                }

    with col_map:
        if layers:
            st.pydeck_chart(
                pdk.Deck(
                    layers=layers,
                    initial_view_state=pdk.ViewState(
                        latitude=50.062,
                        longitude=19.937,
                        zoom=11,
                        pitch=0,
                    ),
                    tooltip=tooltip_config,
                    map_style="light",
                ),
                use_container_width=True,
                height=620,
            )
        else:
            st.info("Zaznacz co najmniej jedną warstwę po lewej stronie.")


# ══ TAB 2: HEATMAPA CZASU ════════════════════════════════════════════════════

with tab_heatmap:
    st.subheader("Średnie opóźnienie wg godziny i dnia tygodnia")
    st.caption("Filtr godzin z panelu bocznego jest tutaj wyłączony — heatmapa zawsze pokazuje pełną dobę.")

    with st.spinner("Ładowanie danych..."):
        hm_df = get_time_heatmap(modes_t, line, start_str, end_str)

    if hm_df.empty:
        st.info("Brak danych dla wybranych filtrów.")
    else:
        pivot = hm_df.pivot_table(
            values="avg_delay", index="dow", columns="hour", fill_value=0
        )
        # Reorder: Snowflake DAYOFWEEK 0=Sun, 1=Mon, ..., 6=Sat
        ordered = [d for d in _DOW_ORDER if d in pivot.index]
        pivot = pivot.reindex(ordered)
        pivot.index = [_DOW_LABELS[d] for d in ordered]

        fig = px.imshow(
            pivot,
            color_continuous_scale="YlOrRd",
            labels={"x": "Godzina", "y": "Dzień tygodnia", "color": "Śr. opóźnienie [min]"},
            aspect="auto",
            text_auto=".1f",
        )
        fig.update_layout(height=380, margin=dict(t=20, b=20))
        fig.update_xaxes(side="bottom", tickmode="linear", dtick=1)
        st.plotly_chart(fig, use_container_width=True)

        # Summary below heatmap
        peak = hm_df.loc[hm_df["avg_delay"].idxmax()]
        st.caption(
            f"Szczyt opóźnień: **{_DOW_LABELS.get(int(peak['dow']), '?')}**, "
            f"godzina **{int(peak['hour']):02d}:00** → śr. **{peak['avg_delay']:.1f} min**"
        )


# ══ TAB 3: TRENDY ════════════════════════════════════════════════════════════

with tab_trends:
    st.subheader("Dzienny trend opóźnień — porównanie linii")

    with st.spinner("Ładowanie rankingu..."):
        rank_for_defaults = get_ranking_lines(modes_t, line, start_str, end_str, hour_min, hour_max)

    _all_routes_set = set(all_routes)
    default_trend_lines = (
        [r for r in rank_for_defaults["route_short_name"].head(5).tolist() if r in _all_routes_set][:3]
        if not rank_for_defaults.empty else []
    )

    selected_trend_lines = st.multiselect(
        "Linie do porównania",
        options=all_routes,
        default=default_trend_lines,
        max_selections=10,
    )

    if not selected_trend_lines:
        st.info("Wybierz co najmniej jedną linię powyżej.")
    else:
        with st.spinner("Ładowanie trendów..."):
            trend_df = get_trend(modes_t, tuple(selected_trend_lines), start_str, end_str)

        if trend_df.empty:
            st.info("Brak danych dla wybranych linii i filtrów.")
        else:
            trend_df["dt"] = pd.to_datetime(trend_df["dt"])

            # 7-day rolling average smoothing option
            smooth = st.toggle("Wygładzenie 7-dniowe", value=True)
            if smooth:
                trend_df = trend_df.sort_values(["route_short_name", "dt"])
                trend_df["avg_delay"] = (
                    trend_df.groupby("route_short_name")["avg_delay"]
                    .transform(lambda s: s.rolling(7, min_periods=1, center=True).mean())
                    .round(2)
                )

            fig = px.line(
                trend_df,
                x="dt",
                y="avg_delay",
                color="route_short_name",
                labels={
                    "dt": "Data",
                    "avg_delay": "Śr. opóźnienie [min]",
                    "route_short_name": "Linia",
                },
                markers=False,
            )
            fig.update_layout(height=460, legend_title_text="Linia", margin=dict(t=20))
            fig.update_yaxes(rangemode="tozero")
            st.plotly_chart(fig, use_container_width=True)


# ══ TAB 4: RANKING ═══════════════════════════════════════════════════════════

with tab_ranking:
    rank_by = st.radio("Rankinguj wg", ["Linie", "Przystanki"], horizontal=True)
    sort_col = st.selectbox(
        "Sortuj malejąco wg",
        ["avg_delay", "median_delay", "pct_on_time"],
        format_func=lambda x: {
            "avg_delay": "Śr. opóźnienie",
            "median_delay": "Mediana opóźnienia",
            "pct_on_time": "% na czas",
        }[x],
    )
    top_n = st.slider("Liczba pozycji na wykresie", 5, 30, 15)

    with st.spinner("Ładowanie rankingu..."):
        if rank_by == "Linie":
            rank_df = get_ranking_lines(modes_t, line, start_str, end_str, hour_min, hour_max)
            label_col = "route_short_name"
            label_name = "Linia"
        else:
            rank_df = get_ranking_stops(modes_t, line, start_str, end_str, hour_min, hour_max)
            label_col = "stop_name"
            label_name = "Przystanek"

    if rank_df.empty:
        st.info("Brak danych dla wybranych filtrów.")
    else:
        ascending = sort_col == "pct_on_time"
        rank_df_sorted = rank_df.sort_values(sort_col, ascending=ascending).reset_index(drop=True)

        # Full sortable table
        st.subheader(f"Ranking {'linii' if rank_by == 'Linie' else 'przystanków'}")
        col_cfg: dict = {
            label_col: st.column_config.TextColumn(label_name, width="medium"),
            "avg_delay": st.column_config.NumberColumn("Śr. opóźnienie [min]", format="%.2f"),
            "median_delay": st.column_config.NumberColumn("Mediana [min]", format="%.2f"),
            "pct_on_time": st.column_config.ProgressColumn(
                "% na czas", format="%.1f%%", min_value=0, max_value=100
            ),
            "n_obs": st.column_config.NumberColumn("Obserwacje", format="%d"),
        }
        if rank_by == "Linie":
            col_cfg["mode"] = st.column_config.TextColumn("Tryb", width="small")

        display_cols = ([label_col, "mode"] if rank_by == "Linie" else [label_col]) + [
            "avg_delay", "median_delay", "pct_on_time", "n_obs"
        ]
        st.dataframe(
            rank_df_sorted[display_cols],
            column_config=col_cfg,
            use_container_width=True,
            height=420,
        )

        # Bar chart: top N
        _sort_labels = {"avg_delay": "Śr. opóźnienie", "median_delay": "Mediana", "pct_on_time": "% na czas"}
        st.subheader(f"Top {top_n} wg: {_sort_labels[sort_col]}")
        chart_df = rank_df_sorted.head(top_n) if not ascending else rank_df_sorted.tail(top_n)
        y_label = {
            "avg_delay": "Śr. opóźnienie [min]",
            "median_delay": "Mediana opóźnienia [min]",
            "pct_on_time": "% kursów na czas",
        }[sort_col]

        fig = px.bar(
            chart_df,
            x=sort_col,
            y=label_col,
            orientation="h",
            color=sort_col,
            color_continuous_scale="YlOrRd" if sort_col != "pct_on_time" else "RdYlGn",
            labels={sort_col: y_label, label_col: label_name},
        )
        fig.update_layout(
            height=max(300, top_n * 28),
            yaxis={"autorange": "reversed"},
            coloraxis_showscale=False,
            margin=dict(t=10),
        )
        st.plotly_chart(fig, use_container_width=True)


# ══ TAB 5: KORELACJA POGODY ══════════════════════════════════════════════════

with tab_weather:
    st.subheader("Korelacja opóźnień z warunkami pogodowymi")
    st.caption(
        "Dane pogodowe: Open-Meteo, uśrednione dla 18 dzielnic Krakowa. "
        "Każdy punkt = średnia dla jednej godziny."
    )

    with st.spinner("Ładowanie danych pogodowych..."):
        w_df = get_weather_correlation(modes_t, line, start_str, end_str, hour_min, hour_max)

    if w_df.empty:
        st.info("Brak danych pogodowych dla wybranego zakresu dat.")
    else:
        w_df["weather_cat"] = w_df["weather_code"].apply(_categorize_wmo)

        # ── Bar chart: weather condition vs avg delay ──────────────────────
        st.markdown("#### Warunki pogodowe vs średnie opóźnienie")
        cat_avg = (
            w_df.groupby("weather_cat")
            .agg(avg_delay=("avg_delay", "mean"), n_hours=("n_obs", "sum"))
            .reset_index()
            .sort_values("avg_delay", ascending=False)
        )
        fig_bar = px.bar(
            cat_avg,
            x="weather_cat",
            y="avg_delay",
            color="weather_cat",
            color_discrete_sequence=px.colors.qualitative.Safe,
            labels={"weather_cat": "Warunek pogodowy", "avg_delay": "Śr. opóźnienie [min]"},
            text_auto=".2f",
        )
        fig_bar.update_layout(height=380, showlegend=False, margin=dict(t=10))
        fig_bar.update_traces(textposition="outside")
        st.plotly_chart(fig_bar, use_container_width=True)

        # ── Heatmap: temperature × precipitation ──────────────────────────
        st.markdown("#### Temperatura × opady — śr. opóźnienie [min]")

        w_df["temp_bin"] = pd.cut(
            w_df["temp"],
            bins=[-30, 0, 5, 10, 15, 20, 25, 40],
            labels=["<0°C", "0-5°C", "5-10°C", "10-15°C", "15-20°C", "20-25°C", ">25°C"],
        )
        w_df["precip_bin"] = pd.cut(
            w_df["precip"],
            bins=[-0.001, 0, 1, 5, 200],
            labels=["0 mm", "0–1 mm", "1–5 mm", ">5 mm"],
        )
        heat_data = (
            w_df.groupby(["temp_bin", "precip_bin"], observed=True)["avg_delay"]
            .mean()
            .unstack("precip_bin")
            .round(2)
        )
        heat_data = heat_data.reindex(
            columns=["0 mm", "0–1 mm", "1–5 mm", ">5 mm"]
        ).dropna(how="all")

        if not heat_data.empty:
            fig_heat = px.imshow(
                heat_data,
                color_continuous_scale="YlOrRd",
                labels={
                    "x": "Opady",
                    "y": "Temperatura",
                    "color": "Śr. opóźnienie [min]",
                },
                text_auto=".1f",
                aspect="auto",
            )
            fig_heat.update_layout(height=360, margin=dict(t=10))
            st.plotly_chart(fig_heat, use_container_width=True)
        else:
            st.info("Za mało danych do heatmapy temperatury × opadów.")

        # ── Wind speed scatter ─────────────────────────────────────────────
        with st.expander("Wiatr vs opóźnienie (scatter)"):
            fig_wind = px.scatter(
                w_df.sample(min(2000, len(w_df)), random_state=42),
                x="wind",
                y="avg_delay",
                opacity=0.4,
                trendline="lowess",
                labels={"wind": "Prędkość wiatru [km/h]", "avg_delay": "Śr. opóźnienie [min]"},
                color_discrete_sequence=["#3b82f6"],
            )
            fig_wind.update_layout(height=340, margin=dict(t=10))
            st.plotly_chart(fig_wind, use_container_width=True)
