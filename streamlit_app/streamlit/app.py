import streamlit as st
from src.db.queries import get_stops_coordinates
from src.db.snowflake_conn import SnowflakeDB
from src.plots.map_plotter import MapPlotter
from streamlit_autorefresh import st_autorefresh

# init DB
db = SnowflakeDB(
    user=st.secrets["user"],
    password=st.secrets["password"],
    account=st.secrets["account"],
    database=st.secrets["database"],
    schema=st.secrets["schema"],
    warehouse=st.secrets["warehouse"],
)

# init map and live bus
map_plotter = MapPlotter()

st.set_page_config(layout="wide")
st.title("Bus Delay Analytics + AI Assistant")
tab1, tab2, tab3, tab4 = st.tabs(
    ["Stops Analysis", "System Overview", "Route Analysis", "Realtime"]
)

with tab1:
    # ========== DASHBOARD ==========
    st.header("Stops Analysis")

    df_stops = get_stops_coordinates()

    df_stops.columns = df_stops.columns.str.upper()

    r = map_plotter.plot_map(df=df_stops, tooltip={"html": "Stop name: {STOP_NAME}"})

    st.pydeck_chart(r, use_container_width=True, width="stretch")
