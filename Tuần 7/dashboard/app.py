import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
import psycopg2


# =========================
# 1. Dashboard Configuration
# =========================
st.set_page_config(
    page_title="Weather Realtime Dashboard",
    page_icon="🌦️",
    layout="wide"
)

st.title("🌦️ Weather Realtime Dashboard")
st.caption("Dashboard hiển thị dữ liệu thời tiết đã xử lý từ PostgreSQL")


# =========================
# 2. PostgreSQL Connection
# =========================
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "weather_db")
DB_USER = os.getenv("POSTGRES_USER", "weather_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_password")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


@st.cache_data(ttl=10)
def load_data(limit=500):
    query = f"""
        SELECT
            id,
            window_start,
            window_end,
            avg_temperature,
            avg_feels_like,
            avg_humidity,
            avg_precipitation,
            avg_wind_speed,
            avg_pressure,
            created_at
        FROM weather_stats
        ORDER BY window_start DESC
        LIMIT {limit};
    """

    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    if not df.empty:
        df = df.sort_values("window_start")

    return df


# =========================
# 3. Sidebar Controls
# =========================
st.sidebar.header("⚙️ Cấu hình hiển thị")

refresh_seconds = st.sidebar.selectbox(
    "Tự động refresh sau:",
    [5, 10, 30, 60],
    index=1
)

limit_rows = st.sidebar.selectbox(
    "Số bản ghi gần nhất:",
    [50, 100, 300, 500, 1000],
    index=3
)

auto_refresh = st.sidebar.checkbox("Bật tự động refresh", value=True)


# =========================
# 4. Load Data
# =========================
df = load_data(limit_rows)

if df.empty:
    st.warning("Chưa có dữ liệu trong bảng weather_stats. Hãy kiểm tra Spark Consumer và PostgreSQL.")
    st.stop()


# =========================
# 5. Latest Metrics
# =========================
latest = df.iloc[-1]

col1, col2, col3 = st.columns(3)
col4, col5, col6 = st.columns(3)

col1.metric("🌡️ Nhiệt độ TB", f"{latest['avg_temperature']:.2f} °C")
col2.metric("🔥 Nhiệt độ cảm nhận", f"{latest['avg_feels_like']:.2f} °C")
col3.metric("💧 Độ ẩm TB", f"{latest['avg_humidity']:.2f} %")
col4.metric("🌧️ Lượng mưa TB", f"{latest['avg_precipitation']:.2f} mm")
col5.metric("💨 Tốc độ gió TB", f"{latest['avg_wind_speed']:.2f} km/h")
col6.metric("🧭 Áp suất TB", f"{latest['avg_pressure']:.2f} hPa")

st.divider()


# =========================
# 6. Time-series Charts
# =========================
st.subheader("📈 Biểu đồ các yếu tố thời tiết theo thời gian")

fig_temp = px.line(
    df,
    x="window_start",
    y=["avg_temperature", "avg_feels_like"],
    markers=True,
    title="Nhiệt độ và nhiệt độ cảm nhận theo thời gian"
)
st.plotly_chart(fig_temp, use_container_width=True)

fig_humidity = px.line(
    df,
    x="window_start",
    y="avg_humidity",
    markers=True,
    title="Độ ẩm trung bình theo thời gian"
)
st.plotly_chart(fig_humidity, use_container_width=True)

fig_rain = px.bar(
    df,
    x="window_start",
    y="avg_precipitation",
    title="Lượng mưa trung bình theo thời gian"
)
st.plotly_chart(fig_rain, use_container_width=True)

fig_wind = px.line(
    df,
    x="window_start",
    y="avg_wind_speed",
    markers=True,
    title="Tốc độ gió trung bình theo thời gian"
)
st.plotly_chart(fig_wind, use_container_width=True)

fig_pressure = px.line(
    df,
    x="window_start",
    y="avg_pressure",
    markers=True,
    title="Áp suất khí quyển trung bình theo thời gian"
)
st.plotly_chart(fig_pressure, use_container_width=True)


# =========================
# 7. Raw Table
# =========================
st.subheader("🗃️ Dữ liệu đã xử lý trong PostgreSQL")

st.dataframe(
    df.sort_values("window_start", ascending=False),
    use_container_width=True
)


# =========================
# 8. Auto Refresh
# =========================
if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()