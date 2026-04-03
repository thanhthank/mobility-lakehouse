import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text

# =========================
# Cấu hình trang
# =========================
st.set_page_config(
    page_title="Bảng dữ liệu Lakehouse dữ liệu di chuyển",
    page_icon="📍",
    layout="wide"
)

# =========================
# CSS tùy biến giao diện
# =========================
st.markdown("""
<style>
    .main {
        background: #f8fafc;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }

    .hero {
        background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 100%);
        padding: 30px 32px;
        border-radius: 24px;
        color: white;
        margin-bottom: 18px;
        box-shadow: 0 10px 25px rgba(29, 78, 216, 0.22);
    }

    .hero h1 {
        margin: 0;
        font-size: 2.3rem;
        font-weight: 800;
        letter-spacing: -0.5px;
    }

    .hero p {
        margin-top: 8px;
        font-size: 1rem;
        opacity: 0.92;
    }

    .kpi-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 18px;
        padding: 18px 20px;
        box-shadow: 0 4px 14px rgba(15, 23, 42, 0.05);
        min-height: 110px;
    }

    .kpi-title {
        color: #6b7280;
        font-size: 0.95rem;
        margin-bottom: 8px;
    }

    .kpi-value {
        color: #111827;
        font-size: 1.9rem;
        font-weight: 800;
        line-height: 1.1;
    }

    .section-title {
        font-size: 1.25rem;
        font-weight: 800;
        color: #111827;
        margin-top: 4px;
        margin-bottom: 8px;
    }

    .subtle-note {
        color: #6b7280;
        font-size: 0.95rem;
        margin-bottom: 12px;
    }

    div[data-testid="stMetric"] {
        background: white;
        border: 1px solid #e5e7eb;
        padding: 16px;
        border-radius: 18px;
        box-shadow: 0 4px 14px rgba(15, 23, 42, 0.05);
    }

    div[data-testid="stSidebar"] {
        background: #ffffff;
        border-right: 1px solid #e5e7eb;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        border-radius: 14px;
        padding-left: 18px;
        padding-right: 18px;
        background-color: #eef2ff;
        color: #1e3a8a;
        font-weight: 700;
    }

    .stTabs [aria-selected="true"] {
        background-color: #1d4ed8 !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)

# =========================
# Kết nối cơ sở dữ liệu
# Nếu bạn đang dùng 5432 thì đổi DB_PORT = 5432
# =========================
DB_HOST = "127.0.0.1"
DB_PORT = 55432
DB_NAME = "mobility"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True
)

# =========================
# Hàm tải dữ liệu
# =========================
@st.cache_data(ttl=15)
def tai_du_lieu():
    truy_van = text("""
        SELECT
            id,
            vehicle_id,
            event_time,
            latitude,
            longitude,
            speed,
            COALESCE(NULLIF(district_name, ''), 'Chưa gán') AS district_name
        FROM gps_points
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY event_time DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(truy_van, conn)

@st.cache_data(ttl=15)
def tai_du_lieu_khong_gian(lat, lon, ban_kinh):
    truy_van = text("""
        SELECT
            vehicle_id,
            event_time,
            latitude,
            longitude,
            speed
        FROM gps_points
        WHERE geom IS NOT NULL
          AND ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                :ban_kinh
          )
        ORDER BY event_time DESC
        LIMIT 200
    """)
    with engine.connect() as conn:
        return pd.read_sql(
            truy_van,
            conn,
            params={"lat": lat, "lon": lon, "ban_kinh": ban_kinh}
        )

# =========================
# Header
# =========================
st.markdown("""
<div class="hero">
    <h1>📍 Bảng dữ liệu Lakehouse dữ liệu di chuyển</h1>
    <p>
        Theo dõi dữ liệu GPS, phân tích tốc độ, trực quan bản đồ và truy vấn không gian
        từ hệ thống Kafka → Spark → Delta Lake → PostGIS.
    </p>
</div>
""", unsafe_allow_html=True)

# =========================
# Nút làm mới
# =========================
cot_trai, cot_phai = st.columns([1, 5])
with cot_trai:
    if st.button("🔄 Làm mới dữ liệu"):
        st.cache_data.clear()
        st.rerun()

df = tai_du_lieu()

if df.empty:
    st.warning("Hiện chưa có dữ liệu trong bảng gps_points.")
    st.stop()

df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

# =========================
# Sidebar bộ lọc
# =========================
st.sidebar.title("⚙️ Bộ lọc dashboard")

ds_xe = ["Tất cả"] + sorted(df["vehicle_id"].dropna().unique().tolist())
xe_duoc_chon = st.sidebar.selectbox("Chọn mã xe", ds_xe)

toc_do_min = float(df["speed"].min()) if df["speed"].notna().any() else 0.0
toc_do_max = float(df["speed"].max()) if df["speed"].notna().any() else 100.0

khoang_toc_do = st.sidebar.slider(
    "Khoảng tốc độ",
    min_value=float(toc_do_min),
    max_value=float(toc_do_max),
    value=(float(toc_do_min), float(toc_do_max))
)

ngay_min = df["event_time"].min().date()
ngay_max = df["event_time"].max().date()

khoang_ngay = st.sidebar.date_input(
    "Khoảng ngày",
    value=(ngay_min, ngay_max),
    min_value=ngay_min,
    max_value=ngay_max
)

if isinstance(khoang_ngay, tuple) and len(khoang_ngay) == 2:
    tu_ngay, den_ngay = khoang_ngay
else:
    tu_ngay, den_ngay = ngay_min, ngay_max

gioi_han_bang = st.sidebar.slider("Số dòng hiển thị trong bảng", 20, 200, 100, step=10)

# =========================
# Lọc dữ liệu
# =========================
du_lieu_loc = df.copy()

if xe_duoc_chon != "Tất cả":
    du_lieu_loc = du_lieu_loc[du_lieu_loc["vehicle_id"] == xe_duoc_chon]

du_lieu_loc = du_lieu_loc[
    (du_lieu_loc["speed"] >= khoang_toc_do[0]) &
    (du_lieu_loc["speed"] <= khoang_toc_do[1]) &
    (du_lieu_loc["event_time"].dt.date >= tu_ngay) &
    (du_lieu_loc["event_time"].dt.date <= den_ngay)
]

# =========================
# KPI
# =========================
tong_ban_ghi = len(du_lieu_loc)
so_xe = du_lieu_loc["vehicle_id"].nunique()
toc_do_tb = du_lieu_loc["speed"].mean() if not du_lieu_loc.empty else 0
ban_ghi_moi_nhat = du_lieu_loc["event_time"].max() if not du_lieu_loc.empty else None

st.markdown('<div class="section-title">📊 Tổng quan nhanh</div>', unsafe_allow_html=True)
st.markdown('<div class="subtle-note">Các chỉ số được tính theo bộ lọc hiện tại.</div>', unsafe_allow_html=True)

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Tổng số bản ghi</div>
        <div class="kpi-value">{tong_ban_ghi}</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Số đơn vị vận hành</div>
        <div class="kpi-value">{so_xe}</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Tốc độ trung bình</div>
        <div class="kpi-value">{toc_do_tb:.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    gia_tri = str(ban_ghi_moi_nhat) if ban_ghi_moi_nhat is not None else "N/A"
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Bản ghi mới nhất</div>
        <div class="kpi-value" style="font-size:1rem;">{gia_tri}</div>
    </div>
    """, unsafe_allow_html=True)

# =========================
# Tabs chính
# =========================
tab1, tab2, tab3, tab4 = st.tabs([
    "📄 Dữ liệu mới nhất",
    "📈 Phân tích thống kê",
    "🗺️ Bản đồ GPS",
    "📍 Truy vấn không gian"
])

# =========================
# TAB 1: Bảng dữ liệu
# =========================
with tab1:
    st.markdown('<div class="section-title">Dữ liệu GPS mới nhất</div>', unsafe_allow_html=True)
    st.dataframe(
        du_lieu_loc[["vehicle_id", "event_time", "latitude", "longitude", "speed"]].head(gioi_han_bang),
        use_container_width=True
    )

    csv_data = du_lieu_loc.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Tải dữ liệu đã lọc (CSV)",
        data=csv_data,
        file_name="du_lieu_gps_da_loc.csv",
        mime="text/csv"
    )

# =========================
# TAB 2: Biểu đồ
# =========================
with tab2:
    st.markdown('<div class="section-title">Phân tích thống kê</div>', unsafe_allow_html=True)

    b1, b2 = st.columns(2)

    with b1:
        fig_toc_do = px.histogram(
            du_lieu_loc,
            x="speed",
            nbins=30,
            title="Phân bố tốc độ"
        )
        fig_toc_do.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            title_x=0.12
        )
        st.plotly_chart(fig_toc_do, use_container_width=True)

    with b2:
        theo_gio = du_lieu_loc.copy()
        theo_gio["giờ"] = theo_gio["event_time"].dt.hour
        thong_ke_gio = theo_gio.groupby("giờ").size().reset_index(name="số_bản_ghi")

        fig_gio = px.bar(
            thong_ke_gio,
            x="giờ",
            y="số_bản_ghi",
            title="Số bản ghi theo giờ"
        )
        fig_gio.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            title_x=0.12
        )
        st.plotly_chart(fig_gio, use_container_width=True)

    c1, c2 = st.columns(2)

    with c1:
        top_xe = (
            du_lieu_loc.groupby("vehicle_id")
            .size()
            .reset_index(name="số_bản_ghi")
            .sort_values("số_bản_ghi", ascending=False)
            .head(10)
        )

        fig_top = px.bar(
            top_xe,
            x="vehicle_id",
            y="số_bản_ghi",
            title="Top 10 phương tiện có nhiều bản ghi nhất"
        )
        fig_top.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            title_x=0.12
        )
        st.plotly_chart(fig_top, use_container_width=True)

    with c2:
        if xe_duoc_chon != "Tất cả":
            du_lieu_xe = du_lieu_loc.sort_values("event_time")
            fig_line = px.line(
                du_lieu_xe,
                x="event_time",
                y="speed",
                title=f"Diễn biến tốc độ của xe {xe_duoc_chon}"
            )
            fig_line.update_layout(
                plot_bgcolor="white",
                paper_bgcolor="white",
                title_x=0.12
            )
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("Chọn một mã xe ở thanh bên trái để xem biểu đồ tốc độ theo thời gian.")

# =========================
# TAB 3: Bản đồ
# =========================
with tab3:
    st.markdown('<div class="section-title">Bản đồ GPS</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtle-note">Màu sắc thể hiện tốc độ, có thể lọc theo xe và thời gian ở thanh bên trái.</div>', unsafe_allow_html=True)

    ban_do = du_lieu_loc.dropna(subset=["latitude", "longitude"]).copy()

    if not ban_do.empty:
        fig_map = px.scatter_mapbox(
            ban_do.head(2000),
            lat="latitude",
            lon="longitude",
            color="speed",
            size="speed",
            hover_name="vehicle_id",
            hover_data={
                "event_time": True,
                "latitude": ':.5f',
                "longitude": ':.5f',
                "speed": ':.2f'
            },
            title="Phân bố điểm GPS trên bản đồ",
            zoom=9,
            height=600
        )
        fig_map.update_layout(
            mapbox_style="open-street-map",
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            title_x=0.12
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("Không có dữ liệu bản đồ theo bộ lọc hiện tại.")

# =========================
# TAB 4: Truy vấn không gian
# =========================
with tab4:
    st.markdown('<div class="section-title">Truy vấn không gian theo bán kính</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtle-note">Nhập tọa độ tâm và bán kính để tìm các bản ghi gần vị trí đó.</div>', unsafe_allow_html=True)

    with st.form("form_khong_gian"):
        lat = st.number_input("Vĩ độ", value=21.0, format="%.6f")
        lon = st.number_input("Kinh độ", value=106.6, format="%.6f")
        ban_kinh = st.number_input("Bán kính (mét)", value=1000, step=100)
        nut_tim = st.form_submit_button("Tìm kiếm")

    if nut_tim:
        ket_qua = tai_du_lieu_khong_gian(lat, lon, ban_kinh)
        st.success(f"Tìm thấy {len(ket_qua)} bản ghi trong vùng tìm kiếm.")

        st.dataframe(ket_qua, use_container_width=True)

        if not ket_qua.empty:
            fig_khong_gian = px.scatter_mapbox(
                ket_qua,
                lat="latitude",
                lon="longitude",
                color="speed",
                hover_name="vehicle_id",
                hover_data={"event_time": True, "speed": ':.2f'},
                title="Kết quả truy vấn không gian",
                zoom=10,
                height=500
            )
            fig_khong_gian.update_layout(
                mapbox_style="open-street-map",
                margin={"r": 0, "t": 50, "l": 0, "b": 0},
                title_x=0.12
            )
            st.plotly_chart(fig_khong_gian, use_container_width=True)