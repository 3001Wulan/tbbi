import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, exc
import plotly.express as px
import time

# ============================================================================
# BAGIAN 1: FUNGSI UNTUK PROSES ETL DAN PEMUATAN DATABASE
# ============================================================================

def run_etl_process():
    """
    Fungsi ini menjalankan seluruh proses ETL dari file CSV.
    Membaca file, melakukan transformasi, dan menghasilkan semua DataFrame
    yang siap untuk dimuat ke database.
    """
    try:
        # Load dataset dari file CSV
        df = pd.read_csv('Top_10000_Movies_IMDb_with_Time.csv')
    except FileNotFoundError:
        st.error("File 'Top_10000_Movies_IMDb_with_Time.csv' tidak ditemukan. Pastikan file tersebut ada di folder yang sama dengan skrip ini.")
        return None

    # --- 1. DIM MOVIE ---
    dim_movie = df[['ID', 'Movie Name', 'Runtime', 'Plot', 'Link']].copy().drop_duplicates()
    dim_movie.columns = ['Movie_ID', 'Movie_Name', 'Runtime', 'Plot', 'Link']

    # --- 2. DIM DIRECTOR ---
    director_exploded = df[['ID', 'Directors']].copy().dropna()
    director_exploded['Directors'] = director_exploded['Directors'].str.split(', ')
    director_exploded = director_exploded.explode('Directors')
    director_exploded['Directors'] = director_exploded['Directors'].str.replace(r'[^a-zA-Z\s\.\-]', '', regex=True).str.strip()
    dim_director = pd.DataFrame(director_exploded['Directors'].unique(), columns=['Director_Name'])
    dim_director['Director_ID'] = dim_director.index + 1

    # --- 3. BRIDGE MOVIE - DIRECTOR ---
    bridge_director = director_exploded.merge(dim_director, left_on='Directors', right_on='Director_Name')
    bridge_director = bridge_director[['ID', 'Director_ID']]
    bridge_director.columns = ['Movie_ID', 'Director_ID']

    # --- 4. DIM STAR ---
    star_exploded = df[['ID', 'Stars']].copy().dropna()
    star_exploded['Stars'] = star_exploded['Stars'].str.split(', ')
    star_exploded = star_exploded.explode('Stars')
    star_exploded['Stars'] = star_exploded['Stars'].str.replace(r'[^a-zA-Z\s\.\-]', '', regex=True).str.strip()
    dim_star = pd.DataFrame(star_exploded['Stars'].unique(), columns=['Star_Name'])
    dim_star['Star_ID'] = dim_star.index + 1

    # --- 5. BRIDGE MOVIE - STAR ---
    bridge_star = star_exploded.merge(dim_star, left_on='Stars', right_on='Star_Name')
    bridge_star = bridge_star[['ID', 'Star_ID']]
    bridge_star.columns = ['Movie_ID', 'Star_ID']

    # --- 6. DIM GENRE ---
    genre_exploded = df[['ID', 'Genre']].copy().dropna()
    genre_exploded['Genre'] = genre_exploded['Genre'].str.split(', ')
    genre_exploded = genre_exploded.explode('Genre')
    dim_genre = pd.DataFrame(genre_exploded['Genre'].unique(), columns=['Genre_Name'])
    dim_genre['Genre_ID'] = dim_genre.index + 1

    # --- 7. BRIDGE MOVIE - GENRE ---
    bridge_genre = genre_exploded.merge(dim_genre, left_on='Genre', right_on='Genre_Name')
    bridge_genre = bridge_genre[['ID', 'Genre_ID']]
    bridge_genre.columns = ['Movie_ID', 'Genre_ID']

    # --- 8. DIM TIME ---
    dim_time = pd.DataFrame(df['Time'].dropna().unique(), columns=['Year'])
    dim_time['Time_ID'] = dim_time.index + 1

    # --- 9. FACT MOVIE ---
    metascore_mode = df['Metascore'].mode().iloc[0]
    df['Metascore'] = df['Metascore'].fillna(metascore_mode)
    fact_movie = df[['ID', 'Rating', 'Metascore', 'Votes', 'Gross', 'Time']].copy()
    fact_movie.columns = ['Movie_ID', 'Rating', 'Metascore', 'Votes', 'Gross', 'Year']
    fact_movie = fact_movie.merge(dim_time, on='Year')
    fact_movie = fact_movie[['Movie_ID', 'Time_ID', 'Rating', 'Metascore', 'Votes', 'Gross']]

    # Mengembalikan semua dataframe dalam sebuah dictionary
    return {
        'dim_movie': dim_movie, 'dim_director': dim_director, 'dim_star': dim_star,
        'dim_genre': dim_genre, 'dim_time': dim_time, 'bridge_director': bridge_director,
        'bridge_star': bridge_star, 'bridge_genre': bridge_genre, 'fact_movie': fact_movie
    }

def load_to_mysql(engine, dataframes_dict):
    """
    Fungsi ini mengambil dictionary of DataFrames dan memuatnya ke MySQL.
    """
    try:
        with st.spinner("Memuat data ke tabel MySQL..."):
            for table_name, df in dataframes_dict.items():
                df.to_sql(table_name, con=engine, if_exists='replace', index=False)
                st.sidebar.text(f"  ✓ Tabel '{table_name}' berhasil dimuat.")
                time.sleep(0.5) # Efek visual agar tidak terlalu cepat
            return True
    except Exception as e:
        st.error(f"Terjadi kesalahan saat memuat data ke MySQL: {e}")
        return False

# ============================================================================
# BAGIAN 2: APLIKASI STREAMLIT DASHBOARD
# ============================================================================

# --- FUNGSI UNTUK KONEKSI DAN MENGAMBIL DATA DARI DATABASE ---

@st.cache_resource
def get_engine():
    """Membuat dan menyimpan koneksi engine database."""
    engine = create_engine("mysql+pymysql://root:@localhost/TB_BI")
    return engine

@st.cache_data(ttl=600) # Cache data selama 10 menit
def load_movie_data_optimized(_engine):
    """
    Kueri yang dioptimalkan untuk mengambil data unik per film.
    Genre dan Sutradara digabungkan menggunakan GROUP_CONCAT di level database
    untuk menghindari duplikasi baris dan mempercepat pemuatan.
    """
    query = """
    SELECT
        dm.Movie_ID,
        dm.Movie_Name,
        dm.Runtime,
        dm.Plot,
        dt.Year,
        fm.Rating,
        fm.Metascore,
        fm.Votes,
        fm.Gross,
        -- Menggabungkan semua genre film menjadi satu string, dipisahkan koma
        (SELECT GROUP_CONCAT(DISTINCT g.Genre_Name SEPARATOR ', ')
         FROM bridge_genre bg JOIN dim_genre g ON bg.Genre_ID = g.Genre_ID
         WHERE bg.Movie_ID = dm.Movie_ID) AS Genres,
        -- Menggabungkan semua sutradara film menjadi satu string, dipisahkan koma
        (SELECT GROUP_CONCAT(DISTINCT d.Director_Name SEPARATOR ', ')
         FROM bridge_director bd JOIN dim_director d ON bd.Director_ID = d.Director_ID
         WHERE bd.Movie_ID = dm.Movie_ID) AS Directors
    FROM
        fact_movie fm
    JOIN
        dim_movie dm ON fm.Movie_ID = dm.Movie_ID
    JOIN
        dim_time dt ON fm.Time_ID = dt.Time_ID;
    """
    df = pd.read_sql(query, con=_engine)

    # Proses pembersihan data sekarang jauh lebih cepat karena data tidak bengkak
    df['Votes'] = pd.to_numeric(df['Votes'], errors='coerce').fillna(0)
    df['Gross'] = pd.to_numeric(df['Gross'], errors='coerce').fillna(0)
    df['Runtime'] = df['Runtime'].astype(str).str.replace(' min', '', regex=False)
    df['Runtime'] = pd.to_numeric(df['Runtime'], errors='coerce').fillna(0).astype(int)
    return df

# --- KONFIGURASI TAMPILAN UTAMA ---
st.set_page_config(
    page_title="Dashboard Analisis Film (Cepat)",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 Dashboard Analisis Film IMDb (Versi Cepat)")
st.markdown("Sebuah dasbor interaktif dari data film yang telah dioptimalkan untuk performa tinggi.")

# --- SIDEBAR: KONTROL APLIKASI ---
st.sidebar.title("⚙️ Kontrol & Filter")
st.sidebar.markdown("Gunakan tombol ini jika Anda ingin menjalankan ulang proses ETL dari file CSV.")

if st.sidebar.button("Jalankan Proses ETL & Muat ke Database"):
    dataframes = run_etl_process()
    if dataframes:
        engine = get_engine()
        success = load_to_mysql(engine, dataframes)
        if success:
            st.success("🎉 Proses ETL dan pemuatan ke database berhasil!")
            st.balloons()
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

st.sidebar.markdown("---")

# --- KONTEN UTAMA: DASHBOARD ---
try:
    engine = get_engine()
    # Panggil fungsi BARU yang efisien. Hasilnya adalah DataFrame yang ramping dan unik per film.
    df_movies = load_movie_data_optimized(engine)

    # --- SIDEBAR: FILTER DATA ---
    st.sidebar.header("Filter Tampilan Data")
    min_year = int(df_movies['Year'].min())
    max_year = int(df_movies['Year'].max())
    selected_year_range = st.sidebar.slider(
        "Pilih Rentang Tahun:",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year)
    )

    min_rating = float(df_movies['Rating'].min())
    max_rating = float(df_movies['Rating'].max())
    selected_rating_range = st.sidebar.slider(
        "Pilih Rentang Rating IMDb:",
        min_value=min_rating,
        max_value=max_rating,
        value=(min_rating, max_rating),
        step=0.1
    )

    # Terapkan filter ke DataFrame utama kita yang sudah ramping. Ini jauh lebih cepat!
    filtered_df = df_movies[
        (df_movies['Year'] >= selected_year_range[0]) & (df_movies['Year'] <= selected_year_range[1]) &
        (df_movies['Rating'] >= selected_rating_range[0]) & (df_movies['Rating'] <= selected_rating_range[1])
    ].copy() # .copy() untuk menghindari SettingWithCopyWarning


    st.header(f"Analisis untuk Tahun {selected_year_range[0]} - {selected_year_range[1]}")

    # --- KALKULASI METRIK ---
    # Logika untuk perbandingan periode sebelumnya
    current_duration = selected_year_range[1] - selected_year_range[0]
    previous_year_end = selected_year_range[0] - 1
    previous_year_start = previous_year_end - current_duration
    previous_period_df = df_movies[
        (df_movies['Year'] >= previous_year_start) & (df_movies['Year'] <= previous_year_end)
    ]
    
    def calculate_delta(current, previous):
        if previous > 0:
            return ((current - previous) / previous) * 100
        return 0

    total_movies_current = filtered_df.shape[0]
    total_movies_previous = previous_period_df.shape[0]
    delta_movies = calculate_delta(total_movies_current, total_movies_previous)

    avg_rating_current = round(filtered_df['Rating'].mean(), 2) if not filtered_df.empty else 0
    avg_rating_previous = round(previous_period_df['Rating'].mean(), 2) if not previous_period_df.empty else 0
    delta_rating = calculate_delta(avg_rating_current, avg_rating_previous)
    
    highest_rating_current = filtered_df['Rating'].max() if not filtered_df.empty else 0
    highest_rating_previous = previous_period_df['Rating'].max() if not previous_period_df.empty else 0
    delta_highest_rating = calculate_delta(highest_rating_current, highest_rating_previous)

    total_gross_current = filtered_df['Gross'].sum()
    total_gross_previous = previous_period_df['Gross'].sum()
    delta_gross = calculate_delta(total_gross_current, total_gross_previous)

    optimal_runtime_current = 0
    if not filtered_df.empty:
        optimal_runtime_current = filtered_df.loc[filtered_df['Rating'].idxmax()]['Runtime']
    
    optimal_runtime_previous = 0
    if not previous_period_df.empty:
        optimal_runtime_previous = previous_period_df.loc[previous_period_df['Rating'].idxmax()]['Runtime']
    delta_runtime = calculate_delta(optimal_runtime_current, optimal_runtime_previous)

    # Kalkulasi genre populer yang sudah disesuaikan dengan struktur data baru
    popular_genre_current = "N/A"
    if not filtered_df.empty and filtered_df['Genres'].notna().any():
        genres_current = filtered_df['Genres'].dropna().str.split(', ').explode()
        popular_genre_current = ", ".join(genres_current.mode().head(3))

    popular_genre_previous = "N/A"
    if not previous_period_df.empty and previous_period_df['Genres'].notna().any():
        genres_previous = previous_period_df['Genres'].dropna().str.split(', ').explode()
        popular_genre_previous = ", ".join(genres_previous.mode().head(3))

    # Tampilkan metrik
    st.markdown("<h6>Statistik Utama</h6>", unsafe_allow_html=True)
    m_row1_col1, m_row1_col2, m_row1_col3 = st.columns(3)
    m_row2_col1, m_row2_col2, m_row2_col3 = st.columns(3)
    m_row1_col1.metric(label="Jumlah Film", value=f"{total_movies_current:,}", delta=f"{delta_movies:.2f}%")
    m_row1_col2.metric(label="Rata-rata Rating", value=f"{avg_rating_current} ⭐", delta=f"{delta_rating:.2f}%")
    m_row1_col3.metric(label="Rating Tertinggi", value=f"{highest_rating_current}/10 ⭐", delta=f"{delta_highest_rating:.2f}%")
    m_row2_col1.metric(label="Total Pendapatan", value=f"$ {total_gross_current:,.0f}", delta=f"{delta_gross:.2f}%")
    m_row2_col2.metric(label="Waktu Tayang Optimal", value=f"{int(optimal_runtime_current)} min", delta=f"{delta_runtime:.2f}%", help="Waktu tayang dari film dengan rating tertinggi.")
    m_row2_col3.metric(label="Genre Populer", value=popular_genre_current, delta=f"Sebelumnya: {popular_genre_previous}", delta_color="off", help="Genre paling sering muncul (maks. 3 jika imbang).")
    st.markdown("---")


    # --- VISUALISASI ANALISIS PENJUALAN DAN PERFORMA ---
    st.header("Analisis Penjualan dan Performa")
    if filtered_df.empty:
        st.warning("Tidak ada data untuk ditampilkan dengan filter yang dipilih.")
    else:
        vis_row1_col1, vis_row1_col2 = st.columns(2)
        vis_row2_col1, vis_row2_col2 = st.columns(2)

        with vis_row1_col1:
            st.subheader("Total Pendapatan per Tahun")
            sales_per_year = filtered_df.groupby('Year')['Gross'].sum().reset_index()
            fig_line_sales = px.line(sales_per_year, x='Year', y='Gross', markers=True, labels={'Year': 'Tahun', 'Gross': 'Total Pendapatan ($)'})
            st.plotly_chart(fig_line_sales, use_container_width=True)

        with vis_row1_col2:
            st.subheader("Performa Film Terpopuler")
            st.caption("Berdasarkan 10 besar jumlah suara (votes) terbanyak")
            top_10_votes = filtered_df.nlargest(10, 'Votes')
            fig_bar_votes = px.bar(top_10_votes.sort_values('Votes', ascending=True), x='Votes', y='Movie_Name', orientation='h', labels={'Votes': 'Jumlah Suara', 'Movie_Name': ''}, text_auto=True)
            st.plotly_chart(fig_bar_votes, use_container_width=True)

        with vis_row2_col1:
            st.subheader("Hubungan Rating IMDb vs Metascore")
            fig_scatter = px.scatter(filtered_df, x='Rating', y='Metascore', color='Rating', color_continuous_scale=px.colors.sequential.Viridis, hover_name='Movie_Name', labels={'Rating': 'Rating Pengguna (IMDb)', 'Metascore': 'Rating Kritikus'})
            st.plotly_chart(fig_scatter, use_container_width=True)

        with vis_row2_col2:
            st.subheader("Top 10 Film Berdasarkan Pendapatan")
            top_10_gross = filtered_df.nlargest(10, 'Gross')
            fig_bar_gross = px.bar(top_10_gross.sort_values('Gross', ascending=True), x='Gross', y='Movie_Name', orientation='h', labels={'Gross': 'Pendapatan (Juta $)', 'Movie_Name': ''}, text_auto='.2s')
            st.plotly_chart(fig_bar_gross, use_container_width=True)

        # --- BAGIAN ANALISIS PREFERENSI GENRE ---
        st.markdown("---")
        st.header("Analisis Preferensi Genre")
        
        genre_col1, genre_col2 = st.columns(2)
        
        # Kalkulasi genre harus diadaptasi dengan meledakkan (exploding) kolom 'Genres'
        if not filtered_df.empty and filtered_df['Genres'].notna().any():
            genres_exploded_df = filtered_df.dropna(subset=['Genres']).copy()
            genres_exploded_df['Genres'] = genres_exploded_df['Genres'].str.split(', ')
            genres_exploded_df = genres_exploded_df.explode('Genres')

            with genre_col1:
                st.subheader("Distribusi Jumlah Film per Genre")
                genre_counts = genres_exploded_df['Genres'].value_counts()
                fig_pie_genre = px.pie(names=genre_counts.index, values=genre_counts.values, title="Proporsi Genre Film")
                fig_pie_genre.update_traces(textposition='inside', textinfo='percent+label', showlegend=False)
                st.plotly_chart(fig_pie_genre, use_container_width=True)

            with genre_col2:
                st.subheader("Genre dengan Penjualan Tertinggi")
                genre_sales = genres_exploded_df.groupby('Genres')['Gross'].sum().nlargest(10).sort_values()
                fig_bar_genre_sales = px.bar(genre_sales, x=genre_sales.values, y=genre_sales.index, orientation='h', text_auto='.2s', labels={'x': 'Total Pendapatan', 'y': 'Genre'})
                st.plotly_chart(fig_bar_genre_sales, use_container_width=True)

        # --- ANALISIS KEPUASAN PENONTON ---
        st.markdown("---")
        st.header("Analisis Kepuasan Penonton")
        
        satisfaction_col1, satisfaction_col2 = st.columns(2)

        with satisfaction_col1:
            st.subheader("Sebaran Rating Film (Box Plot)")
            fig_box_rating = px.box(filtered_df, y='Rating', points='outliers', title="Distribusi Rating Seluruh Film Terfilter")
            st.plotly_chart(fig_box_rating, use_container_width=True)

        with satisfaction_col2:
            st.subheader("Rata-rata Rating Film per Tahun")
            avg_rating_per_year = filtered_df.groupby('Year')['Rating'].mean().reset_index()
            fig_bar_avg_rating = px.bar(avg_rating_per_year, x='Year', y='Rating', text='Rating', title="Tren Rata-rata Rating IMDb")
            fig_bar_avg_rating.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            st.plotly_chart(fig_bar_avg_rating, use_container_width=True)

        # --- GRAFIK TOP 5 SUTRADARA BERDASARKAN GENRE ---
        st.markdown("---")
        st.subheader("Top 5 Sutradara Berdasarkan Genre")
        
        if not filtered_df.empty and filtered_df['Genres'].notna().any():
            # Buat list genre unik dari data yang sudah difilter
            all_genres_list = sorted(filtered_df['Genres'].dropna().str.split(', ').explode().unique())
            
            selected_genre = st.selectbox(
                "Pilih Genre untuk melihat sutradara terbaik:",
                options=all_genres_list
            )
            
            if selected_genre:
                # Filter film yang mengandung genre terpilih
                genre_specific_df = filtered_df[filtered_df['Genres'].str.contains(selected_genre, na=False)].copy()
                
                # Explode kolom sutradara untuk menghitung
                if not genre_specific_df.empty and genre_specific_df['Directors'].notna().any():
                    genre_specific_df['Directors'] = genre_specific_df['Directors'].str.split(', ')
                    top_directors_series = genre_specific_df.explode('Directors')['Directors'].value_counts().nlargest(5)
                    
                    if not top_directors_series.empty:
                        fig_top_directors = px.bar(
                            top_directors_series,
                            x=top_directors_series.values,
                            y=top_directors_series.index,
                            orientation='h',
                            labels={'x': 'Jumlah Film', 'y': 'Nama Sutradara'},
                            text_auto=True,
                            title=f"Top 5 Sutradara Paling Produktif di Genre '{selected_genre}'"
                        )
                        fig_top_directors.update_layout(yaxis={'categoryorder':'total ascending'})
                        st.plotly_chart(fig_top_directors, use_container_width=True)
                    else:
                        st.info(f"Tidak ditemukan data sutradara untuk genre '{selected_genre}'.")

        # --- MENAMPILKAN DATA MENTAH (opsional) ---
        with st.expander("Lihat Data Mentah Hasil Filter"):
            st.dataframe(filtered_df.reset_index(drop=True))

except (exc.OperationalError, exc.ProgrammingError) as e:
    st.error(f"Gagal terhubung atau mengambil data dari database MySQL: TB_BI.\nDetail Error: {e}")
    st.warning("Pastikan server MySQL Anda berjalan dan database 'TB_BI' sudah ada. Jika ini adalah pertama kali, silakan jalankan proses ETL.")
except Exception as e:
    st.error(f"Terjadi kesalahan yang tidak terduga: {e}")