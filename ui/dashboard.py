import streamlit as st
from db.queries import get_stats_generales, get_top_juegos, get_distribucion_precios


def render():
    st.title("Dashboard")
    st.caption("Vista general del catálogo")

    stats = get_stats_generales()

    # ── KPIs ──────────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Juegos totales", f"{stats.get('total_juegos', 0):,}")
    c2.metric("Con Steam ID",   f"{stats.get('con_steam', 0):,}")
    c3.metric("Con precio",     f"{stats.get('con_precio', 0):,}")
    c4.metric("Reseñas",        f"{stats.get('total_reviews', 0):,}")
    c5.metric("Con ITAD",       f"{stats.get('con_itad', 0):,}")
    c6.metric("Con HLTB",       f"{stats.get('con_hltb', 0):,}")

    st.divider()

    col_a, col_b = st.columns(2)

    # ── Top juegos ────────────────────────────────────────────────────────────
    with col_a:
        st.markdown("#### 🏆 Top 10 por puntuación IGDB")
        top = get_top_juegos(10)
        if not top.empty:
            for i, (_, row) in enumerate(top.iterrows(), 1):
                score = row.get('puntuacion_igdb') or 0
                pct = min(int(score), 100) / 100
                c1, c2 = st.columns([4, 1])
                c1.caption(f"{i}. {row['titulo']}")
                c2.caption(f"{score:.1f}")
                st.progress(pct)
        else:
            st.caption("Sin datos.")

    # ── Distribución de precios ───────────────────────────────────────────────
    with col_b:
        st.markdown("#### 💰 Distribución de precios (Steam)")
        df_precios = get_distribucion_precios()
        if not df_precios.empty:
            try:
                import plotly.graph_objects as go
                fig = go.Figure(go.Histogram(
                    x=df_precios['precio'],
                    nbinsx=30,
                    marker_color='#00f5a0',
                    marker_line_color='#0a0a0f',
                    marker_line_width=1,
                    opacity=0.85
                ))
                fig.update_layout(
                    plot_bgcolor='#0f0f1a',
                    paper_bgcolor='#0f0f1a',
                    font_color='#64748b',
                    margin=dict(l=0, r=0, t=10, b=0),
                    xaxis=dict(title="Precio (USD)", gridcolor='#1e1e2e', color='#64748b'),
                    yaxis=dict(title="Juegos", gridcolor='#1e1e2e', color='#64748b'),
                    height=320,
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.bar_chart(df_precios['precio'].value_counts().sort_index())
        else:
            st.caption("Sin datos de precios.")

    # ── Cobertura de datos ────────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 📦 Cobertura del catálogo")

    total = stats.get('total_juegos', 1)
    coberturas = [
        ("Steam ID",     stats.get('con_steam', 0)),
        ("Precio Steam", stats.get('con_precio', 0)),
        ("ITAD precios", stats.get('con_itad', 0)),
        ("HLTB tiempos", stats.get('con_hltb', 0)),
    ]

    cols = st.columns(len(coberturas))
    for col, (label, valor) in zip(cols, coberturas):
        pct = (valor / total * 100) if total > 0 else 0
        with col:
            st.metric(label, f"{pct:.1f}%", delta=f"{valor:,} / {total:,}")
            st.progress(min(pct, 100) / 100)
