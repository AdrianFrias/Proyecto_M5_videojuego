import streamlit as st
from db.queries import (
    get_juego, get_generos, get_plataformas, get_modos,
    get_desarrolladores, get_itad_id, get_historial_precios,
    get_sentimiento_juego,
)
from inference import sentiment, pricing
import os
import requests
import plotly.express as px


@st.cache_data(ttl=3600)
def _get_usd_mxn() -> float:
    try:
        res = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        return res.json()["rates"]["MXN"]
    except Exception:
        return 20.0


def render(juego_id: int):
    juego = get_juego(juego_id)
    if not juego:
        st.error("Juego no encontrado.")
        return

    generos     = get_generos(juego_id)
    plataformas = get_plataformas(juego_id)
    modos       = get_modos(juego_id)
    devs        = get_desarrolladores(juego_id)
    sentimiento = get_sentimiento_juego(juego_id)

    col_img, col_info = st.columns([1, 4])

    with col_img:
        if juego.get('url_portada'):
            st.image(juego['url_portada'], use_container_width=True)

    with col_info:
        st.title(juego.get('titulo', '—'))
        st.caption(str(juego.get('fecha_lanzamiento', 'Fecha desconocida')))

        if generos:
            st.write(" · ".join(generos[:5]))

        score       = juego.get('puntuacion_igdb') or 0
        votos       = juego.get('conteo_votos_igdb') or 0
        precio_steam = juego.get('steam_price_final')
        recom       = juego.get('recommendations_count') or 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Score IGDB",       f"{score:.1f}" if score > 0 else "—")
        m2.metric("Votos",            f"{votos:,}"   if votos > 0 else "—")
        m3.metric("Precio Steam",     f"${precio_steam:.2f}" if precio_steam and precio_steam > 0 else ("Gratis" if precio_steam == 0 else "—"))
        m4.metric("Recomendaciones",  f"{recom:,}"   if recom > 0 else "—")

        if devs:
            st.caption("🏢 " + " · ".join(devs[:3]))
        if plataformas:
            st.caption("🖥 " + " · ".join(plataformas[:5]))

        # ── Sentimiento ───────────────────────────────────────────────────────
        if sentimiento and sentimiento.get('emociones'):
            st.caption(
                f"💬 {sentimiento['total']:,} reseñas analizadas · "
                f"{sentimiento['pct_positivo']:.0f}% positivas"
            )
            cols_emo = st.columns(len(sentimiento['emociones']))
            for col, emo in zip(cols_emo, sentimiento['emociones']):
                with col:
                    st.markdown(
                        f"<div style='text-align:center; padding:6px 4px; "
                        f"background:#1e1e2e; border-radius:8px; font-size:0.78rem;'>"
                        f"<div style='font-size:1.3rem'>{emo['icono']}</div>"
                        f"<div style='color:#e2e8f0; font-weight:600'>{emo['label']}</div>"
                        f"<div style='color:#64748b'>{emo['pct']}%</div>"
                        f"</div>",
                        unsafe_allow_html=True
                    )

    st.divider()

    tab_info, tab_precio, tab_ml = st.tabs([
        "📋 Info", "💰 Precios", "🤖 Análisis IA"
    ])

    with tab_info:
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("#### Descripción")
            st.write(juego.get('resumen', 'Sin datos'))

            if juego.get('historia') and juego['historia'] != 'Sin datos':
                with st.expander("Historia / Storyline"):
                    st.write(juego['historia'])

        with col_b:
            st.markdown("#### Tiempo de juego (HLTB)")
            h_principal = juego.get('hltb_historia_principal')
            h_extra     = juego.get('hltb_historia_extra')
            h_comp      = juego.get('hltb_completacionista')

            if any([h_principal, h_extra, h_comp]):
                for label, val in [
                    ("Historia principal", h_principal),
                    ("Historia + extras",  h_extra),
                    ("Completacionista",   h_comp),
                ]:
                    if val:
                        pct = min(int((val / (h_comp or val)) * 100), 100)
                        c1, c2 = st.columns([3, 1])
                        c1.caption(label)
                        c2.caption(f"{val:.0f}h")
                        st.progress(pct / 100)
            else:
                st.caption("Sin datos de HLTB para este juego.")

            st.markdown("#### Modos de juego")
            st.write(" · ".join(modos) if modos else "—")

    with tab_precio:
        col_s, col_i = st.columns(2)

        with col_s:
            st.markdown("#### Steam")
            p_initial = juego.get('steam_price_initial') or 0
            p_final   = juego.get('steam_price_final') or 0
            descuento = juego.get('steam_discount_percent') or 0

            if p_final and p_final > 0:
                if descuento > 0:
                    st.metric("Precio", f"${p_final:.2f}", delta=f"-{descuento}% (antes ${p_initial:.2f})")
                else:
                    st.metric("Precio", f"${p_final:.2f}")
            elif p_final == 0:
                st.success("Gratis en Steam")
            else:
                st.caption("Sin datos de precio en Steam.")

        with col_i:
            st.markdown("#### Otras tiendas")
            st.caption("Precios en MXN · Fuente: IsThereAnyDeal · Pueden variar por tienda y región")
            itad_id = get_itad_id(juego_id)
            if itad_id:
                with st.spinner("Consultando precios..."):
                    try:
                        res = requests.post(
                            f"https://api.isthereanydeal.com/games/prices/v2?key={os.getenv('ITAD_API_KEY')}&country=MX&nondeals=1",
                            json=[itad_id], timeout=10
                        )
                        deals = res.json()[0].get('deals', []) if res.status_code == 200 else []
                        deals = [d for d in deals if 'steam' not in d['shop'].get('name', '').lower()]
                    except Exception:
                        deals = []

                if deals:
                    tc = _get_usd_mxn()
                    for deal in deals:
                        tienda     = deal['shop'].get('name', '—')
                        precio_usd = deal['price']['amount']
                        moneda     = deal['price'].get('currency', 'USD')
                        precio_mxn = precio_usd * tc if moneda == 'USD' else precio_usd
                        corte      = deal.get('cut', 0)
                        c1, c2 = st.columns([3, 1])
                        c1.write(tienda)
                        precio_str = f"${precio_mxn:.0f} MXN"
                        c2.write(f"-{corte}%  {precio_str}" if corte > 0 else precio_str)
                else:
                    st.caption("Sin ofertas activas en otras tiendas.")
            else:
                st.caption("Sin datos de ITAD para este juego.")

    with tab_ml:
        st.markdown("#### 🤖 Análisis IA")

        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            c1.write("**Predicción de descuento (próximos 30 días)**")
            if pricing.is_available():
                c2.success("ACTIVO")
                try:
                    resultado = pricing.predict(juego_id)
                    st.markdown(f"### {resultado['icono']} {resultado['label']}")
                    st.caption(resultado['descripcion'])
                    st.progress((resultado['clase'] + 1) / 5)
                except ValueError:
                    st.caption("Sin datos suficientes para predecir este juego.")
                except Exception as e:
                    st.caption(f"Error al predecir: {e}")
            else:
                c2.caption("EN DESARROLLO")

        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            c1.write("**Análisis de sentimiento de reseñas**")
            if sentiment.is_available():
                c2.success("ACTIVO")
            else:
                c2.caption("EN DESARROLLO")