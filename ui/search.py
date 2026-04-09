import streamlit as st
from db.queries import buscar_juegos
from ui.game_detail import render as render_detail
import numpy as np
import math

 
def render():
    # Si hay un juego seleccionado, mostrar su detalle
    if "juego_seleccionado" in st.session_state and st.session_state.juego_seleccionado:
        col_back, _ = st.columns([1, 8])
        with col_back:
            if st.button("← Volver"):
                st.session_state.juego_seleccionado = None
                st.rerun()
        render_detail(st.session_state.juego_seleccionado)
        return

    # ── Header ────────────────────────────────────────────────────────────────
    st.title("BUSCAR JUEGO")
    st.caption("Busca en el catálogo de +11,000 juegos")

    # ── Search bar ────────────────────────────────────────────────────────────
    col_input, col_btn = st.columns([6, 1])
    with col_input:
        query = st.text_input("", placeholder="Ej: The Witcher, Dark Souls, Hades...", label_visibility="collapsed", key="search_query")
    with col_btn:
        buscar = st.button("BUSCAR", use_container_width=True)

    if not query and not buscar:
        _render_placeholder()
        return

    if query:
        with st.spinner("Buscando..."):
            resultados = buscar_juegos(query)

        if resultados.empty:
            st.write("🔍")
            st.write(f'Sin resultados para **{query}**')
            return

        st.caption(f"{len(resultados)} resultados")
        _render_grid(resultados)


def _render_placeholder():
    st.write("🎮")
    st.write("ESCRIBE EL NOMBRE DE UN JUEGO PARA COMENZAR")


def _render_grid(df):
    cols = st.columns(6)
    for i, (_, row) in enumerate(df.iterrows()):
        with cols[i % 6]:
            _render_card(row)


def _render_card(row):
    titulo = row.get('titulo', 'Sin título')
    portada = row.get('url_portada') or ''
    score = row.get('puntuacion_igdb') or 0
    precio = row.get('steam_price_final')
    juego_id = row.get('juego_id')

    # Precio display
    if precio is None or (isinstance(precio, float) and math.isnan(precio)):
        precio_str = "—"
    elif precio == -1:
        precio_str = "No disponible"
    elif precio == 0:
        precio_str = "Gratis"
    else:
        precio_str = f"${precio:.2f}"

    score_str = f"★ {score:.0f}" if score > 0 else "—"

    if portada:
        st.image(portada, use_container_width=True)
    else:
        st.write("🎮")

    st.write(f"**{titulo}**")

    col_score, col_precio = st.columns(2)
    with col_score:
        st.write(score_str)
    with col_precio:
        st.write(precio_str)

    if st.button("Ver detalle", key=f"btn_{juego_id}", use_container_width=True):
        st.session_state.juego_seleccionado = int(juego_id)
        st.rerun()
