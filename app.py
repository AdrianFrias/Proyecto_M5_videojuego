import streamlit as st
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(
    page_title="GameLens AI",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Estilos globales ──────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Inter:wght@300;400;500&display=swap');

:root {
    --bg:        #0a0a0f;
    --surface:   #12121a;
    --border:    #1e1e2e;
    --accent:    #00f5a0;
    --accent2:   #00b4d8;
    --danger:    #ff4d6d;
    --text:      #e2e8f0;
    --muted:     #64748b;
    --card-bg:   #0f0f1a;
}

html, body, [data-testid="stAppViewContainer"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'Inter', sans-serif;
}

[data-testid="stSidebar"] {
    background-color: var(--surface) !important;
    border-right: 1px solid var(--border);
}

h1, h2, h3 { font-family: 'Rajdhani', sans-serif; letter-spacing: 0.05em; }

/* Botones */
.stButton > button {
    background: transparent;
    border: 1px solid var(--accent);
    color: var(--accent);
    font-family: 'Rajdhani', sans-serif;
    font-size: 1rem;
    letter-spacing: 0.08em;
    transition: all 0.2s;
    border-radius: 2px;
}
.stButton > button:hover {
    background: var(--accent);
    color: var(--bg);
}

/* Input */
.stTextInput > div > input {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    border-radius: 2px !important;
}
.stTextInput > div > input:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 1px var(--accent) !important;
}

/* Métricas */
[data-testid="metric-container"] {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1rem;
}
[data-testid="metric-container"] label { color: var(--muted) !important; font-size: 0.75rem; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { color: var(--accent) !important; font-family: 'Rajdhani', sans-serif; font-size: 1.8rem; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { border-bottom: 1px solid var(--border); gap: 0; }
.stTabs [data-baseweb="tab"] {
    background: transparent;
    color: var(--muted);
    font-family: 'Rajdhani', sans-serif;
    letter-spacing: 0.06em;
    padding: 0.5rem 1.5rem;
    border-bottom: 2px solid transparent;
}
.stTabs [aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom: 2px solid var(--accent) !important;
    background: transparent !important;
}

/* Divider */
hr { border-color: var(--border) !important; }

/* Scrollbar */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }

/* Ocultar elementos de Streamlit */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar / Navegación ──────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding: 1rem 0 2rem 0;'>
        <div style='font-family: Rajdhani, sans-serif; font-size: 1.8rem; font-weight: 700; color: #00f5a0; letter-spacing: 0.1em;'>
            GAME<span style='color: #e2e8f0;'>LENS</span>
        </div>
        <div style='font-size: 0.7rem; color: #64748b; letter-spacing: 0.2em; margin-top: -4px;'>
            AI · GAME INTELLIGENCE
        </div>
    </div>
    """, unsafe_allow_html=True)

    pagina = st.radio(
        "",
        ["🔍  Buscar juego", "📊  Dashboard", "⚙️  Pipeline"],
        label_visibility="collapsed"
    )

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("<p style='color: #64748b; font-size: 0.72rem; letter-spacing: 0.05em;'>v0.1 · En desarrollo</p>", unsafe_allow_html=True)

# ── Routing ───────────────────────────────────────────────────────────────────
if "🔍" in pagina:
    from ui.search import render
    render()
elif "📊" in pagina:
    from ui.dashboard import render
    render()
elif "⚙️" in pagina:
    from ui.pipeline_admin import render
    render()
