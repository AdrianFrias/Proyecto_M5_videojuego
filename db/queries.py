import os
import sqlite3
import pandas as pd
import functools

DB_PATH = os.getenv('DB_PATH', 'db/gaming_warehouse.db')
PARQUET_SENTIMIENTO = os.getenv('SENTIMIENTO_PATH', 'datos/Resenas_separdas.parquet')

ICONOS_EMOCION = {
    'Gran calidad general con fallos puntuales y variabilidad':'🏆',
    'Bueno pero irregular: divertido, pero caro o incompleto':'⚖️',
    'Problemas técnicos graves arruinan experiencia jugable':'🔧',
    'Entretenido casual, brilla más en multijugador':'👥',
    'Obra maestra nostálgica, adictiva y emocional':'🕹️',
    'Caótico, emocional, memeable y altamente polarizado':'🔥',
    'Buena calidad general en historia, arte y jugabilidad':'⭐',
    'Buen diseño, pero repetitivo y con desgaste':'🎯',
    'Análisis profundo con enfoque narrativo y artístico':'📖',
    'sin_cluster':'❓',
}

LABELS_EMOCION = {
    'Gran calidad general con fallos puntuales y variabilidad': 'Calidad con fallos',
    'Bueno pero irregular: divertido, pero caro o incompleto':  'Bueno pero irregular',
    'Problemas técnicos graves arruinan experiencia jugable':   'Problemas técnicos',
    'Entretenido casual, brilla más en multijugador':           'Casual / Multi',
    'Obra maestra nostálgica, adictiva y emocional':            'Obra maestra',
    'Caótico, emocional, memeable y altamente polarizado':      'Caótico / Meme',
    'Buena calidad general en historia, arte y jugabilidad':    'Buena calidad',
    'Buen diseño, pero repetitivo y con desgaste':              'Diseño con desgaste',
    'Análisis profundo con enfoque narrativo y artístico':      'Narrativo / Artístico',
    'sin_cluster':                                              'Sin clasificar',
}


@functools.lru_cache(maxsize=1)
def _cargar_sentimiento() -> pd.DataFrame:
    try:
        return pd.read_parquet(PARQUET_SENTIMIENTO, columns=['juego_id', 'emocion', 'recomendado'])
    except Exception:
        return pd.DataFrame()


def get_sentimiento_juego(juego_id: int) -> dict:
    """
    Retorna distribución de emociones para un juego.
    {
      'total': int,
      'pct_positivo': float,
      'emociones': [{'emocion': str, 'icono': str, 'label': str, 'count': int, 'pct': float}, ...]
    }
    """
    df = _cargar_sentimiento()
    if df.empty:
        return {}

    sub = df[df['juego_id'] == juego_id]
    if sub.empty:
        return {}

    total = len(sub)
    pct_positivo = sub['recomendado'].mean() * 100

    conteo = (
        sub[sub['emocion'] != 'sin_cluster']['emocion']
        .value_counts()
        # .head(5)
    )

    emociones = [
        {
            'emocion': emo,
            'icono':   ICONOS_EMOCION.get(emo, '•'),
            'label':   LABELS_EMOCION.get(emo, emo.replace('_', ' ').title()),
            'count':   int(cnt),
            'pct':     round(cnt / total * 100, 1),
        }
        for emo, cnt in conteo.items()
    ]

    return {
        'total':        total,
        'pct_positivo': round(pct_positivo, 1),
        'emociones':    emociones,
    }


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def buscar_juegos(query: str, limit: int = 20) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query("""
            SELECT juego_id, titulo, url_portada, puntuacion_igdb,
                   fecha_lanzamiento, steam_price_final, categoria
            FROM CAT_Juego
            WHERE titulo LIKE ?
            AND steam_price_final IS NOT NULL
              AND steam_price_final != -1
            ORDER BY conteo_votos_igdb DESC
            LIMIT ?
        """, conn, params=(f"%{query}%", limit))


def get_historial_precios(juego_id: int) -> pd.DataFrame:
    with get_conn() as conn:
        try:
            return pd.read_sql_query("""
                SELECT
                    datetime(hp.fecha_unix, 'unixepoch') AS fecha,
                    hp.precio,
                    hp.corte_descuento,
                    t.nombre AS tienda
                FROM Hist_Precios_ITAD hp
                JOIN REL_Juego_ITAD ri ON hp.itad_id_texto = ri.itad_id_texto
                JOIN CAT_Tienda t ON hp.id_tienda = t.id_tienda
                WHERE ri.juego_id = ?
                ORDER BY hp.fecha_unix ASC
            """, conn, params=(juego_id,))
        except Exception:
            return pd.DataFrame()


def get_juego(juego_id: int) -> dict:
    with get_conn() as conn:
        row = conn.execute("""
            SELECT * FROM CAT_Juego WHERE juego_id = ?
        """, (juego_id,)).fetchone()
        return dict(row) if row else {}


def get_generos(juego_id: int) -> list:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT g.nombre FROM CAT_Genero g
            JOIN REL_Juego_Genero r ON g.genero_id = r.genero_id
            WHERE r.juego_id = ?
        """, (juego_id,)).fetchall()
        return [r[0] for r in rows]


def get_itad_id(juego_id: int) -> str | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT itad_id_texto FROM REL_Juego_ITAD WHERE juego_id = ?",
            (juego_id,)
        ).fetchone()
        return row[0] if row else None


def get_plataformas(juego_id: int) -> list:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT p.nombre FROM CAT_Plataforma p
            JOIN REL_Juego_Plataforma r ON p.plataforma_id = r.plataforma_id
            WHERE r.juego_id = ?
        """, (juego_id,)).fetchall()
        return [r[0] for r in rows]


def get_modos(juego_id: int) -> list:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT m.nombre FROM CAT_Modo_Juego m
            JOIN REL_Juego_Modo r ON m.modo_id = r.modo_id
            WHERE r.juego_id = ?
        """, (juego_id,)).fetchall()
        return [r[0] for r in rows]


def get_desarrolladores(juego_id: int) -> list:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT e.nombre FROM CAT_Empresa e
            JOIN REL_Juego_Desarrollador r ON e.empresa_id = r.empresa_id
            WHERE r.juego_id = ?
        """, (juego_id,)).fetchall()
        return [r[0] for r in rows]


def get_precio_itad(juego_id: int) -> dict:
    with get_conn() as conn:
        try:
            row = conn.execute("""
                SELECT d.precio_actual, d.precio_minimo, d.en_bundle,
                       t1.nombre as tienda_actual, t2.nombre as tienda_minimo
                FROM Datos_Actuales_ITAD d
                JOIN REL_Juego_ITAD r ON d.itad_id_texto = r.itad_id_texto
                LEFT JOIN CAT_Tienda t1 ON d.tienda_actual = t1.id_tienda
                LEFT JOIN CAT_Tienda t2 ON d.tienda_minimo = t2.id_tienda
                WHERE r.juego_id = ?
            """, (juego_id,)).fetchone()
            return dict(row) if row else {}
        except Exception:
            return {}


def get_stats_generales() -> dict:
    with get_conn() as conn:
        stats = {}
        stats['total_juegos'] = conn.execute("SELECT COUNT(*) FROM CAT_Juego").fetchone()[0]
        stats['con_steam'] = conn.execute("SELECT COUNT(*) FROM CAT_Juego WHERE id_steam IS NOT NULL").fetchone()[0]
        stats['con_precio'] = conn.execute("SELECT COUNT(*) FROM CAT_Juego WHERE steam_price_final IS NOT NULL AND steam_price_final != -1").fetchone()[0]
        try:
            stats['total_reviews'] = conn.execute("SELECT COUNT(*) FROM Hist_Steam_Reviews").fetchone()[0]
        except Exception:
            stats['total_reviews'] = 0
        try:
            stats['con_itad'] = conn.execute("SELECT COUNT(*) FROM Datos_Actuales_ITAD").fetchone()[0]
        except Exception:
            stats['con_itad'] = 0
        try:
            stats['con_hltb'] = conn.execute("SELECT COUNT(*) FROM CAT_Juego WHERE hltb_historia_principal IS NOT NULL").fetchone()[0]
        except Exception:
            stats['con_hltb'] = 0
        return stats


def get_top_juegos(limit: int = 10) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query("""
            SELECT titulo, puntuacion_igdb, conteo_votos_igdb,
                   steam_price_final, recommendations_count
            FROM CAT_Juego
            WHERE puntuacion_igdb > 0
            ORDER BY puntuacion_igdb DESC
            LIMIT ?
        """, conn, params=(limit,))


def get_distribucion_precios() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query("""
            SELECT steam_price_final as precio
            FROM CAT_Juego
            WHERE steam_price_final IS NOT NULL
              AND steam_price_final > 0
              AND steam_price_final < 100
        """, conn)