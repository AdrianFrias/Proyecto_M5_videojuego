"""
Módulo de predicción de descuentos.
Carga el modelo desde models/ y predice la clase de descuento
esperada en los próximos 30 días para un juego dado.
"""
import os
import joblib
import pandas as pd
import sqlite3

MODEL_PATH = os.path.join("models", "discount_predictor_v6_loss1.0262_acc0.5689_20260407_2048.pkl")
DB_PATH    = os.getenv('DB_PATH', 'db/gaming_warehouse.db')

_model = None

CLASES = {
    0: ("Sin descuento esperado",   "🔵", "Sin descuento significativo en los próximos 30 días."),
    1: ("Descuento moderado",       "🟡", "Posible descuento del 20–49% en los próximos 30 días."),
    2: ("Descuento grande",         "🟠", "Posible descuento del 50–74% en los próximos 30 días."),
    3: ("Descuento agresivo",       "🔴", "Posible descuento del 75–89% en los próximos 30 días."),
    4: ("Descuento extremo",        "🔥", "Posible descuento del 90%+ en los próximos 30 días."),
}


def is_available() -> bool:
    return os.path.exists(MODEL_PATH)


def get_model():
    global _model
    if _model is None and is_available():
        _model = joblib.load(MODEL_PATH)
    return _model


def _get_features(juego_id: int) -> pd.DataFrame | None:
    """
    Construye el vector de features para un juego dado
    usando su historial más reciente en Datos_Actuales_ITAD.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("""
            SELECT
                j.puntuacion_igdb,
                j.conteo_votos_igdb,
                j.recommendations_count,
                j.metacritic_score,
                j.conteo_dlc,
                j.achievements_count,
                julianday('now') - julianday(j.fecha_lanzamiento) AS edad_juego_dias,
                COALESCE(da.precio_actual, j.steam_price_final, 0) AS precio_actual,
                COALESCE(da.precio_minimo, j.steam_price_final, 0) AS precio_minimo,
                COALESCE(da.precio_actual / NULLIF(da.precio_minimo, 0), 1.0) AS precio_vs_minimo,
                CAST(strftime('%m', 'now') AS INTEGER) AS mes,
                CAST(strftime('%w', 'now') AS INTEGER) AS dia_semana
            FROM CAT_Juego j
            LEFT JOIN REL_Juego_ITAD ri ON j.juego_id = ri.juego_id
            LEFT JOIN Datos_Actuales_ITAD da ON ri.itad_id_texto = da.itad_id_texto
            WHERE j.juego_id = ?
            LIMIT 1
        """, conn, params=(juego_id,))
        conn.close()
        return df if not df.empty else None
    except Exception:
        return None


def predict(juego_id: int) -> dict:
    model = get_model()
    if model is None:
        raise RuntimeError("Modelo no disponible.")

    features = _get_features(juego_id)
    if features is None:
        raise ValueError("Sin datos suficientes para este juego.")

    # alinear columnas con las que espera el modelo
    model_cols = model.get_booster().feature_names
    for col in model_cols:
        if col not in features.columns:
            features[col] = 0
    features = features[model_cols].fillna(0)

    clase = int(model.predict(features)[0])
    label, icono, descripcion = CLASES[clase]

    return {
        "clase":       clase,
        "label":       label,
        "icono":       icono,
        "descripcion": descripcion,
    }