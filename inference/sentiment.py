"""
Módulo de inferencia de sentimiento.
Cuando el modelo esté entrenado, guarda el .pkl en models/sentiment_clf.pkl
y este módulo se activa automáticamente.
"""
import os

MODEL_PATH = os.path.join("models", "sentiment_clf.pkl")
_model = None


def is_available() -> bool:
    return os.path.exists(MODEL_PATH)


def get_model():
    global _model
    if _model is None and is_available():
        import joblib
        _model = joblib.load(MODEL_PATH)
    return _model


def predict(text: str) -> dict:
    """
    Retorna: {"label": "positive"|"negative", "confidence": float}
    """
    model = get_model()
    if model is None:
        raise RuntimeError("Modelo no disponible.")
    proba = model.predict_proba([text])[0]
    label = "positive" if proba[1] > 0.5 else "negative"
    return {"label": label, "confidence": float(max(proba))}
