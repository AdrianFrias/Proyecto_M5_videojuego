import os
import sqlite3
import requests
import time
import urllib.parse
import pandas as pd

DB_PATH = os.getenv('DB_PATH', 'db/gaming_warehouse.db')


def preparar_tabla_reviews():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Hist_Steam_Reviews (
            resena_id TEXT PRIMARY KEY,
            juego_id INTEGER,
            resena_texto TEXT,
            recomendado INTEGER,
            votos_utiles INTEGER,
            votos_graciosos INTEGER,
            puntuacion_ponderada REAL,
            minutos_al_resenar INTEGER,
            minutos_totales INTEGER,
            fecha_creacion_unix INTEGER,
            autor_num_resenas INTEGER,
            autor_num_juegos INTEGER,
            recibido_gratis INTEGER,
            escrito_acceso_anticipado INTEGER,
            FOREIGN KEY(juego_id) REFERENCES CAT_Juego(juego_id)
        );
    """)
    conn.commit()
    conn.close()


def descargar_reviews_steam(max_reviews_por_juego=100, min_reviews_juego=20):
    conn_principal = sqlite3.connect(DB_PATH)
    juegos = pd.read_sql_query("""
        SELECT 
            j.juego_id, j.id_steam, j.titulo, j.recommendations_count,
            COALESCE(r.reviews_en_bd, 0) as reviews_en_bd
        FROM CAT_Juego j
        LEFT JOIN (
            SELECT juego_id, COUNT(*) as reviews_en_bd 
            FROM Hist_Steam_Reviews GROUP BY juego_id
        ) r ON j.juego_id = r.juego_id
        WHERE j.id_steam IS NOT NULL 
          AND j.recommendations_count >= ?
          AND j.steam_price_final IS NOT NULL
          AND COALESCE(r.reviews_en_bd, 0) < ?
        ORDER BY j.recommendations_count DESC
    """, conn_principal, params=(min_reviews_juego, max_reviews_por_juego))
    conn_principal.close()

    if juegos.empty:
        print("No hay juegos pendientes.")
        return

    total_juegos = len(juegos)
    print(f"Juegos pendientes: {total_juegos}")

    for i, (_, juego) in enumerate(juegos.iterrows(), 1):
        appid = juego['id_steam']
        juego_id = juego['juego_id']
        reviews_en_bd = juego['reviews_en_bd']
        reviews_objetivo = min(max_reviews_por_juego, juego['recommendations_count'])
        reviews_restantes = reviews_objetivo - reviews_en_bd

        if reviews_restantes <= 0:
            continue

        reviews_acumuladas = 0
        cursor = '*'

        print(f"\n[{i}/{total_juegos}] {juego['titulo']} (AppID: {appid})")
        print(f"  En BD: {reviews_en_bd} | Objetivo: {reviews_objetivo} | Faltan: {reviews_restantes}")

        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        db_cursor = conn.cursor()

        try:
            while reviews_acumuladas < reviews_restantes:
                cursor_encoded = urllib.parse.quote(cursor)
                url = (
                    f"https://store.steampowered.com/appreviews/{appid}"
                    f"?json=1&filter=recent&language=latam&purchase_type=all"
                    f"&cursor={cursor_encoded}&num_per_page=100"
                )
                try:
                    res = requests.get(url, timeout=15)
                    if res.status_code != 200:
                        print(f"  > Error HTTP {res.status_code}. Saltando.")
                        break

                    data = res.json()
                    if not data.get('success') or not data.get('reviews'):
                        print("  > Sin más reseñas.")
                        break

                    batch = data['reviews']
                    insert_data = [
                        (
                            str(r['recommendationid']), juego_id, r['review'],
                            1 if r['voted_up'] else 0, r['votes_up'], r['votes_funny'],
                            float(r['weighted_vote_score']), r['author']['playtime_at_review'],
                            r['author']['playtime_forever'], r['timestamp_created'],
                            r['author']['num_reviews'], r['author']['num_games_owned'],
                            1 if r['received_for_free'] else 0,
                            1 if r['written_during_early_access'] else 0
                        )
                        for r in batch
                    ]

                    antes = db_cursor.execute(
                        "SELECT COUNT(*) FROM Hist_Steam_Reviews WHERE juego_id = ?", (juego_id,)
                    ).fetchone()[0]

                    db_cursor.executemany(
                        "INSERT OR IGNORE INTO Hist_Steam_Reviews VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        insert_data
                    )
                    conn.commit()

                    despues = db_cursor.execute(
                        "SELECT COUNT(*) FROM Hist_Steam_Reviews WHERE juego_id = ?", (juego_id,)
                    ).fetchone()[0]

                    nuevas = despues - antes
                    reviews_acumuladas += len(batch)
                    print(f"  > Lote: {len(batch)} | Nuevas: {nuevas} | Sesión: {reviews_acumuladas}/{reviews_restantes}")

                    nuevo_cursor = data.get('cursor')
                    if not nuevo_cursor or nuevo_cursor == cursor:
                        print("  > Fin de paginación.")
                        break
                    cursor = nuevo_cursor

                    if len(batch) < 100:
                        print("  > Último lote parcial.")
                        break

                    time.sleep(0.5)

                except requests.exceptions.Timeout:
                    print("  > Timeout. Reintentando en 5s...")
                    time.sleep(5)
                    continue
                except Exception as e:
                    print(f"  > Error: {e}")
                    break
        finally:
            conn.close()


def run(max_reviews=1000, min_reviews=100):
    preparar_tabla_reviews()
    descargar_reviews_steam(max_reviews_por_juego=max_reviews, min_reviews_juego=min_reviews)
    print("=== Reviews completadas ===")


if __name__ == "__main__":
    run()
