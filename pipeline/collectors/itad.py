import os
import sqlite3
import requests
import time
import datetime
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv(Path(__file__).resolve().parents[2] / '.env')

ITAD_KEY = os.getenv('ITAD_API_KEY')
DB_PATH = os.getenv('DB_PATH', 'db/gaming_warehouse.db')
ITAD_BASE = 'https://api.isthereanydeal.com'
TIMEOUT = 15


def preparar_tablas_itad():
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CAT_ITAD_Juego (
                itad_id_texto TEXT PRIMARY KEY,
                itad_titulo TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS REL_Juego_ITAD (
                juego_id INTEGER,
                itad_id_texto TEXT,
                PRIMARY KEY (juego_id, itad_id_texto),
                FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id),
                FOREIGN KEY (itad_id_texto) REFERENCES CAT_ITAD_Juego(itad_id_texto)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CAT_Tienda (
                id_tienda INTEGER PRIMARY KEY,
                nombre TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Hist_Precios_ITAD (
                itad_id_texto TEXT,
                id_tienda INTEGER,
                precio REAL,
                corte_descuento INTEGER,
                fecha_unix INTEGER,
                PRIMARY KEY (itad_id_texto, id_tienda, fecha_unix)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Datos_Actuales_ITAD (
                itad_id_texto TEXT PRIMARY KEY,
                precio_actual REAL,
                tienda_actual INTEGER,
                precio_minimo REAL,
                tienda_minimo INTEGER,
                fecha_minimo INTEGER,
                en_bundle INTEGER DEFAULT 0,
                fecha_actualizacion INTEGER
            )
        """)
        conn.commit()
        print('Tablas ITAD listas.')
    finally:
        conn.close()


def buscar_ids_itad(limite=100):
    conn = sqlite3.connect(DB_PATH)
    try:
        juegos = conn.execute("""
            SELECT j.juego_id, j.titulo
            FROM CAT_Juego j
            LEFT JOIN REL_Juego_ITAD r ON j.juego_id = r.juego_id
            WHERE j.id_steam IS NOT NULL AND r.itad_id_texto IS NULL AND j.categoria = 0
            LIMIT ?
        """, (limite,)).fetchall()
    finally:
        conn.close()

    if not juegos:
        print('No hay más juegos por vincular.')
        return False

    print(f'{len(juegos)} juegos a vincular')
    for juego_id, titulo in juegos:
        try:
            res = requests.get(
                f'{ITAD_BASE}/games/search/v1',
                params={'key': ITAD_KEY, 'title': titulo, 'limit': 1},
                timeout=10
            )
            if res.status_code == 200:
                data = res.json()
                if data:
                    itad_id = data[0].get('id')
                    if itad_id:
                        conn = sqlite3.connect(DB_PATH)
                        try:
                            cursor = conn.cursor()
                            cursor.execute('INSERT OR IGNORE INTO CAT_ITAD_Juego (itad_id_texto, itad_titulo) VALUES (?, ?)', (itad_id, titulo))
                            cursor.execute('INSERT OR IGNORE INTO REL_Juego_ITAD (juego_id, itad_id_texto) VALUES (?, ?)', (juego_id, itad_id))
                            conn.commit()
                            print(f'  OK: {titulo} -> {itad_id}')
                        finally:
                            conn.close()
            elif res.status_code == 401:
                print('  Error 401 - revisar API key')
                return False
            elif res.status_code == 429:
                print('  Rate limit - esperando 20s')
                time.sleep(20)
                continue
        except Exception as e:
            print(f'  Error con {titulo}: {e}')
        time.sleep(1)

    return True


def _fetch_historial(itad_id, since_iso):
    res = requests.get(f'{ITAD_BASE}/games/history/v2',
        params={'key': ITAD_KEY, 'id': itad_id, 'country': 'MX', 'since': since_iso}, timeout=TIMEOUT)
    return res.json() if res.status_code == 200 else []


def _fetch_low(itad_id):
    res = requests.post(f'{ITAD_BASE}/games/historylow/v1?key={ITAD_KEY}&country=MX', json=[itad_id], timeout=TIMEOUT)
    if res.status_code == 200 and res.json():
        low = res.json()[0].get('low')
        if low:
            ts = int(datetime.datetime.fromisoformat(low['timestamp']).timestamp()) if low.get('timestamp') else None
            return low.get('price', {}).get('amount'), low.get('shop', {}).get('id'), ts
    return None, None, None


def _fetch_precio_actual(itad_id):
    res = requests.post(f'{ITAD_BASE}/games/prices/v2?key={ITAD_KEY}&country=MX', json=[itad_id], timeout=TIMEOUT)
    if res.status_code == 200 and res.json():
        deals = res.json()[0].get('deals')
        if deals:
            return deals[0]['price']['amount'], deals[0]['shop']['id']
    return None, None


def _fetch_bundle(itad_id):
    res = requests.post(f'{ITAD_BASE}/games/bundles/v2?key={ITAD_KEY}', json=[itad_id], timeout=TIMEOUT)
    if res.status_code == 200:
        data = res.json()
        if data and isinstance(data[0].get('bundles'), list):
            return 1 if len(data[0]['bundles']) > 0 else 0
    return 0


def procesar_lote_itad(meses_atras=10, limite_juegos=200):
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT r.itad_id_texto
            FROM REL_Juego_ITAD r
            LEFT JOIN Datos_Actuales_ITAD d ON r.itad_id_texto = d.itad_id_texto
            WHERE d.itad_id_texto IS NULL
            LIMIT ?
        """, (limite_juegos,))
        juegos = [row[0] for row in cursor.fetchall()]

        if not juegos:
            print('Sin juegos pendientes.')
            return False

        print(f'{len(juegos)} juegos a procesar')
        fecha_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=meses_atras * 30)
        since_iso = fecha_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        for itad_id in juegos:
            print(f'\n[{itad_id}]')
            try:
                with ThreadPoolExecutor(max_workers=4) as ex:
                    fut_hist = ex.submit(_fetch_historial, itad_id, since_iso)
                    fut_low = ex.submit(_fetch_low, itad_id)
                    fut_actual = ex.submit(_fetch_precio_actual, itad_id)
                    fut_bundle = ex.submit(_fetch_bundle, itad_id)
                    historial = fut_hist.result()
                    precio_min, tienda_min, fecha_min = fut_low.result()
                    precio_act, tienda_act = fut_actual.result()
                    en_bundle = fut_bundle.result()

                tiendas_nuevas = {
                    (c['shop']['id'], c['shop'].get('name', 'Desconocida'))
                    for c in historial if c.get('shop', {}).get('id')
                }
                if tiendas_nuevas:
                    cursor.executemany('INSERT OR IGNORE INTO CAT_Tienda (id_tienda, nombre) VALUES (?, ?)', tiendas_nuevas)

                hist_rows = []
                for cambio in historial:
                    t_id = cambio.get('shop', {}).get('id')
                    deal = cambio.get('deal', {})
                    precio = deal.get('price', {}).get('amount')
                    cut = deal.get('cut', 0)
                    fecha_str = cambio.get('timestamp')
                    if t_id and precio and fecha_str:
                        ts = int(datetime.datetime.fromisoformat(fecha_str).timestamp())
                        hist_rows.append((itad_id, t_id, precio, cut, ts))

                if hist_rows:
                    cursor.executemany("""
                        INSERT OR IGNORE INTO Hist_Precios_ITAD
                        (itad_id_texto, id_tienda, precio, corte_descuento, fecha_unix)
                        VALUES (?, ?, ?, ?, ?)
                    """, hist_rows)

                cursor.execute("""
                    INSERT OR REPLACE INTO Datos_Actuales_ITAD
                    (itad_id_texto, precio_actual, tienda_actual, precio_minimo,
                     tienda_minimo, fecha_minimo, en_bundle, fecha_actualizacion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))
                """, (itad_id, precio_act, tienda_act, precio_min, tienda_min, fecha_min, en_bundle))

                conn.commit()
                print(f'  historial: {len(hist_rows)} | precio actual: {precio_act} | mínimo: {precio_min}')

            except Exception as e:
                print(f'  error en {itad_id}: {e}')
                conn.rollback()

            time.sleep(0.5)

        return True

    finally:
        conn.close()


def run():
    preparar_tablas_itad()

    print("=== Vinculando IDs ITAD ===")
    while True:
        conn = sqlite3.connect(DB_PATH)
        pendientes = conn.execute("""
            SELECT COUNT(*) FROM CAT_Juego j
            LEFT JOIN REL_Juego_ITAD r ON j.juego_id = r.juego_id
            WHERE j.id_steam IS NOT NULL AND r.itad_id_texto IS NULL AND j.categoria = 0
        """).fetchone()[0]
        conn.close()
        if pendientes == 0: break
        print(f'Faltan {pendientes}')
        if not buscar_ids_itad(limite=100): break
        time.sleep(30)

    print("=== Descargando precios e historial ===")
    while procesar_lote_itad(meses_atras=10, limite_juegos=200):
        time.sleep(5)

    print("=== ITAD completado ===")


if __name__ == "__main__":
    run()
