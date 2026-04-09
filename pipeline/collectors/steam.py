import os
import re
import time
import json
import sqlite3
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from thefuzz import fuzz

load_dotenv(Path(__file__).resolve().parents[2] / '.env')

STEAM_KEY = os.getenv('STEAM_API_KEY')
DB_PATH = os.getenv('DB_PATH', 'db/gaming_warehouse.db')


def normalizar_nombre(nombre):
    if not isinstance(nombre, str): return ""
    nombre = nombre.lower().strip()
    nombre = nombre.replace('™', '').replace('®', '').replace('©', '')
    nombre = re.sub(r'\s+', ' ', nombre)
    return nombre


def estandarizar_romanos(texto):
    romanos = {
        ' i ': ' 1 ', ' ii ': ' 2 ', ' iii ': ' 3 ', ' iv ': ' 4 ',
        ' v ': ' 5 ', ' vi ': ' 6 ', ' vii ': ' 7 ', ' viii ': ' 8 ',
        ' ix ': ' 9 ', ' x ': ' 10 '
    }
    texto_espaciado = f" {texto} "
    for rom, arab in romanos.items():
        texto_espaciado = texto_espaciado.replace(rom, arab)
    return texto_espaciado.strip()


def limpieza_letras(nombre):
    if not isinstance(nombre, str): return ""
    nombre = nombre.lower()
    nombre = re.sub(r'[™®©]', '', nombre)
    nombre = re.sub(r'[^\w\s]', ' ', nombre)
    nombre = re.sub(r'\s+', ' ', nombre).strip()
    nombre = estandarizar_romanos(nombre)
    return nombre


def descargar_catalogo_steam():
    steam_apps = []
    last_appid = 0

    while True:
        url = f"https://api.steampowered.com/IStoreService/GetAppList/v1/?key={STEAM_KEY}&max_results=50000&last_appid={last_appid}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get('response', {})
            apps = data.get('apps', [])
            if not apps: break
            steam_apps.extend(apps)
            print(f"Descargando... {len(steam_apps)} registros.")
            if data.get('have_more_results'):
                last_appid = data.get('last_appid')
                time.sleep(2)
            else:
                break
        else:
            print(f"Error HTTP {response.status_code}")
            break

    return steam_apps


def vincular_ids_steam(steam_apps):
    diccionario_steam = {}
    for app in steam_apps:
        n_limpio = normalizar_nombre(app['name'])
        if n_limpio:
            diccionario_steam.setdefault(n_limpio, []).append(app['appid'])

    with sqlite3.connect(DB_PATH) as conn:
        df_db = pd.read_sql_query("SELECT juego_id, titulo, categoria FROM CAT_Juego WHERE id_steam IS NULL", conn)
        actualizaciones = []
        for _, fila in df_db.iterrows():
            t_limpio = normalizar_nombre(fila['titulo'])
            if t_limpio in diccionario_steam:
                steam_id = min(diccionario_steam[t_limpio])
                actualizaciones.append((steam_id, fila['juego_id']))

        if actualizaciones:
            cursor = conn.cursor()
            cursor.executemany("UPDATE OR IGNORE CAT_Juego SET id_steam = ? WHERE juego_id = ?", actualizaciones)
            conn.commit()
            print(f"Primera pasada: {cursor.rowcount} IDs vinculados.")

    return diccionario_steam


def vincular_ids_fuzzy(steam_apps):
    lista_steam_limpia = [
        {'appid': app['appid'], 'nombre_limpio': limpieza_letras(app.get('name', '')), 'nombre_original': app['name']}
        for app in steam_apps
        if limpieza_letras(app.get('name', ''))
    ]

    with sqlite3.connect(DB_PATH) as conn:
        df_huerfanos = pd.read_sql_query("SELECT juego_id, titulo FROM CAT_Juego WHERE id_steam IS NULL", conn)
        total = len(df_huerfanos)
        print(f"Segunda pasada fuzzy: {total} huerfanos")

        actualizaciones = []
        for contador, (_, fila) in enumerate(df_huerfanos.iterrows(), 1):
            if contador % 500 == 0:
                print(f"Progreso: {contador}/{total}")

            titulo_igdb = limpieza_letras(fila['titulo'])
            if len(titulo_igdb) < 3: continue

            mejor_appid, mejor_score = None, 0
            for steam_app in lista_steam_limpia:
                score_set = fuzz.token_set_ratio(titulo_igdb, steam_app['nombre_limpio'])
                if score_set > mejor_score:
                    score_final = score_set
                    score_sort = fuzz.token_sort_ratio(titulo_igdb, steam_app['nombre_limpio'])
                    if score_set >= 90 and score_sort < 65:
                        score_final -= 25
                    mejor_score = score_final
                    mejor_appid = steam_app['appid']
                if mejor_score == 100: break

            if mejor_score >= 90:
                actualizaciones.append((mejor_appid, fila['juego_id']))

        if actualizaciones:
            cursor = conn.cursor()
            cursor.executemany("UPDATE OR IGNORE CAT_Juego SET id_steam = ? WHERE juego_id = ?", actualizaciones)
            conn.commit()
            print(f"Fuzzy: {cursor.rowcount} IDs adicionales vinculados.")


def descargar_detalles_steam(limite_juegos=200):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    columnas_steam = [
        ("steam_price_initial", "REAL"), ("steam_price_final", "REAL"),
        ("steam_discount_percent", "INTEGER"), ("metacritic_score", "INTEGER"),
        ("recommendations_count", "INTEGER"), ("achievements_count", "INTEGER"),
        ("steam_languages", "TEXT"), ("pc_requirements_json", "TEXT")
    ]
    for col, tipo in columnas_steam:
        try:
            cursor.execute(f"ALTER TABLE CAT_Juego ADD COLUMN {col} {tipo};")
        except sqlite3.OperationalError:
            pass

    cursor.execute("""
        SELECT juego_id, id_steam, titulo 
        FROM CAT_Juego 
        WHERE id_steam IS NOT NULL AND steam_price_final IS NULL 
        LIMIT ?
    """, (limite_juegos,))
    juegos = cursor.fetchall()
    conn.close()

    if not juegos:
        print("No hay juegos pendientes.")
        return False

    print(f"Procesando {len(juegos)} juegos...")
    for juego_id, appid, titulo in juegos:
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=spanish"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                if data and str(appid) in data and data[str(appid)]['success']:
                    info = data[str(appid)].get('data', {})
                    price_info = info.get('price_overview', {})
                    cur.execute("""
                        UPDATE CAT_Juego SET 
                            steam_price_initial = ?, steam_price_final = ?, steam_discount_percent = ?,
                            metacritic_score = ?, recommendations_count = ?, achievements_count = ?,
                            steam_languages = ?, pc_requirements_json = ?
                        WHERE juego_id = ?
                    """, (
                        price_info.get('initial', 0) / 100.0,
                        price_info.get('final', 0) / 100.0,
                        price_info.get('discount_percent', 0),
                        info.get('metacritic', {}).get('score'),
                        info.get('recommendations', {}).get('total', 0),
                        info.get('achievements', {}).get('total', 0),
                        info.get('supported_languages'),
                        json.dumps(info.get('pc_requirements', {})),
                        juego_id
                    ))
                    print(f"OK: {titulo}")
                else:
                    cur.execute("UPDATE CAT_Juego SET steam_price_final = -1 WHERE juego_id = ?", (juego_id,))
                conn.commit()
                conn.close()
            elif response.status_code == 429:
                print("Rate limit. Esperando 60s...")
                time.sleep(60)
                continue
        except Exception as e:
            print(f"Error en {titulo}: {e}")
        time.sleep(1)

    return True


def run():
    if not STEAM_KEY:
        print("ERROR: STEAM_API_KEY no configurada.")
        return

    print("=== Descargando catálogo Steam ===")
    steam_apps = descargar_catalogo_steam()

    print("=== Vinculando IDs (exacto) ===")
    vincular_ids_steam(steam_apps)

    print("=== Vinculando IDs (fuzzy) ===")
    vincular_ids_fuzzy(steam_apps)

    print("=== Descargando detalles ===")
    while descargar_detalles_steam(limite_juegos=200):
        print("Pausa 60s...")
        time.sleep(60)

    print("=== Steam completado ===")


if __name__ == "__main__":
    run()
