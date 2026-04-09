import os
import sqlite3
import time
import pandas as pd
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / '.env')

TWITCH_CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
DB_PATH = os.getenv('DB_PATH', 'db/gaming_warehouse.db')

_token_cache = {"token": None, "Expira": 0}


def obtener_token_igdb():
    ahora = time.time()
    if _token_cache["token"] and ahora < _token_cache["Expira"]:
        return _token_cache["token"]

    respuesta = requests.post(
        "https://id.twitch.tv/oauth2/token",
        params={
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials"
        }
    )
    respuesta.raise_for_status()
    datos = respuesta.json()
    _token_cache["token"] = datos["access_token"]
    _token_cache["Expira"] = ahora + datos.get("expires_in", 3600) - 300
    return _token_cache["token"]


def descargar_y_limpiar_juegos(paginas=1, juegos_por_pagina=500):
    def extraer_nombres(lista):
        if not isinstance(lista, list): return []
        return [item['name'] for item in lista if 'name' in item]

    def extraer_companias(lista, rol):
        if not isinstance(lista, list): return []
        return [
            item['company']['name']
            for item in lista
            if item.get(rol) and 'company' in item and 'name' in item['company']
        ]

    def contar_elementos(lista):
        return len(lista) if isinstance(lista, list) else 0

    def limpiar_cover(cover):
        if not isinstance(cover, dict) or 'url' not in cover: return None
        return f"https:{cover['url'].replace('t_thumb', 't_cover_big')}"

    token = obtener_token_igdb()
    headers = {'Client-ID': TWITCH_CLIENT_ID, 'Authorization': f'Bearer {token}'}
    igdb_url = "https://api.igdb.com/v4/games"

    juegos_crudos = []
    print(f"Iniciando descarga: {paginas} paginas.")

    for pagina in range(paginas):
        offset = pagina * juegos_por_pagina
        print(f"Pagina {pagina + 1} (Offset: {offset})...")

        query = f"""
        fields name, rating, rating_count, category, parent_game, genres.name, themes.name, 
               game_modes.name, player_perspectives.name, involved_companies.company.name, 
               involved_companies.developer, involved_companies.publisher, franchises.name, 
               first_release_date, dlcs, similar_games, keywords.name, summary, 
               storyline, cover.url, videos, platforms.name;
        where platforms = (6,48,49,130,167,169) & rating_count > 5;
        sort rating_count desc;
        limit {juegos_por_pagina};
        offset {offset};
        """

        response = requests.post(igdb_url, headers=headers, data=query)
        if response.status_code != 200:
            print(f"Error: {response.text}")
            break

        datos = response.json()
        juegos_crudos.extend(datos)
        print(f"Recibidos {len(datos)} registros.")

        if pagina < paginas - 1:
            time.sleep(5)

    if not juegos_crudos:
        return pd.DataFrame()

    df = pd.DataFrame(juegos_crudos)

    columnas_lista = ['genres', 'themes', 'game_modes', 'player_perspectives', 'franchises', 'keywords', 'platforms']
    for columna in columnas_lista:
        if columna in df.columns:
            df[f'{columna}_list'] = df[columna].apply(extraer_nombres)

    if 'involved_companies' in df.columns:
        df['developers_list'] = df['involved_companies'].apply(lambda x: extraer_companias(x, 'developer'))
        df['publishers_list'] = df['involved_companies'].apply(lambda x: extraer_companias(x, 'publisher'))

    df['dlc_count'] = df.get('dlcs', pd.Series(dtype='object')).apply(contar_elementos)
    df['video_count'] = df.get('videos', pd.Series(dtype='object')).apply(contar_elementos)
    df['cover_url'] = df.get('cover', pd.Series(dtype='object')).apply(limpiar_cover)
    df['category'] = df.get('category', 0)
    df['parent_game'] = df.get('parent_game', None)

    if 'first_release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['first_release_date'], unit='s').dt.date

    df = df.assign(
        summary=df.get('summary', pd.Series(dtype='object')).fillna('Sin datos'),
        storyline=df.get('storyline', pd.Series(dtype='object')).fillna('Sin datos'),
        rating=df.get('rating', pd.Series(dtype='float64')).fillna(0.0),
        rating_count=df.get('rating_count', pd.Series(dtype='int64')).fillna(0)
    )
    df.drop_duplicates(subset=['id'], inplace=True)
    print(f"Proceso finalizado: {len(df)} registros")
    return df


def insertar_en_base_datos(df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    columnas_principales = [
        'id', 'name', 'category', 'release_date', 'summary', 'storyline',
        'cover_url', 'rating', 'rating_count', 'dlc_count', 'video_count'
    ]
    registros_principales = df[columnas_principales].copy()

    if 'release_date' in registros_principales.columns:
        registros_principales['release_date'] = registros_principales['release_date'].astype(str)
        registros_principales.loc[registros_principales['release_date'] == 'NaT', 'release_date'] = None

    registros_principales = registros_principales.astype(object).where(pd.notna(registros_principales), None)

    query_principal = """
        INSERT OR IGNORE INTO CAT_Juego
        (id_igdb, titulo, categoria, fecha_lanzamiento, resumen, historia, url_portada, puntuacion_igdb, conteo_votos_igdb, conteo_dlc, conteo_videos)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(query_principal, list(registros_principales.itertuples(index=False, name=None)))
    conn.commit()

    mapa_ids = dict(cursor.execute("SELECT id_igdb, juego_id FROM CAT_Juego").fetchall())

    def procesar_relacion(columna_df, tabla_cat, tabla_rel, col_id_cat):
        df_explod = df[['id', columna_df]].explode(columna_df).dropna(subset=[columna_df])
        if df_explod.empty: return
        valores_unicos = df_explod[columna_df].unique()
        cursor.executemany(f"INSERT OR IGNORE INTO {tabla_cat} (nombre) VALUES (?)", [(v,) for v in valores_unicos])
        mapa_cat = dict(cursor.execute(f"SELECT nombre, {col_id_cat} FROM {tabla_cat}").fetchall())
        puentes = []
        for _, fila in df_explod.iterrows():
            j_id = mapa_ids.get(fila['id'])
            c_id = mapa_cat.get(fila[columna_df])
            if j_id and c_id:
                puentes.append((j_id, c_id))
        cursor.executemany(f"INSERT OR IGNORE INTO {tabla_rel} (juego_id, {col_id_cat}) VALUES (?, ?)", puentes)

    mapa_relaciones = [
        ('genres_list', 'CAT_Genero', 'REL_Juego_Genero', 'genero_id'),
        ('themes_list', 'CAT_Tematica', 'REL_Juego_Tematica', 'tematica_id'),
        ('game_modes_list', 'CAT_Modo_Juego', 'REL_Juego_Modo', 'modo_id'),
        ('player_perspectives_list', 'CAT_Perspectiva', 'REL_Juego_Perspectiva', 'perspectiva_id'),
        ('franchises_list', 'CAT_Franquicia', 'REL_Juego_Franquicia', 'franquicia_id'),
        ('keywords_list', 'CAT_Etiqueta', 'REL_Juego_Etiqueta', 'etiqueta_id'),
        ('platforms_list', 'CAT_Plataforma', 'REL_Juego_Plataforma', 'plataforma_id'),
        ('developers_list', 'CAT_Empresa', 'REL_Juego_Desarrollador', 'empresa_id'),
        ('publishers_list', 'CAT_Empresa', 'REL_Juego_Editor', 'empresa_id'),
    ]

    for col, t_cat, t_rel, pk in mapa_relaciones:
        if col in df.columns:
            procesar_relacion(col, t_cat, t_rel, pk)

    if 'parent_game' in df.columns:
        df_dlc = df[['id', 'parent_game']].dropna()
        relaciones_dlc = []
        for _, fila in df_dlc.iterrows():
            id_principal = mapa_ids.get(fila['parent_game'])
            id_dlc = mapa_ids.get(fila['id'])
            if id_principal and id_dlc:
                relaciones_dlc.append((id_principal, id_dlc))
        cursor.executemany("INSERT OR IGNORE INTO REL_Juego_DLC (juego_id_principal, juego_id_dlc) VALUES (?, ?)", relaciones_dlc)

    if 'similar_games' in df.columns:
        df_sim = df[['id', 'similar_games']].explode('similar_games').dropna()
        puentes_sim = [(mapa_ids.get(f['id']), f['similar_games']) for _, f in df_sim.iterrows() if mapa_ids.get(f['id'])]
        cursor.executemany("INSERT OR IGNORE INTO REL_Juegos_Similares (juego_id, id_igdb_similar) VALUES (?, ?)", puentes_sim)

    conn.commit()
    conn.close()
    print("Carga completada.")


def run(paginas=30):
    df = descargar_y_limpiar_juegos(paginas=paginas)
    if not df.empty:
        insertar_en_base_datos(df)


if __name__ == "__main__":
    run()
