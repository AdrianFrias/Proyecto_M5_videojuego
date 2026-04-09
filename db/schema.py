import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / '.env')

DB_PATH = os.getenv('DB_PATH', 'db/gaming_warehouse.db')


def inicializar_base_datos():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    cursor.execute("PRAGMA journal_mode = WAL;")

    esquema_sql = """
    CREATE TABLE IF NOT EXISTS CAT_Juego (
        juego_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_igdb INTEGER UNIQUE,
        id_steam INTEGER UNIQUE,
        titulo TEXT NOT NULL,
        categoria INTEGER,
        fecha_lanzamiento DATE,
        resumen TEXT,
        historia TEXT,
        url_portada TEXT,
        puntuacion_igdb REAL,
        conteo_votos_igdb INTEGER,
        conteo_dlc INTEGER,
        conteo_videos INTEGER,
        hltb_historia_principal REAL,
        hltb_historia_extra REAL,
        hltb_completacionista REAL
    );

    CREATE TABLE IF NOT EXISTS REL_Juego_DLC (
        juego_id_principal INTEGER,
        juego_id_dlc INTEGER,
        PRIMARY KEY (juego_id_principal, juego_id_dlc),
        FOREIGN KEY (juego_id_principal) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE,
        FOREIGN KEY (juego_id_dlc) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS CAT_Genero (genero_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS CAT_Tematica (tematica_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS CAT_Modo_Juego (modo_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS CAT_Perspectiva (perspectiva_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS CAT_Empresa (empresa_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS CAT_Franquicia (franquicia_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS CAT_Etiqueta (etiqueta_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS CAT_Plataforma (plataforma_id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE);

    CREATE TABLE IF NOT EXISTS REL_Juego_Genero (juego_id INTEGER, genero_id INTEGER, PRIMARY KEY (juego_id, genero_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (genero_id) REFERENCES CAT_Genero(genero_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Tematica (juego_id INTEGER, tematica_id INTEGER, PRIMARY KEY (juego_id, tematica_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (tematica_id) REFERENCES CAT_Tematica(tematica_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Modo (juego_id INTEGER, modo_id INTEGER, PRIMARY KEY (juego_id, modo_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (modo_id) REFERENCES CAT_Modo_Juego(modo_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Perspectiva (juego_id INTEGER, perspectiva_id INTEGER, PRIMARY KEY (juego_id, perspectiva_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (perspectiva_id) REFERENCES CAT_Perspectiva(perspectiva_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Desarrollador (juego_id INTEGER, empresa_id INTEGER, PRIMARY KEY (juego_id, empresa_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (empresa_id) REFERENCES CAT_Empresa(empresa_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Editor (juego_id INTEGER, empresa_id INTEGER, PRIMARY KEY (juego_id, empresa_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (empresa_id) REFERENCES CAT_Empresa(empresa_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Franquicia (juego_id INTEGER, franquicia_id INTEGER, PRIMARY KEY (juego_id, franquicia_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (franquicia_id) REFERENCES CAT_Franquicia(franquicia_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Etiqueta (juego_id INTEGER, etiqueta_id INTEGER, PRIMARY KEY (juego_id, etiqueta_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (etiqueta_id) REFERENCES CAT_Etiqueta(etiqueta_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juego_Plataforma (juego_id INTEGER, plataforma_id INTEGER, PRIMARY KEY (juego_id, plataforma_id), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE, FOREIGN KEY (plataforma_id) REFERENCES CAT_Plataforma(plataforma_id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS REL_Juegos_Similares (juego_id INTEGER, id_igdb_similar INTEGER, PRIMARY KEY (juego_id, id_igdb_similar), FOREIGN KEY (juego_id) REFERENCES CAT_Juego(juego_id) ON DELETE CASCADE);
    """

    cursor.executescript(esquema_sql)
    conn.commit()
    conn.close()
    print("Base de datos inicializada.")


if __name__ == "__main__":
    inicializar_base_datos()
