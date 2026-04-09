import os
import sqlite3
import time
from howlongtobeatpy import HowLongToBeat

DB_PATH = os.getenv('DB_PATH', 'db/gaming_warehouse.db')


def buscar_hltb(titulo):
    try:
        resultados = HowLongToBeat().search(titulo)
        if not resultados:
            return None
        juego = max(resultados, key=lambda x: x.similarity)
        if juego.similarity < 0.6:
            return None
        return {
            "historia_principal": juego.main_story or None,
            "historia_extra": juego.main_extra or None,
            "completacionista": juego.completionist or None,
        }
    except Exception:
        return None


def llenar_hltb(delay=1.5):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT juego_id, titulo FROM CAT_Juego WHERE hltb_historia_principal IS NULL")
    juegos = cur.fetchall()
    conn.close()

    print(f"Juegos a procesar: {len(juegos)}")
    for i, (juego_id, titulo) in enumerate(juegos, 1):
        datos = buscar_hltb(titulo)
        conn = sqlite3.connect(DB_PATH)
        if datos:
            conn.execute("""
                UPDATE CAT_Juego
                SET hltb_historia_principal = ?,
                    hltb_historia_extra     = ?,
                    hltb_completacionista   = ?
                WHERE juego_id = ?
            """, (datos["historia_principal"], datos["historia_extra"], datos["completacionista"], juego_id))
            conn.commit()
            print(f"[{i}] OK: {titulo}")
        else:
            print(f"[{i}] Sin datos: {titulo}")
        conn.close()
        time.sleep(delay)


def run():
    llenar_hltb()
    print("=== HLTB completado ===")


if __name__ == "__main__":
    run()
