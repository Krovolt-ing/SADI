import requests
import pandas as pd
import os
from datetime import datetime
import numpy as np

FILE_NAME = "sadi_historico.csv"
BASE_URL = "https://api.cammesa.com/demanda-svc/"

REGIONES_DEM = {
    "SADI": 1002, "NEA": 418, "NOA": 419, "GBA": 426, "Centro": 422, 
    "Patagonia": 111, "Litoral": 417, "Comahue": 420, "Provincia_BSAS": 425, 
    "Cuyo": 429, "Edenor": 1077, "Edesur": 1078, "Edelap": 1943
}
REGIONES_GEN = {
    "SADI": 1002, "NEA": 418, "NOA": 419, "GBA": 426, "Centro": 422, 
    "Patagonia": 111, "Litoral": 417, "Comahue": 420, "Provincia_BSAS": 425, "Cuyo": 429
}

def clean_val(val, default=0):
    try:
        if pd.isna(val) or val is None: return default
        return val
    except: return default

def fetch(path, id_reg):
    try:
        url = f"{BASE_URL}{path}?id_region={id_reg}"
        res = requests.get(url, timeout=25)
        return res.json()
    except: return None

def actualizar_csv():
    print(f"--- Iniciando Auditoría Histórica: {datetime.now()} ---")
    
    raw_sadi = fetch("demanda/ObtieneDemandaYTemperaturaRegion", 1002)
    if not raw_sadi: return

    df_final = pd.DataFrame(raw_sadi).dropna(subset=['demHoy'])
    df_final['fecha'] = pd.to_datetime(df_final['fecha']).dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Columnas base del SADI
    df_final = df_final[['fecha', 'demHoy', 'demAyer', 'demSemanaAnt', 'demPrevista', 'tempHoy', 'tempAyer', 'tempSemanaAnt']]
    df_final.columns = ['fecha', 'sadi_dem_hoy', 'sadi_dem_ayer', 'sadi_dem_sem_ant', 'sadi_dem_prevista', 'sadi_temp_hoy', 'sadi_temp_ayer', 'sadi_temp_sem_ant']

    # Integrar Demandas Regionales (Usando reindex para evitar el KeyError)
    for nombre, id_reg in REGIONES_DEM.items():
        if nombre == "SADI": continue
        raw = fetch("demanda/ObtieneDemandaYTemperaturaRegion", id_reg)
        if raw:
            df_reg = pd.DataFrame(raw)
            # Reindex asegura que si 'tempHoy' no existe, la crea con NaNs en lugar de dar error
            df_reg = df_reg.reindex(columns=['fecha', 'demHoy', 'tempHoy'])
            df_reg['fecha'] = pd.to_datetime(df_reg['fecha']).dt.strftime("%Y-%m-%d %H:%M:%S")
            df_reg.columns = ['fecha', f'dem_{nombre.lower()}_hoy', f'temp_{nombre.lower()}_hoy']
            df_final = pd.merge(df_final, df_reg, on='fecha', how='left')

    # Integrar Generación Regional
    for nombre, id_reg in REGIONES_GEN.items():
        raw = fetch("generacion/ObtieneGeneracioEnergiaPorRegion", id_reg)
        if raw:
            df_gen = pd.DataFrame(raw)
            df_gen = df_gen.reindex(columns=['fecha', 'sumTotal', 'nuclear', 'renovable', 'hidraulico', 'termico', 'importacion'])
            df_gen['fecha'] = pd.to_datetime(df_gen['fecha']).dt.strftime("%Y-%m-%d %H:%M:%S")
            pfx = f"gen_{nombre.lower()}"
            df_gen.columns = ['fecha', f'{pfx}_total', f'{pfx}_nuclear', f'{pfx}_renovable', f'{pfx}_hidraulico', f'{pfx}_termico', f'{pfx}_importacion']
            df_final = pd.merge(df_final, df_gen, on='fecha', how='left')

    df_final = df_final.fillna(0)

    if os.path.isfile(FILE_NAME):
        df_old = pd.read_csv(FILE_NAME)
        df_new_points = df_final[~df_final['fecha'].isin(df_old['fecha'])]
        if not df_new_points.empty:
            df_updated = pd.concat([df_old, df_new_points], ignore_index=True).sort_values('fecha')
            df_updated.to_csv(FILE_NAME, index=False)
            print(f"✅ Se rellenaron {len(df_new_points)} puntos.")
        else:
            print("☕ No hay datos nuevos.")
    else:
        df_final.to_csv(FILE_NAME, index=False)
        print("📁 Archivo inicializado.")

if __name__ == "__main__":
    actualizar_csv()
