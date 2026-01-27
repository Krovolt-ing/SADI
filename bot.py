import requests
import pandas as pd
import os
from datetime import datetime
import numpy as np

FILE_NAME = "sadi_historico.csv"
BASE_URL = "https://api.cammesa.com/demanda-svc/"

ENDPOINTS = {
    "sadi": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1002",
    "gba": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=426",
    "pba_int": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=425",
    "edenor": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1077",
    "edesur": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1078",
    "edelap": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1943",
    "gen": "generacion/ObtieneGeneracioEnergiaPorRegion?id_region=1002"
}

def clean_val(val, default=0):
    """Convierte NaN o None a un valor por defecto (0) y asegura que sea int/float."""
    try:
        if pd.isna(val) or val is None:
            return default
        return val
    except:
        return default

def fetch(key):
    try:
        url = BASE_URL + ENDPOINTS[key]
        res = requests.get(url, timeout=20)
        return res.json()
    except:
        return None

def actualizar_csv():
    print(f"Iniciando captura masiva: {datetime.now()}")
    
    data_raw = {k: fetch(k) for k in ENDPOINTS.keys()}
    
    if not data_raw["sadi"]:
        print("Fallo crítico: No hay datos de SADI.")
        return

    df_sadi = pd.DataFrame(data_raw["sadi"]).dropna(subset=['demHoy'])
    if df_sadi.empty: return
    
    ultimo_sadi = df_sadi.iloc[-1]
    fecha_key = pd.to_datetime(ultimo_sadi['fecha']).strftime("%Y-%m-%d %H:%M:%S")

    # 2. Construir la Fila con limpieza de NaNs (AQUÍ ESTÁ LA SOLUCIÓN)
    nueva_fila = {
        "fecha": fecha_key,
        "sadi_dem_hoy": int(clean_val(ultimo_sadi.get('demHoy'))),
        "sadi_dem_ayer": int(clean_val(ultimo_sadi.get('demAyer'))),
        "sadi_dem_sem_ant": int(clean_val(ultimo_sadi.get('demSemanaAnt'))),
        "sadi_dem_prevista": int(clean_val(ultimo_sadi.get('demPrevista'))),
        "sadi_temp_hoy": float(clean_val(ultimo_sadi.get('tempHoy'))),
        "sadi_temp_ayer": float(clean_val(ultimo_sadi.get('tempAyer'))),
        "sadi_temp_sem_ant": float(clean_val(ultimo_sadi.get('tempSemanaAnt')))
    }

    # Regionales
    for reg in ["gba", "pba_int", "edenor", "edesur", "edelap"]:
        if data_raw[reg]:
            df_reg = pd.DataFrame(data_raw[reg]).dropna(subset=['demHoy'])
            val = int(df_reg.iloc[-1]['demHoy']) if not df_reg.empty else 0
            nueva_fila[f"{reg}_demanda"] = val
        else:
            nueva_fila[f"{reg}_demanda"] = 0

    # Generación
    if data_raw["gen"]:
        df_gen = pd.DataFrame(data_raw["gen"])
        u_gen = df_gen.iloc[-1]
        nueva_fila.update({
            "gen_total": int(clean_val(u_gen.get('sumTotal'))),
            "gen_nuclear": int(clean_val(u_gen.get('nuclear'))),
            "gen_renovable": int(clean_val(u_gen.get('renovable'))),
            "gen_hidraulico": int(clean_val(u_gen.get('hidraulico'))),
            "gen_termico": int(clean_val(u_gen.get('termico'))),
            "gen_importacion": int(clean_val(u_gen.get('importacion')))
        })

    # 3. Guardar o Inicializar CSV
    # Si vas a cambiar las columnas, recordá borrar el archivo viejo manualmente primero
    if not os.path.isfile(FILE_NAME):
        pd.DataFrame([nueva_fila]).to_csv(FILE_NAME, index=False)
        print("Archivo histórico reiniciado con todas las columnas.")
    else:
        df_hist = pd.read_csv(FILE_NAME)
        if fecha_key not in df_hist['fecha'].values:
            # Aseguramos que el nuevo dato tenga todas las columnas de la tabla
            df_nueva_fila = pd.DataFrame([nueva_fila])
            df_hist = pd.concat([df_hist, df_nueva_fila], ignore_index=True)
            df_hist.to_csv(FILE_NAME, index=False)
            print(f"✅ Registro completo guardado para {fecha_key}")
        else:
            print("El registro ya existe.")

if __name__ == "__main__":
    actualizar_csv()
