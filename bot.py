import requests
import pandas as pd
import os
from datetime import datetime

FILE_NAME = "sadi_historico.csv"
BASE_URL = "https://api.cammesa.com/demanda-svc/"

# Diccionario de Endpoints
ENDPOINTS = {
    "sadi": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1002",
    "gba": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=426",
    "pba_int": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=425",
    "edenor": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1077",
    "edesur": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1078",
    "edelap": "demanda/ObtieneDemandaYTemperaturaRegion?id_region=1943",
    "gen": "generacion/ObtieneGeneracioEnergiaPorRegion?id_region=1002"
}

def fetch(key):
    try:
        url = BASE_URL + ENDPOINTS[key]
        res = requests.get(url, timeout=20)
        return res.json()
    except:
        return None

def actualizar_csv():
    print(f"Iniciando captura masiva: {datetime.now()}")
    
    # 1. Obtener datos de todos los canales
    data_raw = {k: fetch(k) for k in ENDPOINTS.keys()}
    
    # Validar SADI (nuestra referencia de tiempo)
    if not data_raw["sadi"]:
        print("Fallo crítico: No hay datos de SADI.")
        return

    df_sadi = pd.DataFrame(data_raw["sadi"]).dropna(subset=['demHoy'])
    if df_sadi.empty: return
    
    ultimo_sadi = df_sadi.iloc[-1]
    fecha_key = pd.to_datetime(ultimo_sadi['fecha']).strftime("%Y-%m-%d %H:%M:%S")

    # 2. Construir la Gran Fila de Datos
    nueva_fila = {
        "fecha": fecha_key,
        # SADI Demanda
        "sadi_dem_hoy": int(ultimo_sadi['demHoy']),
        "sadi_dem_ayer": int(ultimo_sadi.get('demAyer', 0) or 0),
        "sadi_dem_sem_ant": int(ultimo_sadi.get('demSemanaAnt', 0) or 0),
        "sadi_dem_prevista": int(ultimo_sadi.get('demPrevista', 0) or 0),
        # SADI Temperatura
        "sadi_temp_hoy": float(ultimo_sadi.get('tempHoy', 0) or 0),
        "sadi_temp_ayer": float(ultimo_sadi.get('tempAyer', 0) or 0),
        "sadi_temp_sem_ant": float(ultimo_sadi.get('tempSemanaAnt', 0) or 0)
    }

    # Regionales (Último valor disponible)
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
            "gen_total": int(u_gen.get('sumTotal', 0)),
            "gen_nuclear": int(u_gen.get('nuclear', 0)),
            "gen_renovable": int(u_gen.get('renovable', 0)),
            "gen_hidraulico": int(u_gen.get('hidraulico', 0)),
            "gen_termico": int(u_gen.get('termico', 0)),
            "gen_importacion": int(u_gen.get('importacion', 0))
        })

    # 3. Guardar o Inicializar CSV
    if not os.path.isfile(FILE_NAME):
        pd.DataFrame([nueva_fila]).to_csv(FILE_NAME, index=False)
        print("Archivo histórico reiniciado con todas las columnas.")
    else:
        df_hist = pd.read_csv(FILE_NAME)
        # Evitar duplicados por fecha
        if nueva_fila['fecha'] not in df_hist['fecha'].values:
            df_hist = pd.concat([df_hist, pd.DataFrame([nueva_fila])], ignore_index=True)
            df_hist.to_csv(FILE_NAME, index=False)
            print(f"✅ Registro completo guardado para {fecha_key}")
        else:
            print("El registro ya existe.")

if __name__ == "__main__":
    actualizar_csv()
