import requests
import pandas as pd
import os
from datetime import datetime

FILE_NAME = "sadi_historico.csv"
BASE_URL = "https://api.cammesa.com/demanda-svc/"

# 1. LISTADO DE REGIONES PARA DEMANDA (Extraído de tu JSON)
REGIONES_DEMANDA = {
    "NEA": 418, "NOA": 419, "GBA": 426, "Centro": 422, "Patagonia": 111,
    "Litoral": 417, "Comahue": 420, "Provincia_BSAS": 425, "Cuyo": 429,
    "Edenor": 1077, "Edesur": 1078, "Edelap": 1943, "Santa_Fe": 2540,
    "Misiones": 2426, "Entre_Rios": 2541, "Corrientes": 1893, "Chaco": 1892,
    "Formosa": 1886, "Jujuy": 1937, "Salta": 1933, "Tucuman": 1936,
    "Catamarca": 1938, "Santiago_del_Estero": 1905, "San_Luis": 1944,
    "La_Rioja": 1910, "Cordoba": 1945, "San_Juan": 1922, "Mendoza": 1946,
    "Rio_Negro": 2525, "La_Pampa": 427, "Chubut": 2543, "Santa_Cruz": 2542,
    "Tierra_del_Fuego": 23, "Neuquen": 2528
}

# 2. LISTADO DE REGIONES PARA GENERACIÓN (Solo las que permiten el endpoint)
REGIONES_GENERACION = {
    "NEA": 418, "NOA": 419, "GBA": 426, "Centro": 422, "Patagonia": 111,
    "Litoral": 417, "Comahue": 420, "Provincia_BSAS": 425, "Cuyo": 429
}

def clean_val(val, default=0):
    try:
        if pd.isna(val) or val is None: return default
        return val
    except: return default

def fetch_data(endpoint_type, id_reg):
    """endpoint_type puede ser 'demanda' o 'generacion'"""
    try:
        if endpoint_type == "demanda":
            url = f"{BASE_URL}demanda/ObtieneDemandaYTemperaturaRegion?id_region={id_reg}"
        else:
            url = f"{BASE_URL}generacion/ObtieneGeneracioEnergiaPorRegion?id_region={id_reg}"
        
        res = requests.get(url, timeout=15)
        return res.json()
    except:
        return None

def actualizar_csv():
    print(f"Iniciando captura masiva histórica: {datetime.now()}")
    
    # --- A. OBTENER SADI COMO MASTER TIMESTAMP ---
    raw_sadi = fetch_data("demanda", 1002)
    if not raw_sadi:
        print("Error: No se pudo obtener el dato maestro del SADI.")
        return

    df_sadi = pd.DataFrame(raw_sadi).dropna(subset=['demHoy'])
    if df_sadi.empty: return
    
    u_sadi = df_sadi.iloc[-1]
    fecha_key = pd.to_datetime(u_sadi['fecha']).strftime("%Y-%m-%d %H:%M:%S")

    # --- B. CONSTRUIR FILA DE DATOS ---
    nueva_fila = {
        "fecha": fecha_key,
        # Datos Core SADI
        "sadi_dem_hoy": int(clean_val(u_sadi.get('demHoy'))),
        "sadi_dem_ayer": int(clean_val(u_sadi.get('demAyer'))),
        "sadi_dem_sem_ant": int(clean_val(u_sadi.get('demSemanaAnt'))),
        "sadi_dem_prevista": int(clean_val(u_sadi.get('demPrevista'))),
        "sadi_temp_hoy": float(clean_val(u_sadi.get('tempHoy'))),
        "sadi_temp_ayer": float(clean_val(u_sadi.get('tempAyer'))),
        "sadi_temp_sem_ant": float(clean_val(u_sadi.get('tempSemanaAnt')))
    }

    # --- C. RECOLECTAR DEMANDA REGIONAL / PROVINCIAL ---
    print("Recolectando demandas regionales...")
    for nombre, id_reg in REGIONES_DEMANDA.items():
        raw = fetch_data("demanda", id_reg)
        if raw:
            df_reg = pd.DataFrame(raw).dropna(subset=['demHoy'])
            if not df_reg.empty:
                last = df_reg.iloc[-1]
                nueva_fila[f"dem_{nombre.lower()}_hoy"] = int(clean_val(last.get('demHoy')))
                nueva_fila[f"dem_{nombre.lower()}_ayer"] = int(clean_val(last.get('demAyer')))
                nueva_fila[f"dem_{nombre.lower()}_sem_ant"] = int(clean_val(last.get('demSemanaAnt')))
                nueva_fila[f"temp_{nombre.lower()}_hoy"] = float(clean_val(last.get('tempHoy')))

    # --- D. RECOLECTAR GENERACIÓN REGIONAL ---
    print("Recolectando matrices de generación...")
    for nombre, id_reg in REGIONES_GENERACION.items():
        raw = fetch_data("generacion", id_reg)
        if raw:
            df_gen = pd.DataFrame(raw)
            if not df_gen.empty:
                last = df_gen.iloc[-1]
                pfx = f"gen_{nombre.lower()}"
                nueva_fila.update({
                    f"{pfx}_total": int(clean_val(last.get('sumTotal'))),
                    f"{pfx}_nuclear": int(clean_val(last.get('nuclear'))),
                    f"{pfx}_renovable": int(clean_val(last.get('renovable'))),
                    f"{pfx}_hidraulico": int(clean_val(last.get('hidraulico'))),
                    f"{pfx}_termico": int(clean_val(last.get('termico'))),
                    f"{pfx}_importacion": int(clean_val(last.get('importacion')))
                })

    # --- E. GUARDAR ---
    if not os.path.isfile(FILE_NAME):
        pd.DataFrame([nueva_fila]).to_csv(FILE_NAME, index=False)
        print("✅ Base de datos histórica inicializada con éxito.")
    else:
        df_hist = pd.read_csv(FILE_NAME)
        if fecha_key not in df_hist['fecha'].values:
            df_hist = pd.concat([df_hist, pd.DataFrame([nueva_fila])], ignore_index=True)
            df_hist.to_csv(FILE_NAME, index=False)
            print(f"✅ Registro masivo guardado para {fecha_key}")
        else:
            print("Dato ya existente en el archivo.")

if __name__ == "__main__":
    actualizar_csv()
