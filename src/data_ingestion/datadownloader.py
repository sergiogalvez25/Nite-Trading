import os
import io
import requests
import zipfile
import pandas as pd
from datetime import datetime

# --- CONFIGURACIÓN DE NITE TRADING ---
# Monedas que vamos a analizar inicialmente
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"] 
INTERVAL = "5m" 
# Vamos a bajar el historial de 2024 para las pruebas
YEARS = [2023,2024] 
MONTHS = list(range(1,13))

# Gestión de rutas dinámicas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "descargados")

# Aseguramos que la carpeta de destino exista
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def download_binance_monthly_data(symbol: str, interval: str, year: int, month: int) -> pd.DataFrame:
    """
    Descarga datos de Binance Vision, los optimiza para 16GB de RAM
    e incluye métricas de Order Flow (Taker Buy Vol y Number of Trades).
    """
    month_str = f"{month:02d}"
    url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month_str}.zip"
    
    try:
        response = requests.get(url, timeout=20)
        
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_filename = z.namelist()[0]
                
                with z.open(csv_filename) as f:
                    # Definición de todas las columnas que vienen en el CSV original
                    columns = [
                        'timestamp', 'open', 'high', 'low', 'close', 'volume',
                        'close_time', 'quote_asset_volume', 'number_of_trades',
                        'taker_buy_base', 'taker_buy_quote', 'ignore'
                    ]
                    
                    df = pd.read_csv(f, names=columns)
                    
                    # 1. Selección de columnas estratégicas (Order Flow incluido)
                    selected_cols = [
                        'timestamp', 'open', 'high', 'low', 'close', 
                        'volume', 'number_of_trades', 'taker_buy_base'
                    ]
                    df = df[selected_cols]
                    
                    # 2. Optimización de Memoria (Downcasting)
                    # Precios y Volúmenes a float32 (decimales ligeros)
                    float_cols = ['open', 'high', 'low', 'close', 'volume', 'taker_buy_base']
                    for col in float_cols:
                        df[col] = df[col].astype('float32')
                    
                    # Número de trades a int32 (enteros sin decimales)
                    df['number_of_trades'] = df['number_of_trades'].astype('int32')
                    
                    # 3. Transformación de tiempo
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    
                    return df
        else:
            # Silenciamos los 404 para meses donde la moneda no existía
            return pd.DataFrame()
            
    except Exception as e:
        print(f"\n[!] Error descargando {symbol} {year}-{month}: {e}")
        return pd.DataFrame()

def main():
    print("--- 🌙 NITE TRADING: DATA INGESTION ENGINE ---")
    start_time = datetime.now()
    
    for symbol in SYMBOLS:
        print(f"\n📦 Procesando activo: {symbol}")
        all_months_df = []
        
        for year in YEARS:
            for month in MONTHS:
                print(f"   > Descargando {year}-{month:02d}...", end="\r")
                df_month = download_binance_monthly_data(symbol, INTERVAL, year, month)
                
                if not df_month.empty:
                    all_months_df.append(df_month)
        
        if all_months_df:
            # Consolidación de todos los meses descargados
            final_df = pd.concat(all_months_df, ignore_index=True)
            
            # Limpieza final: Ordenar por tiempo y eliminar solapamientos
            final_df.sort_values('timestamp', inplace=True)
            final_df.drop_duplicates(subset=['timestamp'], inplace=True)
            
            # Guardado profesional en Parquet
            output_path = os.path.join(RAW_DATA_DIR, f"{symbol}_{INTERVAL}_raw.parquet")
            final_df.to_parquet(output_path, engine='pyarrow', index=False)
            
            print(f"   [OK] Guardadas {len(final_df)} velas en: {output_path}    ")
        else:
            print(f"   [!] No se obtuvieron datos para {symbol}")

    duration = datetime.now() - start_time
    print(f"\n--- ✅ Proceso finalizado en {duration} ---")

if __name__ == "__main__":
    main()