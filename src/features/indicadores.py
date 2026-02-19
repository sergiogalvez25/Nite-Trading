import polars as pl
import os

# --- RUTAS DE NITE TRADING ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "data", "descargados")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "procesados")

os.makedirs(PROCESSED_DIR, exist_ok=True)

def calculate_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma datos crudos en métricas para la IA.
    Optimizado para 16GB de RAM usando Polars.
    """
    
    # 1. ORDENAR POR TIEMPO
    df = df.sort("timestamp")

    # 2. INDICADORES DE PRESIÓN (ORDER FLOW)
    # Calculamos el Delta de Volumen (Compras Agresivas - Ventas Agresivas)
    # Ventas Agresivas = Volumen Total - Compras Agresivas
    df = df.with_columns([
        (pl.col("taker_buy_base") - (pl.col("volume") - pl.col("taker_buy_base"))).alias("volume_delta")
    ])
    
    # CVD: Suma acumulada del delta para ver quién tiene el control histórico
    df = df.with_columns([
        pl.col("volume_delta").cum_sum().alias("feature_cvd")
    ])

    # 3. INDICADORES DE CONTEXTO (MEDIAS)
    # Distancia a la EMA 200 (en %)
    df = df.with_columns([
        pl.col("close").ewm_mean(span=200).alias("ema_200")
    ])
    df = df.with_columns([
        (((pl.col("close") - pl.col("ema_200")) / pl.col("ema_200")) * 100).alias("feature_dist_ema_200")
    ])

    # 4. MOMENTO (RSI SIMPLIFICADO)
    # Calculamos ganancias y pérdidas para el RSI
    df = df.with_columns([
        (pl.col("close") - pl.col("close").shift(1)).alias("diff")
    ])
    df = df.with_columns([
        pl.when(pl.col("diff") > 0).then(pl.col("diff")).otherwise(0).alias("gain"),
        pl.when(pl.col("diff") < 0).then(pl.col("diff").abs()).otherwise(0).alias("loss")
    ])
    
    # RSI de 14 periodos
    df = df.with_columns([
        pl.col("gain").rolling_mean(window_size=14).alias("avg_gain"),
        pl.col("loss").rolling_mean(window_size=14).alias("avg_loss")
    ])
    df = df.with_columns([
        (100 - (100 / (1 + (pl.col("avg_gain") / (pl.col("avg_loss") + 1e-10))))).alias("feature_rsi")
    ])

    # 5. VOLATILIDAD (ATR para la Triple Barrera)
    # El ATR nos servirá para poner los Stop Loss y Take Profit de forma dinámica
    df = df.with_columns([
        (pl.max_horizontal([
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("close").shift(1)).abs(),
            (pl.col("low") - pl.col("close").shift(1)).abs()
        ])).rolling_mean(window_size=14).alias("atr")
    ])

    return df.drop_nulls()

def main():
    print("📈 NITE TRADING: CALCULANDO FEATURES...")
    
    # Buscamos los archivos descargados por el módulo 1
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".parquet")]
    
    for file in files:
        print(f" > Procesando {file}...")
        
        # Leemos el archivo
        df = pl.read_parquet(os.path.join(RAW_DIR, file))
        
        # Calculamos indicadores
        df_features = calculate_features(df)
        
        # Guardamos el resultado procesado
        output_path = os.path.join(PROCESSED_DIR, file.replace("_raw.parquet", "_processed.parquet"))
        df_features.write_parquet(output_path)
        
        print(f"   [OK] Guardado en data/processed/")

if __name__ == "__main__":
    main()