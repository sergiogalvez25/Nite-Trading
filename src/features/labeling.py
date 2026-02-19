import polars as pl
import pandas as pd
import numpy as np
import os

# --- RUTAS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "procesados")

def apply_dynamic_triple_barrier(df: pl.DataFrame, pt_multiplier=2.0, sl_multiplier=1.0) -> pl.DataFrame:
    """
    Etiquetado con Ratio 1:2 y Tiempo Dinámico basado en Volatilidad.
    """
    # Convertimos a Pandas para la lógica de búsqueda prospectiva
    pdf = df.to_pandas()
    labels = []
    
    close = pdf['close'].values
    atr = pdf['atr'].values
    # Usamos el volumen medio para calcular la velocidad del mercado
    vol_mean = pdf['volume'].rolling(window=20).mean().values

    for i in range(len(pdf)):
        price_now = close[i]
        volatility = atr[i]
        current_vol = pdf['volume'].values[i]
        
        # --- 1. CÁLCULO DE TIEMPO DINÁMICO ---
        # Si el volumen actual es mayor que la media, el mercado va rápido (menos velas)
        # Si es menor, va lento (más velas). Rango: entre 6 y 18 velas.
        if current_vol > vol_mean[i]:
            dynamic_lookahead = 8  # Mercado rápido (40 min)
        else:
            dynamic_lookahead = 16 # Mercado lento (1h 20 min)
            
        # Evitar salirnos del índice del array
        if i + dynamic_lookahead >= len(pdf):
            labels.append(0)
            continue

        # --- 2. DEFINIR BARRERAS (Ratio 1:2) ---
        take_profit = price_now + (volatility * pt_multiplier)
        stop_loss = price_now - (volatility * sl_multiplier)
        
        # --- 3. EL JUEZ ---
        future_segment = close[i+1 : i+1+dynamic_lookahead]
        
        label = 0
        for future_price in future_segment:
            if future_price >= take_profit:
                label = 1  # Éxito LONG
                break
            elif future_price <= stop_loss:
                label = -1 # Éxito SHORT (o SL tocado)
                break
        
        labels.append(label)

    # Añadimos la columna al DataFrame original de Polars
    return df.with_columns(pl.Series(name="target", values=labels))

def main():
    print("⚖️ NITE TRADING: ETIQUETANDO CON TIEMPO DINÁMICO (R:B 1:2)...")
    
    if not os.path.exists(PROCESSED_DIR):
        print(f"Error: No existe la carpeta {PROCESSED_DIR}")
        return

    files = [f for f in os.listdir(PROCESSED_DIR) if f.endswith(".parquet")]
    
    for file in files:
        print(f" > Procesando {file}...")
        path = os.path.join(PROCESSED_DIR, file)
        df = pl.read_parquet(path)
        
        # Aplicamos la lógica dinámica
        df_labeled = apply_dynamic_triple_barrier(df)
        
        # Guardamos el dataset final
        df_labeled.write_parquet(path)
        print(f"   [OK] Target dinámico aplicado con éxito.")

if __name__ == "__main__":
    main()