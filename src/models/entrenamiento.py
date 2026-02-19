import polars as pl
import xgboost as xgb
import os
import numpy as np
from sklearn.metrics import accuracy_score, classification_report
import joblib

# --- CONFIGURACIÓN ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "procesados")
MODEL_DIR = os.path.dirname(os.path.abspath(__file__))


def run_walk_forward_training(file_path):
    # 1. Cargar el dataset completo (24 meses)
    df = pl.read_parquet(file_path).sort("timestamp")
    
    # 2. Definir Features y Target
    features = [c for c in df.columns if c.startswith("feature_")]
    X = df.select(features).to_numpy()
    y = df.select("target").to_numpy().ravel() + 1 # Mapeo (-1,0,1) -> (0,1,2)
    timestamps = df.select("timestamp").to_numpy().ravel()

    # 3. Definir los puntos de corte (Splits)
    # Dividiremos en 4 bloques de validación
    # Bloque 1: Entrena 1-12, Test 13-15
    # Bloque 2: Entrena 4-15, Test 16-18
    # ... y así sucesivamente
    
    n_samples = len(df)
    block_size = n_samples // 8  # Dividimos en octavos para mayor precisión
    
    results = []

    print(f" iniciando Walk-Forward Validation para {os.path.basename(file_path)}...")

    # Realizamos 4 iteraciones de validación
    for i in range(4):
        train_start = i * block_size
        train_end = (i + 4) * block_size
        test_end = (i + 5) * block_size
        
        X_train, y_train = X[train_start:train_end], y[train_start:train_end]
        X_test, y_test = X[train_end:test_end], y[train_end:test_end]
        
        # Configuración de XGBoost optimizada para RAM
        model = xgb.XGBClassifier(
            n_estimators=150,
            max_depth=5,
            learning_rate=0.03,
            objective='multi:softprob',
            tree_method='hist', # Clave para tus 16GB
            random_state=42
        )
        
        model.fit(X_train, y_train)
        
        # Predicciones
        preds = model.predict(X_test)
        acc = accuracy_score(y_test, preds)
        results.append(acc)
        
        print(f"   Iteración {i+1}: Accuracy = {acc:.2%}")
    
    print(f"\n✅ Accuracy Promedio Final: {np.mean(results):.2%}")
    return model

if __name__ == "__main__":
    # Ejecutar para BTC como prueba
    btc_path = os.path.join(PROCESSED_DIR, "BTCUSDT_5m_processed.parquet")
    if os.path.exists(btc_path):
        final_model = run_walk_forward_training(btc_path)
    btc_path2 = os.path.join(PROCESSED_DIR, "ETHUSDT_5m_processed.parquet")
    if os.path.exists(btc_path2):
        final_model = run_walk_forward_training(btc_path2)
    btc_path3 = os.path.join(PROCESSED_DIR, "SOLUSDT_5m_processed.parquet")
    if os.path.exists(btc_path3):
        final_model = run_walk_forward_training(btc_path3)
        
        
        
    model_filename = os.path.join(MODEL_DIR, "nite_model_v1.pkl")
    joblib.dump(final_model, model_filename)
        
    print(f"\n✅ ¡ARCHIVO CREADO! Búscalo en: {model_filename}")