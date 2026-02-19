import websocket
import json
import threading
import pandas as pd

class BinanceWebsocket:
    def __init__(self, symbols=["btcusdt", "ethusdt", "solusdt"]):
        self.symbols = symbols
        # Diccionario para guardar el último cierre de cada moneda
        self.latest_data = {s.upper(): None for s in symbols}
        self.url = "wss://stream.binance.com:9443/ws"
        
    def on_message(self, ws, message):
        msg = json.loads(message)
        # s: Símbolo, k: Datos de la vela, x: ¿Está cerrada?
        if msg['k']['x']: 
            symbol = msg['s']
            self.latest_data[symbol] = {
                "close": float(msg['k']['c']),
                "high": float(msg['k']['h']),
                "low": float(msg['k']['l']),
                "open": float(msg['k']['o']),
                "vol": float(msg['k']['v']),
                "tbb": float(msg['k']['V']) # Volumen de compra agresiva
            }
            print(f"⭐ Datos actualizados para {symbol}")

    def start(self):
        # Creamos la URL para múltiples monedas: btcusdt@kline_5m/ethusdt@kline_5m...
        streams = "/".join([f"{s.lower()}@kline_5m" for s in self.symbols])
        
        def run_ws():
            ws = websocket.WebSocketApp(
                f"{self.url}/{streams}",
                on_message=self.on_message,
                on_error=lambda ws, err: print(f"❌ WS Error: {err}"),
                on_close=lambda ws, c, m: print("🔌 Conexión cerrada")
            )
            ws.run_forever()

        # Lanzamos el proceso en un hilo "Daemon" (se cierra si cierras el programa principal)
        thread = threading.Thread(target=run_ws, daemon=True)
        thread.start()