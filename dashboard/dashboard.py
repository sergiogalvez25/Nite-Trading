import streamlit as st
import os
import sys

# --- CONFIGURACIÓN DE RUTAS (Tu estilo de confianza) ---
# D:\nite_trading\dashboard\dashboard.py -> Subimos 2 niveles para la raíz
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Ahora importamos el motor que está en src/api/
try:
    from src.api.websocket_binance import BinanceWebsocket
except ImportError:
    st.error("❌ No se encontró el motor de Binance en 'src/api/websocket_binance.py'")

# --- CONFIGURACIÓN DE INTERFAZ ESTILO ESCRITORIO ---
st.set_page_config(
    page_title="Nite Trading Terminal",
    page_icon="🌙",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilo CSS para que parezca más una App de escritorio
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stButton>button { width: 100%; border-radius: 5px; height: 3em; background-color: #262730; }
    .stMetric { background-color: #161b22; padding: 15px; border-radius: 10px; border: 1px solid #30363d; }
    </style>
    """, unsafe_allow_html=True)

# --- INICIALIZACIÓN DEL ESTADO (Singleton Pattern) ---
# Esto asegura que el Websocket solo se cree UNA vez
if 'ws_client' not in st.session_state:
    # Definimos los símbolos con los que entrenamos el modelo
    st.session_state.ws_client = BinanceWebsocket(symbols=["btcusdt", "ethusdt", "solusdt"])
    st.session_state.ws_started = False

# --- BARRA LATERAL (NAVEGACIÓN) ---
with st.sidebar:
    st.title("🌙 NITE TRADING")
    st.subheader("v1.0 - IA Engine")
    st.markdown("---")
    # El radio button controla qué "página" se renderiza
    pagina = st.radio("MENÚ DE NAVEGACIÓN", ["Inicio", "Oportunidades", "Historial"])
    
    st.markdown("---")
    # Indicador de estado del motor en la sidebar (siempre visible)
    if st.session_state.ws_started:
        st.success("🟢 MOTOR: CONECTADO")
    else:
        st.error("🔴 MOTOR: DESCONECTADO")

# --- LÓGICA DE RENDERIZADO DE PÁGINAS ---

if pagina == "Inicio":
    st.title("INICIO")
    
    # Layout de bienvenida
    col_info, col_img = st.columns([2, 1])
    
    with col_info:
        st.markdown(f"""
        ### Bienvenido a la Terminal de Control
        Este es el centro de mando de **Nite Trading**. Desde aquí puedes gestionar la 
        conexión en tiempo real con los servidores de Binance y activar el motor de 
        predicción de la IA.
        
        **Estado del Sistema:**
        - **Modelo cargado:** `nite_model_v1.pkl`
        - **Frecuencia de datos:** 5 minutos (Velas)
        - **Modo:** Simulación (Real-time)
        """)
    
    st.markdown("---")
    
    # Sección de Control del Motor
    st.subheader("🕹️ Control del Motor")
    c1, c2, c3 = st.columns(3)
    
    with c1:
        if not st.session_state.ws_started:
            if st.button("🚀 ACTIVAR TERMINAL", help="Inicia la conexión Websocket"):
                with st.spinner("Estableciendo conexión con Binance..."):
                    st.session_state.ws_client.start()
                    st.session_state.ws_started = True
                    st.rerun() # Refrescamos para actualizar los indicadores de estado
        else:
            st.info("El terminal ya está conectado y recibiendo datos.")

    with c2:
        if st.session_state.ws_started:
            if st.button("🛑 DETENER TERMINAL", help="Cierra la conexión segura"):
                # Aquí podrías añadir lógica para cerrar el socket formalmente
                st.session_state.ws_started = False
                st.rerun()
        else:
            st.button("🛑 DETENER TERMINAL", disabled=True)

    with c3:
        # Botón para limpiar logs o resetear métricas (puedes darle uso más adelante)
        if st.button("🧹 REINICIAR MÉTRICAS"):
            st.toast("Métricas reiniciadas localmente")

elif pagina == "Oportunidades":
    st.title("🚀 Oportunidades")
    st.info("Próximamente: Aquí se mostrarán las señales filtradas por la IA.")

elif pagina == "Historial":
    st.title("📋 Historial")
    st.info("Próximamente: Registro de operaciones simuladas.")