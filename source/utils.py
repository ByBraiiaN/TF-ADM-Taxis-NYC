import os
import pandas as pd
import streamlit as st

@st.cache_data # Cache para no recargar datos cada vez que interactúas
def load_data():
    file_path = "data/taxi_data_outl" 
    
    # Lógica para cargar o crear datos de prueba si no existe el archivo
    if os.path.exists(file_path) or os.path.exists(file_path + ".parquet"):
        try:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            st.error(f"Error leyendo el archivo: {e}")
            return pd.DataFrame()
    else:
        # DATOS DUMMY
        st.warning("⚠️ No se encontró el archivo Parquet real. Mostrando datos de ejemplo.")
        data = {
            'tpep_pickup_datetime': pd.date_range(start='1/1/2024', periods=100, freq='h'),
            'trip_distance': [x * 0.5 + 2 for x in range(100)],
            'total_amount': [x * 1.5 + 5 for x in range(100)],
            'passenger_count': [1, 2, 1, 1, 3] * 20,
            'payment_type_name': ['Credit Card', 'Cash', 'Credit Card', 'Mobile', 'Cash'] * 20,
            'PULocationID': [100, 101, 102, 103, 104] * 20,
            'hour': [x % 24 for x in range(100)]
        }
        return pd.DataFrame(data)

@st.cache_data
def load_notebook():
    html_path = "notebooks/data_processing.html"

    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    return html_content        

