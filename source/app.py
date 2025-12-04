import streamlit as st
import pandas as pd
import os
import plotly.express as px
import utils as utl

st.set_page_config(
    page_title="Análisis de movilidad urbana con datos de taxis en la NYC",
    page_icon="🚖",
    layout="wide"
)

# --- FUNCIÓN DE CARGA DE DATOS
with st.spinner("Cargando datos, por favor espere..."):
    df = utl.load_data()

# --- SIDEBAR
with st.sidebar:
    st.image("source/logo.jpg", width=200)
    st.title("🚖 Análisis de movilidad urbana con datos de taxis en la NYC 2024")
    st.markdown("---")
    
    # Menú de Navegación
    st.markdown("### Menú")
    menu = st.radio("", ["Proyecto", "Dashboard", "Dataset", "Notebook"])
    st.markdown("---")
    
    st.write("**Alumno:** Braian Alejandro Pucheta")
    st.write("**Materia:** Análisis de Datos Masivos")
    st.write("**Docente:** Ing. Gustavo Rivadera")
    st.write("Repositorio del proyecto: [GitHub](https://github.com/ByBraiiaN/TF-ADM-Taxis-NYC)")
    

# --- SECCIÓN 1: PROYECTO
if menu == "Proyecto":
    st.title("📋 Anteproyecto: Movilidad Urbana en NYC")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ### 1. Definición del Problema
        La movilidad urbana en Nueva York genera un volumen masivo de datos. Este proyecto busca responder:
        *¿Cómo aprovechar los datos masivos de taxis amarillos de NYC para entender patrones de movilidad urbana y generar información útil?*
        
        ### 2. Objetivos
        - **Procesar:** Manejar millones de registros mensuales (Big Data) usando PySpark.
        - **Limpiar:** Tratar outliers (distancias > 100 millas, tarifas negativas).
        - **Analizar:** Identificar horas pico, zonas rentables y patrones de pago.
        - **Visualizar:** Presentar hallazgos en esta aplicación interactiva.
        """)
    
    with col2:
        st.info("""
        **Tecnologías Utilizadas:**
        - Python
        -  PySpark (MapReduce) & Pandas
        -  Formato Parquet
        -  Streamlit & Plotly
        """)

    st.markdown("### 3. Metodología Aplicada")
    st.write("""
    Se utilizó un enfoque iterativo comenzando con la ingesta de archivos Parquet crudos, 
    pasando por una fase de **MapReduce** para filtrado y limpieza, y finalizando con 
    la agregación de métricas clave para su visualización aquí.
    """)

    st.write("Repositorio del proyecto: [GitHub](https://github.com/ByBraiiaN/TF-ADM-Taxis-NYC)")

# --- SECCIÓN 2: DATASET
elif menu == "Dataset":
    st.title("📖 Exploración del Dataset")
    st.write("Vista previa de los datos procesados (Formato Parquet).")
    
    # Métricas rápidas
    c1, c2, c3 = st.columns(3)
    c1.metric("Total de Registros", f"{len(df):,}")
    c2.metric("Distancia Promedio", f"{df['trip_distance'].mean():.2f} millas")
    c3.metric("Tarifa Promedio", f"${df['total_amount'].mean():.2f}")
    
    # Tabla Interactiva
    st.subheader("Primeros 100 registros")
    st.dataframe(df.head(100), use_container_width=True)
    
    st.subheader("Estructura de Columnas")
    df_types = pd.DataFrame(df.dtypes.astype(str), columns=["Tipo de dato"])
    st.write(df_types)

# --- SECCIÓN 3: DASHBOARD
elif menu == "Dashboard":
    st.title("📊 Análisis con Gráfico")
    
    # Filtros Globales (Ejemplo de interactividad)
    #st.sidebar.subheader("Filtros del Dashboard")
    if 'payment_type_name' in df.columns:
        payment_filter = st.sidebar.multiselect(
            "Filtrar por medio de pago:", 
            options=df['payment_type_name'].unique(),
            default=df['payment_type_name'].unique()
        )
        # Aplicar filtro
        df_filtered = df[df['payment_type_name'].isin(payment_filter)]
    else:
        df_filtered = df

    # --- FILA 1: ANÁLISIS TEMPORAL
    st.header("1. Patrones Temporales")
    st.markdown("*¿A qué hora hay más demanda?*")
    
    # Aquí es donde usarías tu columna 'hour' creada en Spark
    if 'hour' in df_filtered.columns:
        # Agrupamos datos para el gráfico (usando pandas aquí para el frontend)
        trips_by_hour = df_filtered.groupby('hour').size().reset_index(name='viajes')
        
        fig_time = px.bar(
            trips_by_hour, 
            x='hour', 
            y='viajes', 
            title="Cantidad de Viajes por Hora del Día",
            labels={'hour': 'Hora del Día', 'viajes': 'Cantidad de Viajes'},
            color_discrete_sequence=['#F7B500'] # Color taxi amarillo
        )
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.warning("⚠️ Falta la columna 'hour' en el dataset. Revisa el notebook de procesamiento.")

    # --- FILA 2: ANÁLISIS DISTANCIA
    st.markdown("---")
    st.header("2. Distribución de Millas")

    st.markdown("*Distribución de distancia recorrida*")

    fig_hist_dist = px.histogram(
        df_filtered,
        x="trip_distance",
        nbins=40,
        title="Distribución de distancias de viaje",
        opacity=0.75
    )

    st.plotly_chart(fig_hist_dist, use_container_width=True)


    # --- FILA 3: ANÁLISIS ECONÓMICO
    st.markdown("---")
    st.header("3. Relaciones Económicas")
    
    col_eco1, col_eco2 = st.columns(2)
    
    with col_eco1:
        st.subheader("Distancia vs Tarifa")
        # Gráfico de dispersión (Scatter Plot)
        # Tomamos una muestra para no saturar el gráfico si son millones de datos
        sample_plot = df_filtered.sample(min(1000, len(df_filtered)))
        
        fig_scatter = px.scatter(
            sample_plot,
            x='trip_distance',
            y='total_amount',
            color='payment_type_name' if 'payment_type_name' in df.columns else None,
            title="Correlación: Distancia recorrida vs Costo Total",
            opacity=0.6
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    with col_eco2:
        st.subheader("Distribución de Pagos")
        if 'payment_type' in df_filtered.columns:
            rename_map = {
                1: "Credit Card",
                2: "Cash",
                3: "No charge",
                4: "Dispute",
                5: "Other"
            }

            # Crear columna nueva con nombres
            df_filtered['payment_type_name'] = df_filtered['payment_type'].map(rename_map)
            df_filtered['payment_type_name'] = df_filtered['payment_type_name'].fillna("Other")
            
            payment_counts = df_filtered['payment_type_name'].value_counts().reset_index()
            payment_counts.columns = ['Tipo de Pago', 'Cantidad']
            
            fig_pie = px.pie(
                payment_counts, 
                values='Cantidad', 
                names='Tipo de Pago',
                title="Proporción de Métodos de Pago",
                hole=0.4
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    # --- FILA 4: ANÁLISIS SEMANA
    st.markdown("---")
    st.header("4. Distribución semanal")
    st.markdown("*Ingresos entre días de semana y fines de semana*")

    df_filtered['is_weekend'] = df_filtered['is_weekend'].map({0: "Día laboral", 1: "Fin de semana"})

    fig_box = px.box(
        df_filtered,
        x="is_weekend",
        y="total_amount",
        title="Comparación de montos totales entre semana y fin de semana",
    )

    st.plotly_chart(fig_box, use_container_width=True)


# --- SECCIÓN 4: Notebook
elif menu == "Notebook":
    st.title("📊 Vista previo sobre el notebook Trabajado")
    st.components.v1.html(utl.load_notebook(), height=800, scrolling=True)

# Pie de página
st.markdown("---")

st.markdown("© 2025 Braian Pucheta - UCASAL - Análisis de Datos Masivos")

