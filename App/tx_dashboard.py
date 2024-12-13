# tx_dashboard.py
from dash import dcc, html
import dash
import io
import dash_daq as daq
from dash.dependencies import Input, Output, State, ALL
from dash import ctx
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash import no_update
import folium
from folium import plugins
from folium.plugins import MiniMap, MousePosition, FloatImage
from folium.plugins import MarkerCluster
from folium.features import CustomIcon
from folium.plugins import Search
from branca.element import Element
from datetime import datetime, timedelta
import DBcredentials
import mysql.connector
import psycopg2
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import networkx as nx
import os
import unicodedata
import json
from dataclasses import dataclass
from typing import List, Dict
from dash.exceptions import PreventUpdate
from io import BytesIO
import time

meses = {
    'January': 'enero',
    'February': 'febrero',
    'March': 'marzo',
    'April': 'abril',
    'May': 'mayo',
    'June': 'junio',
    'July': 'julio',
    'August': 'agosto',
    'September': 'septiembre',
    'October': 'octubre',
    'November': 'noviembre',
    'December': 'diciembre'
}

opciones = [
    {"label": "Ciudad", "value": "ciudades_agr"},
    {"label": "Agregadores FO WOM", "value": "agregadores_wom"},
    {"label": "Agregación Nacional", "value": "agregador_nal"},
    {"label": "Core", "value": "core"},
    {"label": "Clúster", "value": "cluster"},
    {"label": "Total", "value": "total"}
]

ciudades_principales = [
    'Santa marta',
    'Valledupar',
    'Barranquilla',
    'Cartagena de indias',
    'Sincelejo',
    'Cali',
    'San andres',
    'Pereira',
    'Popayan',
    'Pasto',
    'Cucuta',
    'Bucaramanga',
    'Monteria',
    'Arauca',
    'Medellin',
    'Ibague',
    'Tunja',
    'Bogota, d.c.',
    'Villavicencio'
]

MORADO_WOM = "#641f85"
MORADO_CLARO = "#cab2cd"
AMARILLO_ORO = "#ffcb35"
GRIS_CLARO = '#E0E0E0'  # Un gris claro para la grilla
location_init = [4.6837, -74.0566]
zoom_init = 5
STYLE_ACTUAL_ACTIVE = {"backgroundColor": MORADO_WOM, "borderColor": MORADO_WOM}
STYLE_PROYECCION_ACTIVE = {"backgroundColor": MORADO_WOM, "borderColor": MORADO_WOM}
STYLE_ACTUAL_INACTIVE = {"backgroundColor": "white", "color": MORADO_WOM, "borderColor": MORADO_WOM}
STYLE_PROYECCION_INACTIVE = {"backgroundColor": "white", "color": MORADO_WOM, "borderColor": MORADO_WOM}

# Estilos personalizados para la tarjeta
card_style = {
    "background": "linear-gradient(to right, #6a1b9a, #000000)",  # Degradado de morado a negro
    "borderRadius": "10px",
    "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.2)",  # Sombra para darle profundidad
    "color": "white",  # Texto en blanco
    "padding": "5vh 2vw",  # Padding en unidades de viewport
    "textAlign": "left",  # Alineación a la izquierda
    "position": "relative",
    "minHeight": "38vh",  # Altura mínima en porcentaje de la pantalla
}

# Estilos para el valor de Throughput
value_style = {
    "fontSize": "3vw",  # Tamaño del texto en función del ancho de la pantalla
    "fontWeight": "bold",
    "color": "#ffffff",
    "marginTop": "1vh",
    "textAlign": "left",  # Alinear el texto a la izquierda
    "marginLeft": "-1vw"   # Espacio desde el borde izquierdo
}

def eliminar_tildes(texto): # Funcion que elimina las tildes si es necesario, para uniformidad de datos
    if pd.isna(texto):  # Manejar valores nulos
        return texto
    # Normalizar texto a forma 'NFKD' y eliminar las tildes
    texto_sin_tildes = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto_sin_tildes

def obtener_nodos_aristas(): #Se obtiene los nodos y aristas de las bases de datos
    try:
        conn =  mysql.connector.connect(**DBcredentials.BD_DESEMPENO_PARAMS)
        cursor = conn.cursor()

        query = f"""
            SELECT DISTINCT 
                site_code, 
                site_name, 
                latitude AS latitud, 
                longitude AS longitud, 
                CONCAT(UPPER(SUBSTRING(department, 1, 1)), LOWER(SUBSTRING(department, 2))) AS department, 
                CONCAT(UPPER(SUBSTRING(city, 1, 1)), LOWER(SUBSTRING(city, 2))) AS city, 
                carrier AS carrier_tx, 
                subnet_id
            FROM 
                context_site_inv 
            JOIN 
                topology_nodes_all_masterwom 
            ON 
                context_site_inv.site_name = topology_nodes_all_masterwom.node
            WHERE 
                status_site_name = 'Active';
        """
        ## Se hace la consulta para obtener nodos de la DB de desempeño Tx
        cursor.execute(query)
        nodos = cursor.fetchall()
        df_nodos = pd.DataFrame(nodos, columns=["site_code","site_name","latitud","longitud","department","city","carrier_tx", "subnet_id"]) # Creo dataframe
        df_nodos['tipo'] = 'Nodo'
        #print("Consultas de nodos realizadas con exito (host:10.40.111.42 , database:collector_qa)")
        cursor.close()
        conn.close() 
    except mysql.connector.Error as err:
        print(f"Error connecting to database (collector_qa - 10.40.111.42) : {err}") # Si no logra conectarse a la db de desempeño se conecta a la de digitalización
        try:
            conn2 = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)
            cursor.execute("""SELECT site_code, site_name, latitud, longitud, department, city, carrier_tx, subnet_id FROM sites_tx """)
            nodos = cursor.fetchall()
            df_nodos = pd.DataFrame(nodos, columns=["site_code","site_name","latitud","longitud","department","city","carrier_tx", "subnet_id"]) # Creo dataframe
            df_nodos['tipo'] = 'Nodo'
            print("Consultas de nodos realizadas con exito (host:10.40.111.100)")
            cursor.close()
        except psycopg2.Error as err:
            print(f"Error connecting to database(10.40.111.100): {err}")   
        finally:
            conn2.close() # Siempre se va a cerrar la conexión sin importar si hubo excepción o no. Buena práctica por si hay un error


    try:
        conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)
    except psycopg2.Error as err:
        print(f"Error connecting to database: {err}")

    cursor = conn.cursor()
    ## Se hace la consulta para obtener nodos de la DB de desempeño Tx
    cursor.execute("""SELECT node_a, node_b, carrier_tx, link_type FROM enlaces_tx WHERE link_type != '' """)
    aristas = cursor.fetchall()
    df_aristas = pd.DataFrame(aristas, columns=["node_a","node_b","carrier_tx","link_type"]) # Creo dataframe
    #print("Consultas de aristas realizadas con exito (host:10.40.111.100 , database:analytics_dev)")

    cursor.close()
    conn.close()

    return df_nodos, df_aristas

def obtener_agregadores(): #Se obtiene los agregadores en la db establecida
    try:
        conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)
    except psycopg2.Error as err:
        print(f"Error connecting to database: {err}")

    cursor = conn.cursor()

    ## Se hace la consulta para obtener nodos de la DB de desempeño Tx
    cursor.execute("""SELECT * FROM agregadores_fibra """)
    agrega = cursor.fetchall()
    df_agrega = pd.DataFrame(agrega, columns=["agr_code","agr_name","latitud","longitud","agr_city","tipo","site_ran"]) # Creo dataframe
    #print("Consultas de agregadores realizadas con exito (host:10.40.111.100 , database:analytics_dev)")

    ## Se hace la consulta para obtener nodos de la DB de desempeño Tx
    cursor.execute("""SELECT * FROM agregadores_fibra
                   WHERE agr_type = 'core' """)
    core = cursor.fetchall()
    df_core = pd.DataFrame(core, columns=["agr_code","agr_name","agr_latitud","agr_longitud","agr_city","agr_type","site_ran"]) # Creo dataframe
    #print("Consultas de cores realizadas con exito (host:10.40.111.100 , database:analytics_dev)")
    
    cursor.close()
    conn.close()
    df_agrega['agr_name'] = df_agrega['agr_name'].apply(eliminar_tildes)
    columnas_a_limpiar = ['agr_code', 'agr_name', 'tipo','site_ran']
    df_agrega[columnas_a_limpiar] = df_agrega[columnas_a_limpiar].apply(lambda x: x.str.strip())
    return df_agrega, df_core

def generar_etiqueta_aristas(color_secundario, origen, destino, latitud_origen, longitud_origen, latitud_destino, longitud_destino, tipo_tx, medio_tx, carrier):
    popup_content = f"""
    <div style='fontSize:14px; font-family:Arial, sans-serif; line-height:1.6;'>
        <h4 style='marginBottom:8px; color:{color_secundario};'><b>Arista:</b> {origen} - {destino}</h4>
        <p style='margin:0;'><b>Coordenadas origen:</b></p>
        <p style='margin:0;'>({latitud_origen}, {longitud_origen})</p>
        <p style='margin:0;'><b>Coordenadas destino:</b></p>
        <p style='margin:0;'>({latitud_destino}, {longitud_destino})</p>
        <p style='margin:0;'><b>Tipo:</b> {tipo_tx}</p>
        <p style='margin:0;'><b>Medio de Transmisión:</b> {medio_tx}</p>
        <p style='margin:0;'><b>Carrier:</b> {carrier}</p>
    </div>
    """
    return popup_content

def generar_popup(nodo, color, city, tp, latitud, longitud, id=None, carrier=None):
    id_text = f"<b>Subnet id:</b> {id}<br><br>" if id else ""
    carrier_text = f"<b>Carrier:</b> {carrier}<br><br>" if carrier else ""
    return f"""
    <div style='fontSize:14px;'>
        <h4 style='marginBottom:8px;'><b>Nodo:</b> <b><span style='color:{color};'>{nodo}</span></b></h4>
        {id_text}
        <b>Ciudad:</b> {city}<br><br>
        {carrier_text}
        <b>Coordenadas:</b><br>
        Lat: {latitud}<br>
        Lon: {longitud}<br><br>
        <i>Tipo:</i> <span style='color:blue;'>{tp}</span>
    </div>
    """

def obtener_trafico_init():
    today = datetime.today()
    date_end = today - timedelta(days=0)
    date_start = date_end - timedelta(days=16)
    date_start = str(date_start.strftime("%Y-%m-%d")) + ' 00:00:00'
    date_end = str(date_end.strftime("%Y-%m-%d")) + ' 23:00:00'

    query = f"""
        SELECT
          date_time as "time",
          site_name,
          rx_mean_speed,
          rx_max_speed
        FROM ran_hw_eth_pm_agg_1h
        WHERE date_time BETWEEN '{date_start}'  
        AND '{date_end}'  
        ORDER BY date_time;
    """

    try:
        conn = mysql.connector.connect(**DBcredentials.BD_DESEMPENO_PARAMS)
        cursor = conn.cursor()    
        # Ejecutar la consulta
        cursor.execute(query)
        # Obtener los resultados
        result = cursor.fetchall()
        # Obtener los nombres de las columnas
        column_names = [i[0] for i in cursor.description]
        # Cargar los resultados en un DataFrame
        df_resultados = pd.DataFrame(result, columns=column_names)
        cursor.close()
        print("Se realizó con exitos extracción de data")
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        df_resultados =pd.DataFrame()
    finally:
        conn.close() # Siempre se va a cerrar la conexión sin importar si hubo excepción o no. Buena práctica por si hay un error

    return df_resultados

def capitalizar_primera_letra(texto): # Función para poner la primera letra mayucula de alguna columna
    if pd.isna(texto):  # Manejar valores nulos
        return texto
    return texto.capitalize()

def create_initial_graph():
    fig = go.Figure()

    # Añadir las líneas vacías para mantener la estructura
    fig.add_trace(go.Scatter(
        x=[], y=[], mode='lines', name='Avg Rx Thrp Subnetwork',
        line=dict(color='lightgreen', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=[], y=[], mode='lines', name='BWn',
        line=dict(color=AMARILLO_ORO, width=2)
    ))

    # Configurar los ejes y el layout
    fig.update_layout(
        title=dict(text='THROUGHPUT (Hora)', font=dict(size=24, color=MORADO_WOM, family='Arial Black')),
        xaxis=dict(
            title=dict(text='Tiempo', font=dict(size=18, color=MORADO_WOM, family='Arial Black')),
            showgrid=True, gridcolor=GRIS_CLARO, gridwidth=1,
            linecolor=MORADO_WOM, linewidth=2, 
            ticks='outside', tickfont=dict(family='Arial', size=12)
        ),
        yaxis=dict(
            title=dict(text='Valor (Mb/s)', font=dict(size=18, color=MORADO_WOM, family='Arial Black')),
            showgrid=True, gridcolor=GRIS_CLARO, gridwidth=1,
            linecolor=MORADO_WOM, linewidth=2,
            ticks='outside', tickfont=dict(family='Arial', size=12)
        ),
        legend=dict(
            orientation='h',  # Horizontal
            yanchor='top',
            y=-0.2,  # Posición debajo de la gráfica
            xanchor='left',
            x=0,  # Alineado a la izquierda
            font=dict(size=16),  # Letra grande
            bgcolor='rgba(255,255,255,0.5)',  # Fondo semitransparente
            bordercolor='Black',
            borderwidth=1
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=60, r=30, t=50, b=100)  # Aumentado el margen inferior para la leyenda
    )

    return fig

#--------------Funciones para hacer busqueda de sitios según la agrupación---------------------#
def obtener_mapa_nodos_ciudades(ciudad): # Se obtiene los nodos que corresponden a la agrrupación de ciudades
    # Función para encontrar dependencias de un nodo
    def encontrar_dependencias(df, start_node):
        dependencias = set()  # Para almacenar dependencias sin duplicados
        visitados = set()  # Para evitar ciclos

        # Función recursiva de búsqueda
        def busqueda_recursiva(node):
            if node in visitados:
                return  # Evitar ciclos o nodos excluidos
            visitados.add(node)

            # Filtrar dependencias directas del nodo actual
            direct_dependencias = df[((df['node_a'] == node) | (df['node_b'] == node)) & (df['link_type'].isin(['Nodo - Nodo','Nodo - Agregador','Nodo - Pre Agregador']))] 

            # Iterar sobre las dependencias
            for _, row in direct_dependencias.iterrows():
                # Procesar node_b
                if row['node_a'] == node:
                    nodo_relacionado = row['node_b']
                else:
                    nodo_relacionado = row['node_a']

                if (row['link_type'] == 'Nodo - Agregador' and row['node_a'] != start_node):
                    pass
                elif (nodo_relacionado != start_node):
                    dependencias.add(nodo_relacionado)
                    busqueda_recursiva(nodo_relacionado)  # Continuar con la recursión

        # Llamada inicial a la función recursiva
        busqueda_recursiva(start_node)

        # Convertir las dependencias a un DataFrame
        df_dependencias = pd.DataFrame(list(dependencias), columns=['node_b'])
        return df_dependencias
    
    agregadores = agregadores_fibra[agregadores_fibra['agr_city'] == ciudad]

    all_dependencias = pd.DataFrame()

    # Iterar sobre los agregadores y encontrar dependencias
    for _, r in agregadores.iterrows():
        df_result = encontrar_dependencias(aristas_totales, r['agr_name'])
        all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)

    # Eliminar duplicados en dependencias
    all_dependencias.drop_duplicates().reset_index(drop=True)

    # Asegurarse de que filtramos por los nodos dependientes, no solo node_a
    df_nodos = nodos_totales[nodos_totales['site_name'].isin(all_dependencias['node_b'])]
    
    # Se busca a donde conecta el agregador seleccionado
    core_conect = aristas_totales[(aristas_totales['node_b'].isin(agregadores['agr_name'])) & (aristas_totales['link_type'].isin(['Agregador - Agregador','Agregador - Core']))]
    core_conect = core_conect[['node_a']]
    cores_properties = agregadores_fibra[agregadores_fibra['agr_name'].isin(core_conect['node_a'])]

    # Obtener el subnet_id de los nodos seleccionados
    subnet_ids = df_nodos['subnet_id'].unique()  # Obtenemos los subnet_id únicos de los nodos seleccionados

    # Filtrar todos los nodos de nodos_totales que tengan esos subnet_id
    nodos_mismo_subnet = nodos_totales[nodos_totales["subnet_id"].isin(subnet_ids)]

    # Combinar ambos DataFrames: los nodos originales (df_nodos) y los nodos que comparten subnet_id
    df_nodos_final = pd.concat([df_nodos, nodos_mismo_subnet]).drop_duplicates().reset_index(drop=True)

    
    # Crear grafo
    G = nx.MultiGraph()
    G.clear
    G.add_nodes_from((row["site_name"], dict(row)) for _, row in df_nodos_final.iterrows())
    G.add_nodes_from((rowg["agr_name"], dict(rowg)) for _, rowg in agregadores.iterrows())
    G.add_nodes_from((rowc["agr_name"], dict(rowc)) for _, rowc in cores_properties.iterrows())
    nodos_filtrados = set(G.nodes)
    mask_final = aristas_totales['node_a'].isin(nodos_filtrados) & aristas_totales['node_b'].isin(nodos_filtrados)
    aristas_filtradas = aristas_totales[mask_final]
    G.add_edges_from((row['node_b'], row['node_a'], dict(row)) for _, row in aristas_filtradas.iterrows())

    if  df_nodos_final.empty:
        df_nodos_final = None

    latitud = agregadores['latitud'].mean()
    longitud = agregadores['longitud'].mean()
    return df_nodos_final,G, latitud,longitud

def obtener_mapa_nodos_agregadores_wom(agregador):

    # Función para encontrar dependencias de un nodo
    def encontrar_dependencias(df, start_node , excluidos, condition):
        dependencias = set()  # Para almacenar dependencias sin duplicados
        visitados = set()  # Para evitar ciclos

        # Función recursiva de búsqueda
        def busqueda_recursiva(node):
            if node in visitados:
                return  # Evitar ciclos o nodos excluidos
            visitados.add(node)

            # Filtrar dependencias directas del nodo actual
            direct_dependencias = df[((df['node_a'] == node) | (df['node_b'] == node)) & ((df['carrier_tx']=='WOM FO' )| (df['carrier_tx']=='WOM MW' )) & (df['link_type'].isin(condition))] 
            #direct_dependencias = df[((df['node_a'] == node) | (df['node_b'] == node))] 
            direct_dependencias = direct_dependencias[~direct_dependencias['node_a'].isin(excluidos)]
            direct_dependencias = direct_dependencias[~direct_dependencias['node_b'].isin(excluidos)]
            # Iterar sobre las dependencias
            for _, row in direct_dependencias.iterrows():
                # Procesar node_b
                if row['node_a'] == node:
                    nodo_relacionado = row['node_b']
                else:
                    nodo_relacionado = row['node_a']

                if nodo_relacionado != start_node:
                    dependencias.add(nodo_relacionado)
                    busqueda_recursiva(nodo_relacionado)  # Continuar con la recursión

        # Llamada inicial a la función recursiva
        busqueda_recursiva(start_node)

        # Convertir las dependencias a un DataFrame
        df_dependencias = pd.DataFrame(list(dependencias), columns=['node_b'])
        df_dependencias["nodo_dep"] = start_node
        return df_dependencias
    
    agr_wom = agregadores_fibra[agregadores_fibra['agr_name'] == agregador]
    # Asignar un valor predeterminado o procesar 'agr_wom' si está vacío
    if agr_wom.empty:
        type_agr = 'ciudad'  # Asignar 'ciudad' como valor predeterminado
    else:
        type_agr = agr_wom['tipo'].unique()


    if type_agr[0] == 'agregador':
        type_agr[0] = 'agregador_wom'
    
    all_dependencias = pd.DataFrame()

    if type_agr[0] == 'agregador_wom':
        condition = ('Nodo - Agregador','Pre Agregador - Pre Agregador','Pre Agregador - Agregador')
        excluidos = agregadores_fibra[agregadores_fibra['tipo'].isin(['agregador', 'agregador_wom', 'core'])]
        excluidos = excluidos[excluidos['agr_name'] != agregador]
        excluidos = excluidos['agr_name'].unique()
        df_result = encontrar_dependencias(aristas_totales, agregador, excluidos, condition)
        all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)
        condition = ('Nodo - Nodo','Nodo - Pre Agregador')
        excluidos = agregadores_fibra[~agregadores_fibra['agr_name'].isin(all_dependencias['node_b'])]
        excluidos = excluidos['agr_name'].unique()
        for _,row in all_dependencias.iterrows():
            df_result = encontrar_dependencias(aristas_totales, row['node_b'],excluidos, condition)
            all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)
    elif type_agr[0] == 'pre_agregador_wom':
        condition = ('Nodo - Nodo','Nodo - Pre Agregador')
        excluidos = agregadores_fibra[agregadores_fibra['agr_name'] != agregador]
        excluidos = excluidos['agr_name'].unique()
        df_result = encontrar_dependencias(aristas_totales, agregador,excluidos, condition)
        all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)
    
    if type_agr == 'ciudad':
        condition = ('Nodo - Nodo','Nodo - Pre Agregador','Nodo - Agregador','Agregador - Agregador','Agregador - Core')
        agregadores_finales = pd.DataFrame()
        if agregador == 'RED FO BOG':
            agregadores = agregadores_fibra[agregadores_fibra['agr_city'] == 'Bogota']
            for _,row in agregadores.iterrows():
                agr_excluidos = agregadores_fibra[agregadores_fibra['agr_name'] != row['agr_name']]
                excluidos = agr_excluidos['agr_name'].unique()
                df_result = encontrar_dependencias(aristas_totales, row['agr_name'],excluidos, condition)
                if not df_result.empty:
                    # Agregar la fila actual a 'agregadores_finales' cuando hay resultados
                    agregadores_finales = pd.concat([agregadores_finales, row.to_frame().T], ignore_index=True)
                    # Concatenar las dependencias encontradas a 'all_dependencias'
                    all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)

        elif agregador == 'RED FO MED':
            agregadores = agregadores_fibra[agregadores_fibra['agr_city'] == 'Medellin']  
            for _,row in agregadores.iterrows():
                agr_excluidos = agregadores_fibra[agregadores_fibra['agr_name'] != row['agr_name']]
                excluidos = agr_excluidos['agr_name'].unique()
                df_result = encontrar_dependencias(aristas_totales, row['agr_name'],excluidos, condition)
                if not df_result.empty:
                    # Agregar la fila actual a 'agregadores_finales' cuando hay resultados
                    agregadores_finales = pd.concat([agregadores_finales, row.to_frame().T], ignore_index=True)
                    # Concatenar las dependencias encontradas a 'all_dependencias'
                    all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)

        elif agregador == 'RED FO CAL':
            agregadores = agregadores_fibra[agregadores_fibra['agr_city'] == 'Cali']
            for _,row in agregadores.iterrows():
                agr_excluidos = agregadores_fibra[agregadores_fibra['agr_name'] != row['agr_name']]
                excluidos = agr_excluidos['agr_name'].unique()
                df_result = encontrar_dependencias(aristas_totales, row['agr_name'],excluidos, condition)
                if not df_result.empty:
                    # Agregar la fila actual a 'agregadores_finales' cuando hay resultados
                    agregadores_finales = pd.concat([agregadores_finales, row.to_frame().T], ignore_index=True)
                    # Concatenar las dependencias encontradas a 'all_dependencias'
                    all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)  
        agr_c = aristas_totales.query(
            "((node_a in @agregadores['agr_name']) or (node_b in @agregadores['agr_name'])) and "
            "(link_type in ['Agregador - Agregador', 'Agregador - Core']) and carrier_tx == 'WOM FO'"
        )
        agregadores1 = agregadores_fibra[agregadores_fibra["agr_name"].isin(agr_c['node_a'])]
        agregadores2 = agregadores_fibra[agregadores_fibra["agr_name"].isin(agr_c['node_b'])]
        agregadores_finales = pd.concat([agregadores_finales,agregadores1,agregadores2]).drop_duplicates().reset_index(drop=True)
    # Eliminar duplicados en dependencias
    all_dependencias.drop_duplicates(inplace=True)

    df_nodos = nodos_totales[nodos_totales['site_name'].isin(all_dependencias['node_b'])]

    # Obtener el subnet_id de los nodos seleccionados
    subnet_ids = df_nodos['subnet_id'].unique()  # Obtenemos los subnet_id únicos de los nodos seleccionados

    # Filtrar todos los nodos de nodos_totales que tengan esos subnet_id
    nodos_mismo_subnet = nodos_totales[nodos_totales["subnet_id"].isin(subnet_ids)]

    # Combinar ambos DataFrames: los nodos originales (df_nodos) y los nodos que comparten subnet_id
    df_nodos_final = pd.concat([df_nodos, nodos_mismo_subnet]).drop_duplicates().reset_index(drop=True)

    # Asegurarse de que todos los nodos tienen aristas conectadas
    nodos_con_aristas = set(aristas_totales['node_a']).union(set(aristas_totales['node_b']))
    df_nodos_final = df_nodos_final[df_nodos_final['site_name'].isin(nodos_con_aristas)]

    if type_agr == 'ciudad':
        df_agr_properties = agregadores_fibra[agregadores_fibra['agr_name'].isin(agregadores_finales['agr_name'])]
        latitud = df_nodos_final['latitud'].mean()
        longitud = df_nodos_final['longitud'].mean()
    else:
        df_agr_properties = agregadores_fibra[agregadores_fibra['agr_name'].isin(all_dependencias['node_b'])]
        df_agr_properties = pd.concat([df_agr_properties, agr_wom]).drop_duplicates().reset_index(drop=True)
        latitud = agr_wom['latitud']
        longitud = agr_wom['longitud']

    # Crear el grafo
    G = nx.MultiGraph()
    G.add_nodes_from((row["site_name"], dict(row)) for _, row in df_nodos_final.iterrows())
    G.add_nodes_from((rowg["agr_name"], dict(rowg)) for _, rowg in df_agr_properties.iterrows())

    # Filtrar aristas para que solo incluyan nodos que están en el grafo
    nodos_filtrados = set(G.nodes)
    mask_final = aristas_totales['node_a'].isin(nodos_filtrados) & aristas_totales['node_b'].isin(nodos_filtrados)
    aristas_filtradas = aristas_totales[mask_final]
    aristas_filtradas = aristas_filtradas[aristas_filtradas['carrier_tx'].isin(['WOM FO', 'WOM MW'])]
    G.add_edges_from((row['node_b'], row['node_a'], dict(row)) for _, row in aristas_filtradas.iterrows())

    if  df_nodos_final.empty:
        df_nodos_final = None

    return df_nodos_final, G, latitud, longitud

def obtener_mapa_nodos_agregadores(agregador):
    agregadores_jerarquia = {
        "BOG AGR Equinix-RT8H1": 1,
        "MED AGR GTD-RT8H1": 2,
        "BQL AGR Nogales-RT3H1": 3,
        "CLI AGR Chipichape-RT3H1": 3,
        "CAR AGR Cerro La Popa-RT3H1": 4,
        "CAR AGR Chambacu-RT3H1": 4
    }

    def obtener_agregadores_excluidos(node):
        # Si el agregador está en el diccionario, obtenemos su jerarquía
        if node in agregadores_jerarquia:
            jerarquia_agregador = agregadores_jerarquia[node]
        else:
            # Si no está en el diccionario, consideramos que es de jerarquía mayor que categoría 3
            jerarquia_agregador = 5

        # Filtrar los agregadores que tengan jerarquía mayor o igual a la del evaluado
        agregadores_excluidos = [nombre for nombre, jerarquia in agregadores_jerarquia.items() if (jerarquia <= jerarquia_agregador) and (nombre != node)]
        
        return agregadores_excluidos
    

    
        # Función para encontrar dependencias de un nodo
    def encontrar_dependencias(df, start_node , condition):
        dependencias = set()  # Para almacenar dependencias sin duplicados
        visitados = set()  # Para evitar ciclos

        # Función recursiva de búsqueda
        def busqueda_recursiva(node,excluidos):
            if node in visitados:
                return  # Evitar ciclos o nodos excluidos
            visitados.add(node)

            # Filtrar dependencias directas del nodo actual
            direct_dependencias = df[((df['node_a'] == node) | (df['node_b'] == node)) & (df['link_type'].isin(condition))] 
            # Filtrar los nodos excluidos solo si excluidos no está vacío
            if excluidos:  # Esto verifica si excluidos no es una lista vacía
                direct_dependencias = direct_dependencias[~direct_dependencias['node_a'].isin(excluidos)]
                direct_dependencias = direct_dependencias[~direct_dependencias['node_b'].isin(excluidos)]

            # Iterar sobre las dependencias
            for _, row in direct_dependencias.iterrows():
                # Procesar node_b
                if row['node_a'] == node:
                    nodo_relacionado = row['node_b']
                else:
                    nodo_relacionado = row['node_a']

                if (row['link_type'] == 'Nodo - Agregador' and row['node_a'] != start_node):
                    pass
                elif (nodo_relacionado != start_node):
                    dependencias.add(nodo_relacionado)
                    agregadores_excluidos = obtener_agregadores_excluidos(nodo_relacionado)
                    busqueda_recursiva(nodo_relacionado,agregadores_excluidos)  # Continuar con la recursión

        agregadores_excluidos = obtener_agregadores_excluidos(start_node)
        # Llamada inicial a la función recursiva
        busqueda_recursiva(start_node, agregadores_excluidos)

        # Convertir las dependencias a un DataFrame
        df_dependencias = pd.DataFrame(list(dependencias), columns=['node_b'])
        return df_dependencias
    
    
    condition1 = ('Pre Agregador - Pre Agregador','Pre Agregador - Agregador','Agregador - Agregador')
    agregadores_depend = encontrar_dependencias(aristas_totales, agregador, condition1)
    
    fila_agr = {'node_b': agregador}

    # Convertir el diccionario a un DataFrame
    nueva_fila = pd.DataFrame([fila_agr])

    # Agregar la nueva fila al final del DataFrame usando pd.concat()
    agregadores_depend = pd.concat([agregadores_depend, nueva_fila], ignore_index=True)

    # Acumulador de dependencias
    all_dependencias = pd.DataFrame()

    condition2 = ('Nodo - Nodo','Nodo - Pre Agregador','Nodo - Agregador')
    # Iterar sobre los agregadores y encontrar dependencias
    for _, r in agregadores_depend.iterrows():
        df_result = encontrar_dependencias(aristas_totales, r['node_b'],condition2)
        all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)

    # Eliminar duplicados en dependencias
    all_dependencias.drop_duplicates().reset_index(drop=True)

    # Asegurarse de que filtramos por los nodos dependientes, no solo node_a
    df_nodos = nodos_totales[nodos_totales['site_name'].isin(all_dependencias['node_b'])]
    

    # Se busca a donde conecta el agregador seleccionado
    core_conect = aristas_totales[(aristas_totales['node_b'] == agregador) & (aristas_totales['link_type'] == 'Agregador - Core')]
    core_conect = core_conect[['node_a']]

    excluidos = obtener_agregadores_excluidos(agregador)
    agr_conect = aristas_totales[(aristas_totales['node_b'] == agregador) & (aristas_totales['link_type'] == 'Agregador - Agregador')]
    agr_conect = agr_conect[agr_conect['node_a'].isin(excluidos)]
    core_conect = core_conect[['node_a']]

    # Renombrar 'node_b' a 'node_a' y seleccionar columnas
    agregadores_depend = agregadores_depend.rename(columns={'node_b': 'node_a'})

    # Agregar la nueva fila al final del DataFrame usando pd.concat()
    agregadores_depend = pd.concat([agregadores_depend, agr_conect, core_conect]).drop_duplicates().reset_index(drop=True)

    agregadores = agregadores_fibra[agregadores_fibra['agr_name'].isin(agregadores_depend['node_a'])]

    # Obtener el subnet_id de los nodos seleccionados
    subnet_ids = df_nodos['subnet_id'].unique()  # Obtenemos los subnet_id únicos de los nodos seleccionados

    # Filtrar todos los nodos de nodos_totales que tengan esos subnet_id
    nodos_mismo_subnet = nodos_totales[nodos_totales["subnet_id"].isin(subnet_ids)]

    # Combinar ambos DataFrames: los nodos originales (df_nodos) y los nodos que comparten subnet_id
    df_nodos_final = pd.concat([df_nodos, nodos_mismo_subnet]).drop_duplicates().reset_index(drop=True)

    # Crear grafo
    G = nx.MultiGraph()
    G.clear
    G.add_nodes_from((row["site_name"], dict(row)) for _, row in df_nodos_final.iterrows())
    G.add_nodes_from((rowg["agr_name"], dict(rowg)) for _, rowg in agregadores.iterrows())
    nodos_filtrados = set(G.nodes)
    mask_final = aristas_totales['node_a'].isin(nodos_filtrados) & aristas_totales['node_b'].isin(nodos_filtrados)
    aristas_filtradas = aristas_totales[mask_final]
    G.add_edges_from((row['node_b'], row['node_a'], dict(row)) for _, row in aristas_filtradas.iterrows())

    if  df_nodos_final.empty:
        df_nodos_final = None

    latitud = agregadores['latitud'][agregadores['agr_name'] == agregador].iloc[0]
    longitud = agregadores['longitud'][agregadores['agr_name'] == agregador].iloc[0]
    return df_nodos_final,G, latitud,longitud

def obtener_mapa_nodos_cores(core):
    
    agregadores_jerarquia = {
        "NE8000-X8-PE1-BOG":1,
        "NE8000-X8-PE2-BOG":1,
        "NE8000-X8-PE1-MED":2,
        "NE8000-X8-PE2-MED":2,
        "BOG AGR Equinix-RT8H1": 3,
        "MED AGR GTD-RT8H1": 4,
        "BQL AGR Nogales-RT3H1": 5,
        "CLI AGR Chipichape-RT3H1": 5,
        "CAR AGR Cerro La Popa-RT3H1": 6,
        "CAR AGR Chambacu-RT3H1": 6
    }

    def obtener_agregadores_excluidos(node):
        # Si el agregador está en el diccionario, obtenemos su jerarquía
        if node in agregadores_jerarquia:
            jerarquia_agregador = agregadores_jerarquia[node]
        else:
            # Si no está en el diccionario, consideramos que es de jerarquía mayor que categoría 3
            jerarquia_agregador = 7

        # Filtrar los agregadores que tengan jerarquía mayor o igual a la del evaluado
        agregadores_excluidos = [nombre for nombre, jerarquia in agregadores_jerarquia.items() if (jerarquia <= jerarquia_agregador) and (nombre != node)]
        
        return agregadores_excluidos

        # Función para encontrar dependencias de un nodo
    def encontrar_dependencias(df, start_node , condition):
        dependencias = set()  # Para almacenar dependencias sin duplicados
        visitados = set()  # Para evitar ciclos

        # Función recursiva de búsqueda
        def busqueda_recursiva(node,excluidos):
            if node in visitados:
                return  # Evitar ciclos o nodos excluidos
            visitados.add(node)

            # Filtrar dependencias directas del nodo actual
            direct_dependencias = df[((df['node_a'] == node) | (df['node_b'] == node)) & (df['link_type'].isin(condition))] 
            # Filtrar los nodos excluidos solo si excluidos no está vacío
            if excluidos:  # Esto verifica si excluidos no es una lista vacía
                direct_dependencias = direct_dependencias[~direct_dependencias['node_a'].isin(excluidos)]
                direct_dependencias = direct_dependencias[~direct_dependencias['node_b'].isin(excluidos)]

            # Iterar sobre las dependencias
            for _, row in direct_dependencias.iterrows():
                # Procesar node_b
                if row['node_a'] == node:
                    nodo_relacionado = row['node_b']
                else:
                    nodo_relacionado = row['node_a']

                if (row['link_type'] == 'Nodo - Agregador' and row['node_a'] != start_node):
                    pass
                elif (nodo_relacionado != start_node):
                    dependencias.add(nodo_relacionado)
                    agregadores_excluidos = obtener_agregadores_excluidos(nodo_relacionado)
                    busqueda_recursiva(nodo_relacionado,agregadores_excluidos)  # Continuar con la recursión

        agregadores_excluidos = obtener_agregadores_excluidos(start_node)
        # Llamada inicial a la función recursiva
        busqueda_recursiva(start_node, agregadores_excluidos)

        # Convertir las dependencias a un DataFrame
        df_dependencias = pd.DataFrame(list(dependencias), columns=['node_b'])
        return df_dependencias
    
    def buscar_sitios(core):
        condition1 = ('Pre Agregador - Pre Agregador','Pre Agregador - Agregador','Agregador - Agregador','Agregador - Core')
        agregadores_depend = encontrar_dependencias(aristas_totales, core, condition1)
        
        fila_agr = {'node_b': core}

        # Convertir el diccionario a un DataFrame
        nueva_fila = pd.DataFrame([fila_agr])

        # Agregar la nueva fila al final del DataFrame usando pd.concat()
        agregadores_depend = pd.concat([agregadores_depend, nueva_fila], ignore_index=True)

        # Acumulador de dependencias
        all_dependencias = pd.DataFrame()
        condition2 = ('Nodo - Nodo','Nodo - Pre Agregador','Nodo - Agregador')
        # Iterar sobre los agregadores y encontrar dependencias
        for _, r in agregadores_depend.iterrows():
            df_result = encontrar_dependencias(aristas_totales, r['node_b'],condition2)
            all_dependencias = pd.concat([all_dependencias, df_result], ignore_index=True)

        # Eliminar duplicados en dependencias
        all_dependencias.drop_duplicates().reset_index(drop=True)

        # Asegurarse de que filtramos por los nodos dependientes, no solo node_a
        df_nodos = nodos_totales[nodos_totales['site_name'].isin(all_dependencias['node_b'])]
        
        # Se busca a donde conecta el agregador seleccionado
        core_conect = aristas_totales[(aristas_totales['node_b'] == core) & (aristas_totales['link_type'] == 'Core - Core')]
        core_conect = core_conect[['node_a']]

        # Renombrar 'node_b' a 'node_a' y seleccionar columnas
        agregadores_depend = agregadores_depend.rename(columns={'node_b': 'node_a'})

        # Agregar la nueva fila al final del DataFrame usando pd.concat()
        agregadores_depend = pd.concat([agregadores_depend, core_conect]).drop_duplicates().reset_index(drop=True)
        agregadores = agregadores_fibra[agregadores_fibra['agr_name'].isin(agregadores_depend['node_a'])]

        # Obtener el subnet_id de los nodos seleccionados
        subnet_ids = df_nodos['subnet_id'].unique()  # Obtenemos los subnet_id únicos de los nodos seleccionados

        # Filtrar todos los nodos de nodos_totales que tengan esos subnet_id
        nodos_mismo_subnet = nodos_totales[nodos_totales["subnet_id"].isin(subnet_ids)]

        # Combinar ambos DataFrames: los nodos originales (df_nodos) y los nodos que comparten subnet_id
        df_nodos_final = pd.concat([df_nodos, nodos_mismo_subnet]).drop_duplicates().reset_index(drop=True)
        return df_nodos_final, agregadores
    
    if core == 'NE8000-X8-BOGOTA' or core == 'NE8000-X8-MEDELLIN':
        if core == 'NE8000-X8-BOGOTA':
            df_nodos_final1, agregadores1 = buscar_sitios('NE8000-X8-PE1-BOG')
            df_nodos_final2, agregadores2 = buscar_sitios('NE8000-X8-PE2-BOG')
            df_nodos_final = pd.concat([df_nodos_final1,df_nodos_final2]).drop_duplicates().reset_index(drop=True)
            agregadores = pd.concat([agregadores1,agregadores2]).drop_duplicates().reset_index(drop=True)
        elif core == 'NE8000-X8-MEDELLIN':
            df_nodos_final1, agregadores1 = buscar_sitios('NE8000-X8-PE1-MED')
            df_nodos_final2, agregadores2 = buscar_sitios('NE8000-X8-PE2-MED')
            df_nodos_final = pd.concat([df_nodos_final1,df_nodos_final2]).drop_duplicates().reset_index(drop=True)
            agregadores = pd.concat([agregadores1,agregadores2]).drop_duplicates().reset_index(drop=True)  
        cores = agregadores[agregadores['tipo']=='core'] 
        latitud = cores['latitud'].mean()
        longitud = cores['longitud'].mean()   
    else:
        df_nodos_final, agregadores = buscar_sitios(core)
        latitud = agregadores['latitud'][agregadores['agr_name'] == core].iloc[0]
        longitud = agregadores['longitud'][agregadores['agr_name'] == core].iloc[0]

    # Crear grafo
    G = nx.MultiGraph()
    G.clear
    G.add_nodes_from((row["site_name"], dict(row)) for _, row in df_nodos_final.iterrows())
    G.add_nodes_from((rowg["agr_name"], dict(rowg)) for _, rowg in agregadores.iterrows())
    nodos_filtrados = set(G.nodes)
    mask_final = aristas_totales['node_a'].isin(nodos_filtrados) & aristas_totales['node_b'].isin(nodos_filtrados)
    aristas_filtradas = aristas_totales[mask_final]
    G.add_edges_from((row['node_b'], row['node_a'], dict(row)) for _, row in aristas_filtradas.iterrows())

    if  df_nodos_final.empty:
        df_nodos_final = None

    return df_nodos_final,G, latitud,longitud

def obtener_mapa_nodos_cluster (cluster):
    df_nodos = nodos_totales[nodos_totales["subnet_id"] == cluster]
    # Crear grafo
    G = nx.MultiGraph()
    G.add_nodes_from((row["site_name"], dict(row)) for _, row in df_nodos.iterrows())
    
    nodos_filtrados = set(df_nodos['site_name'])

    global aristas_totales
    if 'WOM FO' in df_nodos['carrier_tx'].values:
        df_nodo_main = df_nodos[df_nodos["carrier_tx"] == 'WOM FO']
        nodo_main = df_nodo_main['site_name'].iloc[0]  # Obtener el primer nodo
        agr_series = agregadores_fibra[agregadores_fibra["site_ran"] == nodo_main]

        if not agr_series.empty:
            agr = agr_series['agr_name'].iloc[0]

            # Filtrar aristas
            mask = aristas_totales['node_a'].isin(nodos_filtrados) | aristas_totales['node_b'].isin(nodos_filtrados)
            aristas_filtradas = aristas_totales[mask].copy()

            # Modificar aristas
            mask_omitir = (aristas_filtradas['node_b'] == nodo_main) & (aristas_filtradas['node_a'] == agr)
            mask_modificar = aristas_filtradas['node_a'] == agr
            aristas_filtradas = aristas_filtradas[~mask_omitir]
            aristas_filtradas.loc[mask_modificar, 'node_a'] = nodo_main
            aristas_filtradas.loc[mask_modificar, 'link_type'] = 'Nodo - Nodo'
            aristas_finales = aristas_filtradas
        else:
            aristas_finales = aristas_totales
    else:
        aristas_finales = aristas_totales
    

    # Filtrar nodos que no son de 'WOM MW'
    nodos_main = df_nodos[df_nodos['carrier_tx'] != 'WOM MW']

    # Obtener los nombres únicos de los nodos principales (column 'site_name')
    nodos_main_name = nodos_main['site_name'].unique()

    # Filtrar aristas que se conectan a estos nodos en 'node_b' y con el tipo de enlace requerido
    aristas_agregador = aristas_totales[
        (aristas_totales['node_b'].isin(nodos_main_name)) & 
        (aristas_totales['link_type'].isin(['Nodo - Agregador', 'Nodo - Pre Agregador']))
    ]

    # Obtener los nombres de 'node_a' a los que se conectan esos nodos
    agrs = aristas_agregador['node_a'].unique()
    agregadores = agregadores_fibra[agregadores_fibra["agr_name"].isin(agrs)]
    # Filtrar aristas finales
    mask_final = aristas_finales['node_a'].isin(nodos_filtrados) & aristas_finales['node_b'].isin(nodos_filtrados)
    aristas_filtradas = aristas_finales[mask_final]
    aristas_filtradas = pd.concat([aristas_filtradas,aristas_agregador]).drop_duplicates().reset_index(drop=True)
    G.add_nodes_from((row["agr_name"], dict(row)) for _, row in agregadores.iterrows())
    # Agregar aristas al grafo
    G.add_edges_from((row['node_b'], row['node_a'], dict(row)) for _, row in aristas_filtradas.iterrows())
    return df_nodos,G

def obtener_mapa_nodos_total():
    # Crear grafo
    G = nx.MultiGraph()
    G.clear
    G.add_nodes_from((row["site_name"], dict(row)) for _, row in nodos_totales.iterrows())
    G.add_nodes_from((rowg["agr_name"], dict(rowg)) for _, rowg in agregadores_fibra.iterrows())
    nodos_filtrados = set(G.nodes)
    mask_final = aristas_totales['node_a'].isin(nodos_filtrados) & aristas_totales['node_b'].isin(nodos_filtrados)
    aristas_filtradas = aristas_totales[mask_final]
    G.add_edges_from((row['node_b'], row['node_a'], dict(row)) for _, row in aristas_filtradas.iterrows())
    return nodos_totales,G


# Función para crear el mapa base solo una vez
def create_or_get_base_map(mapa_location,zoom):
    # Inicializa el mapa con ubicación y zoom
    Mapa = folium.Map(
        location=mapa_location,  # Coordenadas de Bogotá
        zoom_start=zoom,  # Nivel de zoom inicial
        min_zoom=4,  # Zoom mínimo
        max_zoom=60,  # Zoom máximo
        max_bounds=True  # Restringe el mapa a los límites del mundo
    )
    # Agregar plugins al mapa
    MiniMap(toggle_display=True, position="bottomleft", width=260, height=240).add_to(Mapa)
    MousePosition(
        position='bottomright',
        separator=" | ",
        lng_first=False,
        num_digits=6,
        prefix="Coordenadas: ",
    ).add_to(Mapa)

    # Capa de Google Maps
    google_maps_tile = folium.TileLayer(
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}&key=YOUR_API_KEY',
        attr='Google',
        name='Google Satellite',
        overlay=False,
        control=True,
        show=False
    )
    google_maps_tile.add_to(Mapa)
    plugins.Geocoder(position="bottomright").add_to(Mapa)

    # CSS personalizado con valores ajustados
    css = """
    <style>
    .legend-image {
        position: fixed !important;
        bottom: 260px !important;  /* Subido considerablemente para estar sobre el minimapa */
        left: 45px !important;
        z-index: 1000 !important;
        width: 220px !important;   /* Aumentado el tamaño base */
        max-width: 26% !important; /* Aumentado el máximo porcentaje de pantalla */
        height: auto !important;
        backgroundColor: white !important;
        borderRadius: 5px !important;
        boxShadow: 0 0 10px rgba(0,0,0,0.2) !important;
    }
    @media (max-width: 768px) {
        .legend-image {
            width: 165px !important;  /* Aumentado para pantallas medianas */
            bottom: 300px !important;
        }
    }
    @media (max-width: 480px) {
        .legend-image {
            width: 140px !important;  /* Aumentado para pantallas pequeñas */
            bottom: 330px !important;
        }
    }
    </style>
    """
    
    Mapa.get_root().html.add_child(folium.Element(css))
    
    ruta_leyenda = "assets/ConvencionesEnlaces.png"
    # Agregar la leyenda como imagen flotante con clase CSS personalizada
    if os.path.exists(ruta_leyenda):
        legend_html = f"""
            <img src="{ruta_leyenda}" class="legend-image">
        """
        Mapa.get_root().html.add_child(folium.Element(legend_html))

    return Mapa

def agregar_nodos_aristas_mapa(mapa, nodos_json, grafo, selection):
    
    # Crear capas
    layer_main = folium.FeatureGroup(name=f'Selección Actual: {selection}', show=True)
    marker_main = MarkerCluster(disableClusteringAtZoom=15)
    marker_main.add_to(layer_main)

    layer_nodos = folium.FeatureGroup(name="Sitios totales", show=False)
    marker_nodos = MarkerCluster(disableClusteringAtZoom=16)
    marker_nodos.add_to(layer_nodos)

    layer_mw = folium.FeatureGroup(name="Sitios Microondas", show=False)
    marker_mw = MarkerCluster(disableClusteringAtZoom=16)
    marker_mw.add_to(layer_mw)

    layer_fo_wom = folium.FeatureGroup(name="Sitios FO WOM", show=False)
    marker_fo_wom = MarkerCluster(disableClusteringAtZoom=16)
    marker_fo_wom.add_to(layer_fo_wom)

    layer_fo_carrier = folium.FeatureGroup(name="Sitios FO Carrier", show=False)
    marker_fo_carrier = MarkerCluster(disableClusteringAtZoom=16)
    marker_fo_carrier.add_to(layer_fo_carrier)

    layer_agregadores = folium.FeatureGroup(name="Agregadores", show=False)
    marker_agregadores = MarkerCluster(disableClusteringAtZoom=16).add_to(layer_agregadores)

    # Crear grupos de capas para aristas
    mw_layer = folium.FeatureGroup(name='Enlaces Microondas',show = False) 
    fo_wom_layer = folium.FeatureGroup(name='Enlaces Fibra óptica (Propia)', show = False) 
    fo_carrier_layer = folium.FeatureGroup(name='Enlaces Fibra óptica (Carrier)', show = False)
    nacional_layer = folium.FeatureGroup(name='Enlaces Nacionales', show = False)

    
    def agregar_marcador_device(latitud, longitud, popup_content, nodo ,tipo_node,carrier, cluster):
        if tipo_node == 'Nodo':
            if carrier == "WOM MW":
                script_dir = os.path.dirname(__file__)  # Directorio del script actual
                icon_path = os.path.join(script_dir, 'assets', 'icon_mw.png')  # Ajusta 'icons' y 'router.png' según tu estructura
            elif carrier == 'WOM FO':
                script_dir = os.path.dirname(__file__)  # Directorio del script actual
                icon_path = os.path.join(script_dir, 'assets', 'icon_foWom.png')  # Ajusta 'icons' y 'router.png' según tu estructura

            else:
                script_dir = os.path.dirname(__file__)  # Directorio del script actual
                icon_path = os.path.join(script_dir, 'assets', 'icon_foCarrier.png')  # Ajusta 'icons' y 'router.png' según tu estructura  
            # Crear un CustomIcon
            icon = CustomIcon(
                icon_image=icon_path,
                icon_size=(60, 60),  # Tamaño del icono en píxeles
                icon_anchor=(30, 20),  # Punto de anclaje del icono
                popup_anchor=(0, -20)  # Punto de anclaje del popup
            )
        elif tipo_node == 'Agregador' or tipo_node == 'agregador_wom':
            script_dir = os.path.dirname(__file__)  # Directorio del script actual
            icon_path = os.path.join(script_dir, 'assets', 'icon_agregador.png')  # Ajusta 'icons' y 'router.png' según tu estructura        
            # Crear un CustomIcon
            icon = CustomIcon(
                icon_image=icon_path,
                icon_size=(56, 62),  # Tamaño del icono en píxeles
                icon_anchor=(28, 31),  # Punto de anclaje del icono
                popup_anchor=(0, -25)  # Punto de anclaje del popup
            )
        
        elif tipo_node == 'pre_agregador_wom':
            script_dir = os.path.dirname(__file__)  # Directorio del script actual
            icon_path = os.path.join(script_dir, 'assets', 'icon_pre_agregador.png')  # Ajusta 'icons' y 'router.png' según tu estructura
            # Crear un CustomIcon
            icon = CustomIcon(
                icon_image=icon_path,
                icon_size=(48, 52),  # Tamaño del icono en píxeles
                icon_anchor=(24, 26),  # Punto de anclaje del icono
                popup_anchor=(0, -22)  # Punto de anclaje del popup
            )
        elif tipo_node == 'Core':
            script_dir = os.path.dirname(__file__)  # Directorio del script actual
            icon_path = os.path.join(script_dir, 'assets', 'icon_core.png')  # Ajusta 'icons' y 'router.png' según tu estructura
            
            # Crear un CustomIcon
            icon = CustomIcon(
                icon_image=icon_path,
                icon_size=(60, 70),  # Tamaño del icono en píxeles
                icon_anchor=(30, 35),  # Punto de anclaje del icono
                popup_anchor=(0, -25)  # Punto de anclaje del popup
            )

        folium.Marker(
            location=[latitud, longitud],
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=folium.Tooltip(nodo),
            icon=icon  # Usa icon
        ).add_to(cluster)

    # Itera sobre los nodos y agrégalos al mapa
    for nodo_data in nodos_json["nodes"]:
        latitud = nodo_data.get("latitud", 0)
        longitud = nodo_data.get("longitud", 0)
        if latitud != 0 and longitud != 0:
            tipo = nodo_data.get("tipo", "")
            if tipo == "core":
                tipo = 'Core'
            elif tipo == 'agregador':
                tipo = 'Agregador'
            city = nodo_data.get("agr_city", nodo_data.get("city", ""))
            carrier = nodo_data.get("carrier_tx", "")
            
            if tipo == "Core":
                color = 'red'
                tp = 'Core'
                popup_content = generar_popup(nodo_data["id"], color, city, tp, latitud, longitud)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_main)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_agregadores)
            
            elif tipo == "Agregador":
                color = 'red'
                tp = 'Agregador'
                popup_content = generar_popup(nodo_data["id"], color, city, tp, latitud, longitud)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_main)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_agregadores)
            
            elif tipo == "agregador_wom":
                color = 'darkgreen'
                tp = 'Agregador (propio)'
                id = nodo_data.get("subnet_id", "No está")
                popup_content = generar_popup(nodo_data["id"], color, city, tp, latitud, longitud, id=id)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_main)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_agregadores)
            
            elif tipo == "pre_agregador_wom":
                color = 'green'
                tp = 'Pre-Agregador (propio)'
                id = nodo_data.get("subnet_id", "No está")
                popup_content = generar_popup(nodo_data["id"], color, city, tp, latitud, longitud, id=id)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_main)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_agregadores)
            
            elif tipo == "Nodo":
                color = {'WOM MW': 'orange', 'WOM FO': 'purple'}.get(carrier, 'gray')
                tp = 'Nodo Microondas' if carrier == "WOM MW" else 'Nodo FO (propio)' if carrier == "WOM FO" else 'Nodo FO'
                id = nodo_data.get("subnet_id", "")
                popup_content = generar_popup(nodo_data["id"], color, city, tp, latitud, longitud, id=id, carrier=carrier)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_main)
                agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_nodos)
                if carrier == "WOM MW":
                    agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_mw)
                elif carrier == "WOM FO":
                    agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_fo_wom)
                else:
                    agregar_marcador_device(latitud, longitud, popup_content, nodo_data["id"], tipo, carrier, marker_fo_carrier)




    def agregar_linea(grafo, origen, destino, datos, color_primario, color_secundario, grosor, tx, capas, dash_array=None):
        latitud_origen = grafo.nodes[origen]['latitud']
        longitud_origen = grafo.nodes[origen]['longitud']
        latitud_destino = grafo.nodes[destino]['latitud']
        longitud_destino = grafo.nodes[destino]['longitud']
        
        if latitud_origen != 0 and longitud_origen != 0 and latitud_destino != 0 and longitud_destino != 0:
            popup_content = generar_etiqueta_aristas(
                color_secundario, origen, destino, latitud_origen, longitud_origen, latitud_destino, longitud_destino, datos.get('link_type'), tx, datos.get('carrier_tx')
            )
            for capa in capas:
                folium.PolyLine(
                    locations=[(latitud_origen, longitud_origen), (latitud_destino, longitud_destino)],
                    color=color_primario,
                    weight=grosor,
                    popup=folium.Popup(popup_content, max_width=195),
                    dash_array=dash_array
                ).add_to(capa)
    
    def agregar_linea_doble(grafo, origen, destino, datos, color_primario, color_secundario, grosor, tx, capas, dash_array=None):
        latitud_origen = grafo.nodes[origen]['latitud']
        longitud_origen = grafo.nodes[origen]['longitud']
        latitud_destino = grafo.nodes[destino]['latitud']
        longitud_destino = grafo.nodes[destino]['longitud']
            # Coordenadas de los municipios intermedios
        san_carlos_lat = 6.188185
        san_carlos_lon = -74.9922221
        manizales_lat = 5.067113
        manizales_lon = -75.510063
        if latitud_origen != 0 and longitud_origen != 0 and latitud_destino != 0 and longitud_destino != 0:
            popup_content = generar_etiqueta_aristas(
                color_secundario, origen, destino, latitud_origen, longitud_origen, latitud_destino, longitud_destino, datos.get('tipo'), tx, datos.get('medio_tx')
            )

        # Añadir el punto intermedio en función del carrier
        if datos.get('medio_tx') == "INTERNEXA":
            # Si es Carrier 1, pasa por San Carlos
            locations = [(latitud_origen, longitud_origen), (san_carlos_lat, san_carlos_lon), (latitud_destino, longitud_destino)]
        elif datos.get('medio_tx') == "UFINET":
            # Si es Carrier 2, pasa por Manizales
            locations = [(latitud_origen, longitud_origen), (manizales_lat, manizales_lon), (latitud_destino, longitud_destino)]
        else:
            # Si no hay un carrier específico, trazar una línea directa
            locations = [(latitud_origen, longitud_origen), (latitud_destino, longitud_destino)]
        for capa in capas:
            folium.PolyLine(
                locations=locations,
                color=color_primario,
                weight=grosor,
                popup=folium.Popup(popup_content, max_width=195),
                dash_array=dash_array
            ).add_to(capa)

    # Agregar líneas para cada arista en el grafo
    for origen, destino, datos in grafo.edges(data=True):
        tipo = datos.get('link_type')
        medio_tx = datos.get('carrier_tx')
        color_fo_wom_senc1 = '#FFFF00'
        color_fo_wom_senc2 = '#000080'
        color_mw_wom1 = 'blue'
        color_mw_wom2 = 'blue'
        color_fo_no_wom1 = '#ff5232'
        color_fo_no_wom2 = 'white'
        # Colores para la lineas
        
        if tipo == "Nodo - Nodo":
            if medio_tx == "WOM FO":
                agregar_linea(grafo, origen, destino, datos, color_fo_wom_senc1, color_fo_wom_senc2, 5, "Fibra Óptica", [layer_main,fo_wom_layer])
            elif medio_tx == "WOM MW":
                agregar_linea(grafo, origen, destino, datos, color_mw_wom2, color_mw_wom1, 6, "Microondas", [layer_main,mw_layer],dash_array='5, 9')
        elif tipo == "Nodo - Agregador":
            if medio_tx == "WOM FO":
                agregar_linea(grafo, origen, destino, datos, color_fo_wom_senc1, color_fo_wom_senc2, 5, "Fibra Óptica", [layer_main,fo_wom_layer])
            elif medio_tx == "WOM MW":
                agregar_linea(grafo, origen, destino, datos, color_mw_wom2, color_mw_wom1, 6, "Microondas", [layer_main,mw_layer],dash_array='5, 9')
            elif  medio_tx not in ("WOM FO", "WOM MW"):
                agregar_linea(grafo, origen, destino, datos, color_fo_no_wom1,  color_fo_no_wom1, 4, "Fibra Óptica", [layer_main,fo_carrier_layer])
                agregar_linea(grafo, origen, destino, datos,  color_fo_no_wom2,  color_fo_no_wom1, 3, "Fibra Óptica", [layer_main,fo_carrier_layer], dash_array='6, 10')
        elif tipo == "Nodo - Pre Agregador":
            if medio_tx == "WOM FO":
                agregar_linea(grafo, origen, destino, datos, color_fo_wom_senc1, color_fo_wom_senc2, 5, "Fibra Óptica", [layer_main,fo_wom_layer])
            elif medio_tx == "WOM MW":
                agregar_linea(grafo, origen, destino, datos, color_mw_wom2, color_mw_wom1, 6, "Microondas", [layer_main,mw_layer],dash_array='5, 9')
            elif  medio_tx not in ("WOM FO", "WOM MW"):
                agregar_linea(grafo, origen, destino, datos, color_fo_no_wom1,  color_fo_no_wom1, 4, "Fibra Óptica", [layer_main,fo_carrier_layer])
                agregar_linea(grafo, origen, destino, datos,  color_fo_no_wom2,  color_fo_no_wom1, 3, "Fibra Óptica", [layer_main,fo_carrier_layer], dash_array='6, 10')
        elif tipo  == "Pre Agregador - Pre Agregador":
            agregar_linea(grafo, origen, destino, datos, '#FFFF00', '#32CD32', 7, "Fibra Óptica", [layer_main, fo_wom_layer])
            agregar_linea(grafo, origen, destino, datos, '#32CD32', '#FFFF00', 3.5, "Fibra Óptica", [layer_main, fo_wom_layer], dash_array='4, 10')
        elif tipo == "Pre Agregador - Agregador":
            if medio_tx == 'WOM FO':
                agregar_linea(grafo, origen, destino, datos, '#FFFF00', '#32CD32', 7, "Fibra Óptica", [layer_main, fo_wom_layer])
                agregar_linea(grafo, origen, destino, datos, '#32CD32', '#32CD32', 3.5, "Fibra Óptica", [layer_main, fo_wom_layer], dash_array='4, 10')
            else:
                agregar_linea(grafo, origen, destino, datos, color_fo_no_wom1,  color_fo_no_wom1, 4, "Fibra Óptica", [layer_main, fo_carrier_layer])
                agregar_linea(grafo, origen, destino, datos,  color_fo_no_wom2,  color_fo_no_wom1, 3, "Fibra Óptica", [layer_main, fo_carrier_layer], dash_array='6, 10')
        elif tipo == "Agregador - Agregador":
            city_origen = grafo.nodes[origen].get('ciudad', '')  # Acceder a la ciudad de origen
            city_destino = grafo.nodes[destino].get('ciudad', '')  # Acceder a la ciudad de destino
            if medio_tx == 'WOM FO':
                agregar_linea(grafo, origen, destino, datos, '#FFFF00', '#000080', 8, "Fibra Óptica", [layer_main, fo_wom_layer])
                agregar_linea(grafo, origen, destino, datos, '#000080', '#000080', 5, "Fibra Óptica", [layer_main, fo_wom_layer], dash_array='4, 10')
            else:
                agregar_linea(grafo, origen, destino, datos, '#ff5232', '#ff5232', 6, "Fibra Óptica", [layer_main, nacional_layer])
                agregar_linea(grafo, origen, destino, datos, 'black', '#ff5232', 5, "Fibra Óptica", [layer_main,nacional_layer], dash_array='6, 10')                    
        
        elif tipo == "Agregador - Core":
            if medio_tx == 'WOM FO':
                agregar_linea(grafo, origen, destino, datos, '#FFFF00', '#000080', 8, "Fibra Óptica", [layer_main,fo_wom_layer])
                agregar_linea(grafo, origen, destino, datos, '#000080', '#000080', 5, "Fibra Óptica", [layer_main,fo_wom_layer], dash_array='4, 10')
            else:
                agregar_linea(grafo, origen, destino, datos, '#ff5232', '#ff5232', 6, "Fibra Óptica", [layer_main, nacional_layer])
                agregar_linea(grafo, origen, destino, datos, 'black', '#ff5232', 5, "Fibra Óptica", [layer_main, nacional_layer], dash_array='6, 10')   

        elif tipo == "Core - Core":
            agregar_linea(grafo, origen, destino, datos, '#ff5232', '#ff5232', 6, "Fibra Óptica", [layer_main,nacional_layer])
            agregar_linea(grafo, origen, destino, datos, 'black', '#ff5232', 5, "Fibra Óptica", [layer_main,nacional_layer], dash_array='6, 10')    

    layer_main.add_to(mapa)
    layer_agregadores.add_to(mapa)
    layer_nodos.add_to(mapa)
    layer_mw.add_to(mapa)
    layer_fo_wom.add_to(mapa)
    layer_fo_carrier.add_to(mapa)
    mw_layer.add_to(mapa)
    fo_wom_layer.add_to(mapa)
    fo_carrier_layer.add_to(mapa)
    nacional_layer.add_to(mapa)


    # Control de capas
    folium.LayerControl(collapsed=False).add_to(mapa)

    # CSS personalizado para establecer un tamaño fijo y ajustar el texto
    css = """
        <style>
            .leaflet-control-layers {
                fontSize: 14px; /* Cambia el tamaño de la fuente */
                padding: 10px; /* Ajusta el padding */
                width: 220px; /* Establece un ancho fijo */
                word-wrap: break-word; /* Ajusta el texto si es muy largo */
                white-space: normal; /* Permite que el texto ocupe varias líneas */
            }
        </style>
    """

    # Añadir el CSS personalizado al mapa
    element = Element(css)
    mapa.get_root().html.add_child(element)
    # Agregar el script JavaScript para controlar la visibilidad de las capas
    script_capas = """
    <script>
    document.addEventListener("DOMContentLoaded", function() {
        var mainLayer = 'Selección Actual: {selection}';
        var totalSitesLayer = 'Sitios totales';
        var specificSitesLayers = ['Sitios Microondas', 'Sitios FO WOM', 'Sitios FO Carrier'];
        var otherLayers = ['Sitios totales','Sitios Microondas', 'Sitios FO WOM', 'Sitios FO Carrier', 'Agregadores', 'Enlaces Microondas', 'Enlaces Fibra óptica (Propia)', 'Enlaces Fibra óptica (Carrier)', 'Enlaces Nacionales'];
        var layerControl = document.querySelector('.leaflet-control-layers-list');

        function updateLayers(clickedLayerName) {
            var checkboxes = Array.from(layerControl.querySelectorAll('input[type="checkbox"]'));
            
            if (clickedLayerName === mainLayer) {
                // Si se activa la capa principal, desactivar todas las demás
                otherLayers.forEach(function(layerName) {
                    var checkbox = checkboxes.find(cb => cb.nextSibling.textContent.trim() === layerName);
                    if (checkbox && checkbox.checked) {
                        checkbox.click();
                    }
                });
            } else if (clickedLayerName === totalSitesLayer) {
                // Si se activa Sitios totales, desactivar Sitios Microondas, Sitios FO WOM y Sitios FO Carrier y la capa principal
                specificSitesLayers.forEach(function(layerName) {
                    var checkbox = checkboxes.find(cb => cb.nextSibling.textContent.trim() === layerName);
                    if (checkbox && checkbox.checked) {
                        checkbox.click();
                    }
                });
                
                var mainCheckbox = checkboxes.find(cb => cb.nextSibling.textContent.trim() === mainLayer);
                if (mainCheckbox && mainCheckbox.checked) {
                    mainCheckbox.click();
                }
            } else if (specificSitesLayers.includes(clickedLayerName)) {
                // Si se activa un sitio específico, desactivar Sitios totales y el mainLayer
                var totalSitesCheckbox = checkboxes.find(cb => cb.nextSibling.textContent.trim() === totalSitesLayer);
                if (totalSitesCheckbox && totalSitesCheckbox.checked) {
                    totalSitesCheckbox.click();
                }
                
                var mainCheckbox = checkboxes.find(cb => cb.nextSibling.textContent.trim() === mainLayer);
                if (mainCheckbox && mainCheckbox.checked) {
                    mainCheckbox.click();
                }
            } else {
                // Si se activa cualquier otra capa, desactivar la principal
                var mainCheckbox = checkboxes.find(cb => cb.nextSibling.textContent.trim() === mainLayer);
                if (mainCheckbox && mainCheckbox.checked) {
                    mainCheckbox.click();
                }
            }
        }

        layerControl.addEventListener('click', function(e) {
            if (e.target.type === 'checkbox') {
                var clickedLayerName = e.target.nextSibling.textContent.trim();
                if (clickedLayerName === mainLayer || 
                    clickedLayerName === totalSitesLayer || 
                    specificSitesLayers.includes(clickedLayerName) || 
                    otherLayers.includes(clickedLayerName)) {
                    updateLayers(clickedLayerName);
                }
            }
        });
    });
    </script>
    """

    # Reemplazar {selection} con el valor real
    script_capas = script_capas.replace("{selection}", selection)

    mapa.get_root().html.add_child(folium.Element(script_capas))

    return mapa

def obtener_trafico_nodos_full(df, df_nodos, start_date, end_date):
    # Comprobaciones iniciales para evitar errores
    if df.empty or df_nodos.empty:
        print("Alerta: Uno o ambos DataFrames están vacíos.")
        return pd.DataFrame(columns=['time', 'Avg Rx Thrp Subnetwork', 'BWn'])
    
    # Formato de las fechas
    date_start = str(start_date) + ' 00:00:00'
    date_end = str(end_date) + ' 23:59:59'

    # Convertir los nombres de los sitios en una lista
    site_names = df_nodos['site_name'].tolist()

    # Filtrar el dataframe por los sitios de interés y por el rango de fechas
    df_filtrado = df[
        (df['site_name'].isin(site_names)) & 
        (df['time'] >= date_start) & 
        (df['time'] <= date_end)
    ]

    # Comprobar si el DataFrame filtrado está vacío después del filtrado
    if df_filtrado.empty:
        print("Aviso: No se encontraron datos en el rango de fechas o para los sitios especificados.")
        # Generar DataFrame
        df_vacio = pd.DataFrame({
            'time': pd.date_range(start=date_start, end=date_end, freq='1h'),
            'Avg Rx Thrp Subnetwork': 0,
            'BWn': 0
        })
        return df_vacio[['time', 'Avg Rx Thrp Subnetwork', 'BWn']]

    # Agrupar por fecha (time) para realizar las agregaciones necesarias
    df_agrupado = df_filtrado.groupby('time').agg(
        sum_rx_mean_speed=('rx_mean_speed', 'sum'),
        avg_rx_max_speed=('rx_max_speed', 'mean'),
        avg_rx_mean_speed=('rx_mean_speed', 'mean'),
        count_sites=('site_name', 'size')
    ).reset_index()

    # Evitar división por cero en la columna 'A'
    df_agrupado['A'] = (
        df_agrupado['avg_rx_max_speed'] / df_agrupado['avg_rx_mean_speed']
    ).fillna(0) - 1

    # Calcular las nuevas columnas
    df_agrupado['Avg Rx Thrp Subnetwork'] = df_agrupado['sum_rx_mean_speed']
    df_agrupado['BWn'] = (
        df_agrupado['count_sites'] * df_agrupado['avg_rx_mean_speed'] *
        (1 + (df_agrupado['A'] / df_agrupado['count_sites']))
    )
    df_agrupado[['time', 'Avg Rx Thrp Subnetwork', 'BWn']] = (df_agrupado[['time', 'Avg Rx Thrp Subnetwork', 'BWn']].replace([np.inf, -np.inf], 0).fillna(0))
    # Redondear los valores a enteros
    df_agrupado['Avg Rx Thrp Subnetwork'] = df_agrupado['Avg Rx Thrp Subnetwork'].round(0).astype(int)
    df_agrupado['BWn'] = df_agrupado['BWn'].round(0).astype(int)

    # Devolver el DataFrame con las nuevas columnas
    return df_agrupado[['time','BWn']]
    
def obtener_trafico(cursor, date_start, date_end, df_traffic):
    date_start = str(date_start) + ' 00:00:00'
    date_end = str(date_end) + ' 23:00:00'
    # Crear la consulta SQL solo para seleccionar las columnas necesarias
    query = f"""
        SELECT
          time,
          site_name,
          rx_mean_speed,
          rx_max_speed
        FROM ran_hw_eth_pm_agg_1h
        WHERE time BETWEEN '{date_start}'  
        AND '{date_end}'  
        ORDER BY time;
    """
    
    # Ejecutar la consulta
    cursor.execute(query)

    # Obtener los resultados
    result = cursor.fetchall()

    # Obtener los nombres de las columnas
    column_names = [i[0] for i in cursor.description]

    # Cargar los resultados en un DataFrame
    df_resultados = pd.DataFrame(result, columns=column_names)
    df_traffic['time'] = pd.to_datetime(df_traffic['time']) 
    df_resultados['time'] = pd.to_datetime(df_resultados['time']) 
    # Devolver el DataFrame con las nuevas columnas
    df_traffic_db = pd.concat([df_traffic, df_resultados]).drop_duplicates().reset_index(drop=True)
    return df_traffic_db

def calculos_report_total(traffic_data,output, start_date, end_date):
    # Usar ExcelWriter para escribir varias hojas
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for option in opciones:
            aggregation = option['value']
            sheet_name = option['label']  # Nombre de la hoja será el label de la opción
            report_data = []  # Lista para almacenar los datos de cada hoja
            if aggregation == 'agregador_nal':
                agregadores_ext = agregadores_fibra[agregadores_fibra['tipo'] == 'agregador']
                agregadores = sorted(agregadores_ext['agr_name'].unique())

                for agregador in agregadores:
                    df_nodos, _, _, _ = obtener_mapa_nodos_agregadores(agregador)
                    if df_nodos is None or df_nodos.empty:
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Agregador': agregador,
                            'Fecha': 'Sin dato',
                            'Traffic(Gbps)': 0
                        })
                    else: 
                        traffic = obtener_trafico_nodos_full(traffic_data, df_nodos, start_date, end_date)
                        
                        # Convertir BWn a numérico
                        traffic['BWn'] = pd.to_numeric(traffic['BWn'], errors='coerce')
                        
                        # Obtener máximo de tráfico y fecha correspondiente
                        max_traffic = traffic['BWn'].max()
                        fecha_max = traffic.loc[traffic['BWn'].idxmax(), 'time']
                        fecha_max = fecha_max.strftime("%H:%M, %d/%m/%Y")
                        max_traffic = (max_traffic / 1000).round(2)  # Convertir a Gbps y redondear
                        
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Agregador': agregador,
                            'Fecha': fecha_max,
                            'Traffic(Gbps)': max_traffic
                        })

            elif aggregation == 'agregadores_wom':
                agregadores_wom = agregadores_fibra[agregadores_fibra['tipo'].isin(['agregador_wom', 'pre_agregador_wom'])]
                agregadores = agregadores_wom['agr_name'].unique()
                agregadores = np.append(agregadores, ['RED FO BOG', 'RED FO CAL','RED FO MED','CLI AGR Chipichape-RT3H1','CLI AGR Parcelaciones Pance C-RT3H1'])
                agregadores = sorted(agregadores)
                
                for agregador in agregadores:
                    df_nodos, _, _, _ = obtener_mapa_nodos_agregadores_wom(agregador)
                    # Si df_nodos está vacío, pasar al siguiente agregador
                    if df_nodos is None or df_nodos.empty:
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Agregador': agregador,
                            'Fecha': 'Sin dato',
                            'Traffic(Gbps)': 0
                        })
                    else:
                        traffic = obtener_trafico_nodos_full(traffic_data, df_nodos, start_date, end_date)
                        
                        # Convertir BWn a numérico
                        traffic['BWn'] = pd.to_numeric(traffic['BWn'], errors='coerce')
                        
                        # Obtener máximo de tráfico y fecha correspondiente
                        max_traffic = traffic['BWn'].max()
                        fecha_max = traffic.loc[traffic['BWn'].idxmax(), 'time']
                        fecha_max = fecha_max.strftime("%H:%M, %d/%m/%Y")
                        max_traffic = (max_traffic / 1000).round(2)  # Convertir a Gbps y redondear
                        
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Agregador': agregador,
                            'Fecha': fecha_max,
                            'Traffic(Gbps)': max_traffic
                        })

            elif aggregation == 'ciudades_agr':
                ciudades = agregadores_fibra['agr_city'].unique()
                ciudades = sorted(ciudades)
                
                for ciudad in ciudades:
                    df_nodos, _, _, _ = obtener_mapa_nodos_ciudades(ciudad)
                    if df_nodos is None or df_nodos.empty:
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Ciudad': ciudad,
                            'Fecha': 'Sin dato',
                            'Traffic(Gbps)': 0
                        })
                    else: 
                        traffic = obtener_trafico_nodos_full(traffic_data, df_nodos, start_date, end_date)
                        
                        # Convertir BWn a numérico
                        traffic['BWn'] = pd.to_numeric(traffic['BWn'], errors='coerce')
                        
                        # Obtener máximo de tráfico y fecha correspondiente
                        max_traffic = traffic['BWn'].max()
                        fecha_max = traffic.loc[traffic['BWn'].idxmax(), 'time']
                        fecha_max = fecha_max.strftime("%H:%M, %d/%m/%Y")
                        max_traffic = (max_traffic / 1000).round(2)  # Convertir a Gbps y redondear
                        
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Agregador':ciudad,
                            'Fecha': fecha_max,
                            'Traffic(Gbps)': max_traffic
                        })

            elif aggregation == 'core':
                cores = nodos_cores['agr_name'].unique()
                cores = np.append(cores, ['NE8000-X8-BOGOTA','NE8000-X8-MEDELLIN'])
                cores = sorted(cores)
                for core in cores:
                    df_nodos, _, _, _ = obtener_mapa_nodos_cores(core)
                    if df_nodos is None or df_nodos.empty:
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Core': core,
                            'Fecha': 'Sin dato',
                            'Traffic(Gbps)': 0
                        })
                    else: 
                        traffic = obtener_trafico_nodos_full(traffic_data, df_nodos, start_date, end_date)
                        
                        # Convertir BWn a numérico
                        traffic['BWn'] = pd.to_numeric(traffic['BWn'], errors='coerce')
                        
                        # Obtener máximo de tráfico y fecha correspondiente
                        max_traffic = traffic['BWn'].max()
                        fecha_max = traffic.loc[traffic['BWn'].idxmax(), 'time']
                        fecha_max = fecha_max.strftime("%H:%M, %d/%m/%Y")
                        max_traffic = (max_traffic / 1000).round(2)  # Convertir a Gbps y redondear
                        
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Core': core,
                            'Fecha': fecha_max,
                            'Traffic(Gbps)': max_traffic
                        })

            elif aggregation == 'cluster':
                # Filtramos solo los sitios donde 'carrier_tx' no sea 'WOM MW'
                nodos_filtrados = nodos_totales[nodos_totales['carrier_tx'] != 'WOM MW']
                # Agrupamos por 'subnet_id' y concatenamos los nombres de sitios en una sola cadena
                clusters = nodos_filtrados.groupby('subnet_id')['site_name'].apply(lambda x: '_'.join([f"{x.name}_{site}" for site in x])).reset_index()
                # Renombramos la columna resultante para que sea más clara
                clusters.columns = ['subnet_id', 'site_names']
                
                for _,row in clusters.iterrows():
                    df_nodos, _, = obtener_mapa_nodos_cluster(row['subnet_id'])
                    if df_nodos is None or df_nodos.empty or len(df_nodos) < 3:
                        """""
                        # Agregar datos a la lista de la hoja actual
                        report_data.append({
                            'Cluster': row['site_names'],
                            'Fecha': 'Sin dato',
                            'Traffic(Gbps)': 0
                        })
                        """
                        pass
                    else: 
                        traffic = obtener_trafico_nodos_full(traffic_data, df_nodos, start_date, end_date)
                        # Convertir BWn a numérico
                        traffic['BWn'] = pd.to_numeric(traffic['BWn'], errors='coerce')
                        # Obtener máximo de tráfico y fecha correspondiente si BWn no está vacío
                        max_traffic = traffic['BWn'].max()
                        if max_traffic == 0:
                            pass
                        else:
                            fecha_max = traffic.loc[traffic['BWn'].idxmax(), 'time']
                            fecha_max = fecha_max.strftime("%H:%M, %d/%m/%Y")
                            # Agregar datos a la lista de la hoja actual
                            report_data.append({
                                'Cluster': row['site_names'],
                                'Fecha': fecha_max,
                                'Traffic(Mbps)': max_traffic
                            })

            elif aggregation == 'total':
                df_nodos,_ = obtener_mapa_nodos_total()
                traffic = obtener_trafico_nodos_full(traffic_data, df_nodos, start_date, end_date)         
                # Convertir BWn a numérico
                traffic['BWn'] = pd.to_numeric(traffic['BWn'], errors='coerce')
                
                # Obtener máximo de tráfico y fecha correspondiente
                max_traffic = traffic['BWn'].max()
                fecha_max = traffic.loc[traffic['BWn'].idxmax(), 'time']
                fecha_max = fecha_max.strftime("%H:%M, %d/%m/%Y")
                max_traffic = (max_traffic / 1000).round(2)  # Convertir a Gbps y redondear
                
                # Agregar datos a la lista de la hoja actual
                report_data.append({
                    'Total': 'Sitios totales',
                    'Fecha': fecha_max,
                    'Traffic(Gbps)': max_traffic
                })

            # Convertir report_data en DataFrame y escribir en una hoja con el nombre especificado
            df_report = pd.DataFrame(report_data)
            df_report.to_excel(writer, sheet_name=sheet_name, index=False)

    # Guardar el archivo Excel en el buffer
    output.seek(0)
    return output

#-----------------------------------Funciones Globales TX--------------------------------------#
def run_global_tx():
    global nodos_totales, aristas_totales,agregadores_fibra, nodos_cores, traffic_json_init
    nodos_totales, aristas_totales = obtener_nodos_aristas()
    agregadores_fibra, nodos_cores = obtener_agregadores()
    print("Funciones globales de TX ejecutadas")
    df_traffic_init = obtener_trafico_init()
    traffic_json_init = df_traffic_init.to_json(date_format='iso', orient='split')  

# Layout para el dashboard
tx_layout = dbc.Container([
    dcc.Store(id='mapLocation'),
    dcc.Store(id='mapZoom'),
    dcc.Store(id='store-init', data=False),  # Almacenamos el estado de ejecución
    dcc.Store(id='traffic-kpi'),
    dcc.Store(id='store-nodos'),  # Componente para almacenar los nodos
    dcc.Store(id='store-grafo'),  # Componente para almacenar el grafo
    dcc.Store(id='data-report'),
    dcc.Store(id="store_proyeccion", data={}), # Guarda los porcentajes de proyección
    dcc.Store(id='data-report-total-proyectado'),
    dcc.Store(id='sel-ant'),
    dcc.Store(id='data-actual'),
    dcc.Store(id='data-proyeccion'),
    dcc.Store(id='last_processed_dates',data={'start_date': None, 'end_date': None}),
    dcc.Store(id="callback_active", data=False),  # Estado inicial: activo, cambia si se pasa a modo de proyección
    dcc.Location(id='url', refresh=False),  # Componente para manejar la URL
    html.Div(id='scroll-trigger', style={'display': 'none'}),  # Trigger para el desplazamiento
    dbc.Row([
        # Sección izquierda (Automática o Manual)
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    # Botones de Actual y Proyección
                    html.H5("Estimación:", className="card-title"),
                    html.Div(style={"height": "1vh"}),  # Espacio en blanco vertical
                        dbc.ButtonGroup(
                        [
                            dbc.Button(
                                "ACTUAL", 
                                id="actual_tab", 
                                color="primary", 
                                outline=False, 
                                active=True,
                                style={
                                    "backgroundColor": MORADO_WOM, 
                                    "borderColor": MORADO_WOM, 
                                    "width": "100%",    # Asegurar que ocupe todo el espacio asignado
                                    "textAlign": "center"
                                }
                            ),
                            dbc.Button(
                                "PROYECCIÓN", 
                                id="proyeccion_tab", 
                                color="primary", 
                                outline=False, 
                                active=False,
                                style={
                                    "backgroundColor": "white", 
                                    "color": MORADO_WOM, 
                                    "borderColor": MORADO_WOM, 
                                    "width": "100%",    # Asegurar que ocupe todo el espacio asignado
                                    "textAlign": "center"
                                }
                            ),
                        ],
                        vertical=True,
                        style={
                            "width": "100%",       # Hacer que el grupo ocupe todo el ancho disponible
                            "display": "flex",    # Habilitar flexbox para distribución uniforme
                            "height": "100%"
                        }
                    ),
                    html.Div(style={"height": "4vh"}),  # Espacio en blanco vertical
                    # Selección de Modo (Automática o Manual)
                    html.H5("Selección de sitios", className="card-title"),
                    html.Div(style={"height": "1vh"}),  # Espacio en blanco vertical
                    dbc.RadioItems(
                        id="modo_seleccion_thr",
                        options=[
                            {"label": "Automática", "value": "auto"},
                            {"label": "Manual", "value": "manual"}
                        ],
                        value="auto",  # Valor predeterminado
                        inline=True,
                        labelClassName="mr-3",
                        style={
                            "width": "100%",    # Asegurar que ocupe todo el espacio asignado
                            "textAlign": "center"
                        }
                    )
                ])
            ], body=True, className="h-100 d-flex flex-column")
        ], width=3),  # Ajusta el ancho según sea necesario

        # Sección central (Menú principal)
        dbc.Col([
            dbc.Card([
                html.Div(id='content-sel'),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Tiempo"),
                        dcc.DatePickerRange(
                            id="time_tx",
                            display_format='YYYY-MM-DD',
                        )
                    ], width=5),
                    dbc.Col([
                        html.Div(id='content-actual-proyeccion', style={"width": "100%"}),
                    ], width=7)
                ], className="mb-4"),

                dbc.Row([
                    dbc.Col([
                        dcc.Loading(children=dcc.Download(id="actualizar_mapa"), color=MORADO_WOM),       
                        # Botones principales
                        html.Div(id='content-btn'),
                    ], width=4, style={"textAlign": "left"}),
                    dbc.Col([
                        dbc.Button("Descargar Reporte", id="reporte_max_avg_tx", n_clicks=0, color="primary", className="mt-3", disabled=True),
                        dcc.Loading(children=dcc.Download(id="download_report_max_avg_tx"), color=MORADO_WOM),
                    ], width=3, style={"textAlign": "center"}),
                    dbc.Col([
                        html.Div(id='content-btn-total-report') # Botón para reporte total
                    ], width=4),
                ], justify="between", align="center")
            ],body=True, className="h-100 p-0", style={"height": "100vh","Width":"100vw"})
        ], width=9),  # Ajusta el ancho según sea necesario
    ], className="mb-4" , style={"height": "100%", "margin": "0", "padding": "0"}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dcc.Loading( # Componente para mostrar estado de carga
                    children=[html.Div(id="test_tx", children="Haz una selección, a nivel de Transmisión",  style={"height": "100%", "fontSize": "20px"})],
                    color="white"
                )
            ], className="bg-primary text-white p-2 mb-2 text-center", style={"height": "100%"})
        ], width=12, align="center", style={"height": "100%"})
    ], justify="center", className="g-0 mx-0", style={"height": "8vh", "margin": "0", "padding": "0", "overflow": "hidden"}),

    dbc.Row([        
        # Columna para el mapa
        dbc.Col([
            dbc.Card([
                html.Div(id='map-marker'), 
                dbc.Row([
                    dcc.Loading(
                        id="loading-map",
                        type="circle",
                        overlay_style={"visibility": "visible", "filter": "blur(4px)"},
                        children=[
                            html.Iframe(
                                id='tx-map',
                                srcDoc=create_or_get_base_map(location_init,zoom_init)._repr_html_(),
                                style={
                                    "border": "none",  # Sin borde alrededor del Iframe
                                    "width": "100%",  # Ocupa el 100% del ancho del contenedor
                                    "height": "105vh",  # Ocupa casi todo el alto de la pantalla menos el margen inferior
                                    "margin": "0",  # Elimina todos los márgenes
                                    "padding": "0",  # Elimina todo el relleno
                                    "marginBottom": "0px",  # Solo deja un margen inferior
                                    "overflow": "hidden"  # Evita desbordamientos
                                }
                            )
                        ],
                        color=MORADO_WOM
                    )
                ]),
            ], style={"height": "100%"}) 
        ], width=10, style={"height": "105vh", "padding": "0px"}),

        # Columna para los indicadores de MAPA
        dbc.Col([ #Panel convenciones mapa
            dbc.Card([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Selecciona un Sitio:",style={"fontSize": "1vw", "marginTop": "0vh"}),
                        dcc.Dropdown(
                            id="select_node",
                            placeholder="Select...",
                        ),
                    ], width=12)
                ],className="g-0 mx-0", style={"margin": "0", "padding": "0"}),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Buscar sitio", 
                            id="search_site", 
                            color="primary", 
                            className="mt-3", 
                            style={"fontSize": "12px"}  # Cambia el tamaño de la letra aquí
                        )
                    ], width=12, className="d-flex justify-content-center")
                ],className="g-0 mx-0", style={"margin": "0", "padding": "0"}),
                dbc.Row([
                    # Título "CONVENCIONES SITIOS"
                    html.H4('CONVENCIONES SITIOS', className="text-center", style={"fontWeight": "bold", "fontSize": "1.7vw", "marginTop": "3.6vh"}),
                    
                    # Columna de imágenes
                    dbc.Col([
                        html.Div([
                            html.Img(src='/assets/icon_mw.png', style={
                                "width": "67%",  # Ajusta al 70% del contenedor para que sea más responsivo
                                "display": "block",
                                "margin": "auto",
                                "marginTop": "0.8vh",
                                "marginBottom": "2vh"
                            })
                        ]),
                        html.Div([
                            html.Img(src='/assets/icon_foWom.png', style={
                                "width": "70%",
                                "display": "block",
                                "margin": "auto",
                                "marginTop": "2vh",
                                "marginBottom": "2vh"
                            })
                        ]),
                        html.Div([
                            html.Img(src='/assets/icon_foCarrier.png', style={
                                "width": "70%",
                                "display": "block",
                                "margin": "auto",
                                "marginTop": "2vh",
                                "marginBottom": "2vh"
                            })
                        ]),
                        html.Div([
                            html.Img(src='/assets/icon_pre_agregador.png', style={
                                "width": "68%",
                                "display": "block",
                                "margin": "auto",
                                "marginTop": "2vh",
                                "marginBottom": "2vh"
                            })
                        ]),
                        html.Div([
                            html.Img(src='/assets/icon_agregador.png', style={
                                "width": "68%",
                                "display": "block",
                                "margin": "auto",
                                "marginTop": "2vh",
                                "marginBottom": "2vh"
                            })
                        ]),
                        html.Div([
                            html.Img(src='/assets/icon_core.png', style={
                                "width": "67%",
                                "display": "block",
                                "margin": "auto",
                                "marginTop": "2vh",
                                "marginBottom": "2vh"
                            })
                        ])
                    ], width=5, style={"padding": "0", "margin": "0"}),

                    # Columna de descripciones de los sitios
                    dbc.Col([
                        html.H4('SITIO MICROONDAS', style={"fontWeight": "bold", "fontSize": "1.1vw", "marginTop": "2.5vh"}),
                        html.H4('SITIO FIBRA ÓPTICA (Propio)', style={"fontWeight": "bold", "fontSize": "1.1vw", "marginTop": "5.5vh"}),
                        html.H4('SITIO FIBRA ÓPTICA (Carrier)', style={"fontWeight": "bold", "fontSize": "1.1vw", "marginTop": "6vh"}),
                        html.H4('SITIO PRE AGREGADOR', style={"fontWeight": "bold", "fontSize": "1.1vw", "marginTop": "5.3vh"}),
                        html.H4('SITIO AGREGADOR', style={"fontWeight": "bold", "fontSize": "1.1vw", "marginTop": "5.3vh"}),
                        html.H4('SITIO CORE', style={"fontWeight": "bold", "fontSize": "1.1vw", "marginTop": "7.5vh"})
                    ], width={"size": 7, "offset": 0, "order": 1}, style={"padding": "0", "margin": "0"})
                ])
            ], body=True, className="h-100 p-0", style={"height": "105vh"})
        ], width=2, className="h-100 p-0")
        
    ], className="g-0 mx-0", style={"height": "105vh", "margin": "0", "padding": "0"}),

    # Nueva fila para mostrar el valor de BWn de manera más atractiva
    dbc.Row([
        dbc.Col([
            # Tarjeta para mostrar el valor y título
            dbc.Card([
                dbc.CardBody([
                    # Imagen como marca de agua en el fondo
                    html.Img(
                        src="/assets/throughtput.png",  # Coloca la imagen en la carpeta assets de Dash
                        style={
                            "position": "absolute",
                            "opacity": "0.1",  # Transparencia de la marca de agua
                            "top": "50%",  # Centrado vertical
                            "left": "50%", 
                            "transform": "translate(-50%, -50%)",
                            "width": "20vw",  # Tamaño de la imagen en función del ancho de la pantalla
                            "height": "auto"  # Mantener la proporción de la imagen
                        }
                    ),
                    
                    # Imagen de Throughput en la esquina superior derecha
                    html.Img(
                        src="/assets/throughtput2.png",  # Coloca la imagen en la carpeta assets de Dash
                        style={
                            "position": "absolute",
                            "top": "8%",
                            "right": "5%", 
                            "width": "4vw",  # Tamaño de la imagen en función del ancho de la pantalla
                            "height": "auto",
                            "zIndex": 1
                        }
                    ),
                    
                    # Título dentro de la tarjeta
                    html.H4("Throughput Máximo", style={
                        "fontSize": "1.8vw",
                        "fontWeight": "bold",
                        "color": "#ffffff",
                        "marginTop": "2.2vh",
                        "textAlign": "left",
                        "padding": "0",  # Elimina el padding
                        "position": "relative",  # Añade posición relativa
                        "left": "-1vw"  # Fuerza la posición a la izquierda
                    }),

                    # Valor del throughput
                    dcc.Loading(
                        id="loading-max-value",
                        type="circle",
                        overlay_style={"visibility": "visible", "filter": "blur(4px)"},
                        children=[
                            html.Div(id='bwn-value', children="750 Mbps", style={**value_style, "position": "relative", "zIndex": 1})
                        ],color = '#ffffff'
                    ),
                    # Título dentro de la tarjeta
                    html.Div(id='max_fecha',children="Sin selección", style={
                        "fontSize": "1.2vw",
                        "fontWeight": "bold",
                        "color": "#ffffff",
                        "marginTop": "1vh",
                        "textAlign": "left",
                        "padding": "0",  # Elimina el padding
                        "position": "relative",  # Añade posición relativa
                        "left": "-0.8vw"  # Fuerza la posición a la izquierda
                    }),

                    # Imagen de diagrama de barras centrada en la parte inferior
                    html.Img(
                        src="/assets/bar_chart.png",  # Coloca una imagen de diagrama de barras en la carpeta assets
                        style={
                            "position": "absolute",
                            "top": "72%",
                            "right": "0.5%", 
                            "width": "12vw",  # Tamaño de la imagen en función del ancho de la pantalla
                            "height": "auto",
                            "zIndex": 1
                        }
                    )
                ])
            ], style=card_style, className="h-100")  # Estilo personalizado para la tarjeta
        ], width=6),  # La tarjeta ocupará la mitad del ancho
        dbc.Col([
            dbc.Card([
                dcc.Loading(
                    id="loading-graf-granu",
                    type="circle",
                    overlay_style={"visibility": "visible", "filter": "blur(4px)"},
                    children=[
                        dcc.Graph(id="max_avg_graf",style={"height": "37vh"})
                    ],color=MORADO_WOM
                )
            ], style={"padding": "0", "textAlign": "left", "position": "relative","maxHeight": "40vh","minHeight": "38vh"}, className="h-100")  # Altura mínima en porcentaje de la pantalla})
        ],width=6) 
    ], className="g-0 mx-0"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dcc.Loading(
                    id="loading-graf-hour",
                    type="circle",
                    overlay_style={"visibility": "visible", "filter": "blur(4px)"},
                    children=[
                        dcc.Graph(id='combined-graph')
                    ],color=MORADO_WOM
                )
            ], body=True, className="h-100 d-flex flex-column")
        ], width=12)
    ], className="mb-4", style={"height": "auto"}) 
], fluid=True, style={"minHeight": "100vh", "padding": "0px"})

# Función para registrar los callbacks
def register_tx_callbacks(app):
    @app.callback(
        [Output("actual_tab", "active"),
        Output("proyeccion_tab", "active"),
        Output("actual_tab", "style"),
        Output("proyeccion_tab", "style"),
        Output('content-actual-proyeccion', 'children'),
        Output('content-btn-total-report','children'),
        Output('store-nodos', 'data')],
        [Input("actual_tab", "n_clicks"),
        Input("proyeccion_tab", "n_clicks")],
        [State('store-nodos', 'data')]
    )
    def toggle_tabs(n_clicks_actual, n_clicks_proyeccion,sitios):
        # Validar entradas
        n_clicks_actual = n_clicks_actual or 0
        n_clicks_proyeccion = n_clicks_proyeccion or 0

        # Determinar estados
        active_actual = n_clicks_actual >= n_clicks_proyeccion
        active_proyeccion = not active_actual

        # Definir estilos
        style_actual = STYLE_ACTUAL_ACTIVE if active_actual else STYLE_ACTUAL_INACTIVE
        style_proyeccion = STYLE_PROYECCION_ACTIVE if active_proyeccion else STYLE_PROYECCION_INACTIVE

        # Contenido dinámico
        if active_actual:
            menu = html.Div([
                dbc.Label("Granularidad"),
                dcc.Dropdown(
                    id="granularidad_tx",
                    options=[
                        {"label": "Día", "value": "dia"},
                        {"label": "Semana", "value": "semana"},
                        {"label": "Mes", "value": "mes"}
                    ],
                    value="dia",
                    clearable=False,
                ),
                html.Div(id="ciudades-panel")
            ])
            div_btn_report = html.Div([
                dbc.Row([
                    dbc.Col(dcc.Loading(children=dcc.Download(id="download_report_total"), color=MORADO_WOM), width="auto"),
                    dbc.Col(dbc.Button("Descargar Reporte Total", id="reporte_total", n_clicks=0, color="primary", className="mt-3"), width="auto")
                ], justify="end", align="center", style={"marginLeft": "auto"})
            ])
        else:
            menu = html.Div([
                dcc.Store(id="granularidad_tx", data="dia"),
                dbc.Row([
                    dbc.Col([
                        html.Div([
                            html.Button("Lectura Proyección", id="btn-toggle", n_clicks=0, className="btn btn-primary mb-3 me-1"),
                            html.Button(
                                "Guardar Valores", 
                                id="btn-guardar", 
                                n_clicks=0, 
                                className="btn btn-success mb-3 me-1", 
                                style={"display": "inlineBlock"}
                            ),
                            dcc.Loading(
                                children=dcc.Store(id="downloading_report"),
                                color=MORADO_WOM,
                                style={"display": "inlineBlock", "verticalAlign": "top", "marginBottom": "-5vh","marginLeft": "6vh"}
                            )
                        ], className="d-flex align-items-start")
                    ], width=12)
                ]),
                html.Div(
                    id="ciudades-panel",
                    children=[
                        dbc.Collapse(id="collapse-panel", is_open=True)
                    ]
                )
            ])
            div_btn_report = html.Div([
                dbc.Row([
                    dbc.Col(dcc.Loading(children=dcc.Download(id="download_report_total"), color=MORADO_WOM), width="auto"),
                    dbc.Col(dbc.Button("Descargar Reporte Total", id="reporte_total_proy", color="primary", className="mt-3"), width="auto"),
                    dcc.Store(id='state-btn-report', data = False)
                ], justify="end", align="center", style={"marginLeft": "auto"})
            ])
        return active_actual, active_proyeccion, style_actual, style_proyeccion, menu, div_btn_report, sitios

    @app.callback(
        [Output('content-sel', 'children'),
         Output('content-btn', 'children'),
         Output("reporte_max_avg_tx", "disabled")],
        Input("modo_seleccion_thr", "value")
    )
    def panel_modo_sites(value):
        if value == 'auto':
            panel_select = html.Div([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Agregación Geográfica"),
                        dcc.Dropdown(
                            id="aggregation_tx",
                            options=opciones,
                            clearable=False,
                        )
                    ], width=5),
                    dbc.Col([
                        dbc.Label("Selección"),
                        dcc.Dropdown(
                            id="select_grupo",
                            placeholder="Selecciona un punto",
                        ),
                    ], width=7),    
                ], className="mb-4")
            ])
            botones = html.Div([
                dbc.Button("Buscar Sitios", id="update-map-button", color="primary", className="mt-3",disabled=True)
            ])
        elif value=='manual':
            global nodos_totales
            sitios = nodos_totales['site_name'].unique()
            sitios = sorted(sitios)
            panel_select = html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Div(style={"height": "2vh"}),  # Espacio en blanco vertical
                        dbc.Label("Selección de los sitios de manera manual para obtener el máximo Throughtput")
                    ], width=5),
                    dbc.Col([
                        dbc.Label("Selección"),
                        dcc.Dropdown(
                            id="nodos_select_manual",
                            options=sitios,
                            value=[],  # Lista inicial vacía
                            multi=True,  # Permitir selección múltiple
                            placeholder="Selecciona uno o más sitios...",  # Texto de ayuda
                            style={"width": "100%"}  # Ajusta el ancho al contenedor
                        ),
                    ], width=7),    
                ], className="mb-4")              
            ])
            botones = html.Div([
                dbc.Button("Buscar Sitios", id="update-map-button-manual", color="primary", className="mt-3", disabled=True)
            ])

        return panel_select, botones, True

    # Callback para desactivar el botón si hay cambios en el dropdown
    @app.callback(
        [Output("reporte_max_avg_tx", "disabled", allow_duplicate=True),  # Controlar el estado del botón
         Output('update-map-button-manual', 'disabled',allow_duplicate=True)],
        Input("nodos_select_manual", "value"),  # Monitorear cambios en el dropdown
        prevent_initial_call=True
    )
    def toggle_button_manual(sitios_seleccionados):
        # Desactivar el botón si el dropdown cambia
        return True, not bool(sitios_seleccionados)
    
    # Callback para desactivar el botón si hay cambios en cualquier dropdown
    @app.callback(
        [Output("reporte_max_avg_tx", "disabled", allow_duplicate=True),  # Controlar el estado del botón
         Output('update-map-button', 'disabled',allow_duplicate=True)],
        Input("aggregation_tx", "value"),  # Monitorear cambios en aggregation_tx
        Input("select_grupo", "value"),  # Monitorear cambios en select_grupo
        prevent_initial_call=True
    )
    def toggle_button_auto( aggregation_value, grupo_value):
        # Desactivar el botón si hay cambios en cualquier dropdown
        # El botón estará habilitado solo si alguno de los valores no está vacío
        return any([aggregation_value, grupo_value]), not any([aggregation_value, grupo_value])
    
    # Callback combinado para manejar el estado del panel y guardar valores
    @app.callback(
        Output("collapse-panel", "is_open"),  # Controla el estado del panel
        Input("btn-toggle", "n_clicks"),      # Botón para mostrar/ocultar
        #Input("btn-guardar", "n_clicks")],    # Botón para guardar valores
        State("collapse-panel", "is_open"),
        prevent_initial_call=True
    )
    def manejar_panel(toggle_clicks, is_open):
        if toggle_clicks is None:
            return dash.no_update
        # Convertir el json almacenado en un DataFrame utilizando StringIO
        return not is_open
    
    # Callback combinado para manejar el estado del panel y guardar valores
    @app.callback(
        [Output("collapse-panel", "is_open", allow_duplicate=True), 
         Output('data-proyeccion', 'data'),
         Output("callback_active", "data", allow_duplicate=True),
         Output('reporte_max_avg_tx', 'disabled',allow_duplicate=True),
         Output('data-report-total-proyectado', 'data'),
         Output('store_proyeccion','data'),
         Output('downloading_report','data')],  # Controla el estado del panel
        Input("btn-guardar", "n_clicks"),      # Botón para mostrar/ocultar
        State({"type": "input-city", "index": ALL}, "value"),  # Todos los inputs dinámicos
        State({"type": "input-city", "index": ALL}, "id"),  # IDs completos (incluyen "index")
        State('store-nodos', 'data'),
        State('state-btn-report', 'data'),
        State('store_proyeccion','data'),
        State('traffic-kpi','data'),
        prevent_initial_call=True,
        running=[(Output(component_id='btn-guardar', component_property='disabled'), True, False)] # Mientras el callback esté corriendo desactiva el botón
    )
    def guardar_panel(toggle_clicks, valores_inputs, ids, nodos_data, btn_report_final,percent_proy,traffic_json):

        def actualizar_diccionario(diccionario_grande, diccionario_nuevo):
            if diccionario_grande is None:
                diccionario_grande = {}

            for clave, valor in diccionario_nuevo.items():
            # Si la clave ya existe, actualiza el valor
                if clave in diccionario_grande:
                    diccionario_grande[clave] = valor
                else:
                    # Si no existe, agrega la nueva clave-valor
                    diccionario_grande[clave] = valor
            return diccionario_grande
        
        if toggle_clicks:
            # Crear DataFrame desde JSON
            df_traffic = pd.read_json(io.StringIO(traffic_json), orient='split')
            if btn_report_final == True:
                df_nodos = nodos_totales
                # Extraer los índices (ciudades) de los IDs
                opciones = [id_dict["index"] for id_dict in ids]

                # Combinar valores con opciones
                resultados = dict(zip(opciones, valores_inputs))
                # Función para determinar el valor en 'inc'
                def asignar_inc(fila):
                    if fila['city'] in resultados:
                        return resultados[fila['city']]
                    elif fila['department'] in resultados:
                        return resultados[fila['department']]
                    else:
                        return None  # Si no coincide ni ciudad ni departamento

                # Aplicar la lógica para asignar la columna 'inc'
                df_nodos['inc'] = df_nodos.apply(asignar_inc, axis=1)
                # Crear un diccionario de incrementos por site_name
                incrementos = df_nodos.set_index('site_name')['inc'].to_dict()

                # Filtrar registros de df_traffic que coincidan con los site_name de df_nodos
                traffic_proyectado = df_traffic[df_traffic['site_name'].isin(df_nodos['site_name'])].copy()

                # Aplicar el incremento porcentual según el valor en df_nodos['inc']
                traffic_proyectado['rx_mean_speed'] *= (1 + traffic_proyectado['site_name'].map(incrementos) / 100)
                traffic_proyectado['rx_max_speed'] *= (1 + traffic_proyectado['site_name'].map(incrementos) / 100)
                traffic_proyectado.reset_index(drop=True)
                data_proyectada = traffic_proyectado.to_json(date_format='iso', orient='split')           
                percent_proy = actualizar_diccionario(percent_proy, resultados)
                return False, dash.no_update, dash.no_update, dash.no_update, data_proyectada, percent_proy, None
            
            # Imprimir valores para depuración
            df_nodos = pd.read_json(io.StringIO(nodos_data), orient='split')

            # Extraer los índices (ciudades) de los IDs
            opciones = [id_dict["index"] for id_dict in ids]

            # Combinar valores con opciones
            resultados = dict(zip(opciones, valores_inputs))
            # Función para determinar el valor en 'inc'
            def asignar_inc(fila):
                if fila['city'] in resultados:
                    return resultados[fila['city']]
                elif fila['department'] in resultados:
                    return resultados[fila['department']]
                else:
                    return None  # Si no coincide ni ciudad ni departamento

            # Aplicar la lógica para asignar la columna 'inc'
            df_nodos['inc'] = df_nodos.apply(asignar_inc, axis=1)

            # Crear un diccionario de incrementos por site_name
            incrementos = df_nodos.set_index('site_name')['inc'].to_dict()

            # Filtrar registros de df_traffic que coincidan con los site_name de df_nodos
            traffic_proyectado = df_traffic[df_traffic['site_name'].isin(df_nodos['site_name'])].copy()

            # Aplicar el incremento porcentual según el valor en df_nodos['inc']
            traffic_proyectado['rx_mean_speed'] *= (1 + traffic_proyectado['site_name'].map(incrementos) / 100)
            traffic_proyectado['rx_max_speed'] *= (1 + traffic_proyectado['site_name'].map(incrementos) / 100)
            traffic_proyectado.reset_index(drop=True)
            data_proyectada = traffic_proyectado.to_json(date_format='iso', orient='split')           
            percent_proy = actualizar_diccionario(percent_proy, resultados)
            return False, data_proyectada, True, False, dash.no_update, percent_proy, None
        return dash.no_update, None, False, True, dash.no_update, dash.no_update,None
  
    @app.callback(
        Output('scroll-trigger', 'children'),
        Input('update-map-button', 'n_clicks')
    )
    def trigger_scroll(n_clicks):
        if n_clicks is not None and n_clicks > 0:
            return "scroll"
        return dash.no_update

    # Callback del lado del cliente para realizar el desplazamiento
    app.clientside_callback(
        """
        function(trigger) {
            if (trigger === "scroll") {
                const element = document.getElementById('map-marker');
                if (element) {
                    element.scrollIntoView({behavior: 'smooth', block: 'start'});
                }
            }
            return null;  // Cambiado de window.dash_clientside.no_update a null
        }
        """,
        Output('url', 'href'),
        Input('scroll-trigger', 'children')
    )
           
    @app.callback(
        [Output('time_tx', 'start_date'), Output('time_tx', 'end_date')],
        [Input('time_tx', 'id')]
    )
    def update_date_range(_):
        today = datetime.today()
        end_date = today - timedelta(days=0)
        start_date = end_date - timedelta(days=16)
        
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    # Callback para desactivar el botón si hay cambios en el dropdown
    @app.callback(
        Output("callback_active", "data", allow_duplicate=True),  # Controlar el estado del botón
        Input("granularidad_tx", "value"),  # Monitorear cambios en el dropdown
        State("callback_active", "data"),
        prevent_initial_call=True
    )
    def toggle_granularidad(_,active):
        # Desactivar el botón si el dropdown cambia
        return active
    
    @app.callback(
        [Output('time_tx', 'start_date',allow_duplicate=True), 
         Output('time_tx', 'end_date',allow_duplicate=True), 
         Output('reporte_max_avg_tx', 'disabled',allow_duplicate=True),
         Output('store-nodos', 'data', allow_duplicate=True)],
        [Input('time_tx', 'start_date'), Input('time_tx', 'end_date')],
        State('store-nodos', 'data'),
        prevent_initial_call=True
    )
    def validate_date_range(start_date, end_date,data_nodos):
        today = datetime.today().strftime("%Y-%m-%d")
        
        # Verificar y ajustar las fechas si alguna es mayor al día de hoy
        if start_date > today:
            start_date = today
        if end_date > today:
            end_date = today

        return start_date, end_date, True, data_nodos

    @app.callback(
        Output('tx-map', 'srcDoc', allow_duplicate=True),
        Input('search_site', 'n_clicks'),
        State('select_node', 'value'),
        State('tx-map', 'srcDoc'),
        State('store-grafo', 'data'),
        State('sel-ant', 'data'),
        prevent_initial_call=True
    )
    def update_map_view(n_clicks, node, current_map,graph_json,grupo):
        if n_clicks is None or not current_map or not node:
            return current_map
        
        # Obtener latitud y longitud del nodo seleccionado
        latitud = float(nodos_totales[nodos_totales['site_name'] == node]['latitud'].iloc[0])
        longitud = float(nodos_totales[nodos_totales['site_name'] == node]['longitud'].iloc[0])
        ubicacion = [latitud-0.00000018, longitud]
        if isinstance(grupo, (int, float)):
            grupo = "Cluster: subnet_id" + str(grupo)
        G = nx.node_link_graph(graph_json)
        mapa = create_or_get_base_map(ubicacion,23)
        mapa = agregar_nodos_aristas_mapa(mapa,graph_json,G,grupo)
        return mapa._repr_html_()

    @app.callback(
        [Output('store-nodos', 'data', allow_duplicate=True), 
         Output('store-grafo', 'data', allow_duplicate=True), 
         Output(component_id='test_tx', component_property='children',allow_duplicate=True), 
         Output('sel-ant','data', allow_duplicate=True),
         Output("callback_active", "data", allow_duplicate=True),
         Output('update-map-button', 'disabled',allow_duplicate=True),
         Output('mapLocation','data'),
         Output('mapZoom','data')],
        Input('update-map-button', 'n_clicks'),
        State('aggregation_tx', 'value'),
        State('select_grupo', 'value'),
        prevent_initial_call=True,  # Importante para evitar actualizaciones iniciales
        running=[(Output(component_id='update-map-button', component_property='disabled'), True, False)] # Mientras el callback esté corriendo desactiva el botón
    )
    def update_nodos_auto(n_clicks, aggregation, grupo):

        if n_clicks is None:
            raise PreventUpdate
        
        print("Comenzando búsqueda de Nodos")
        if aggregation == 'core':
            # Lógica para mostrar agregadores
            df_nodos,G,latitud, longitud = obtener_mapa_nodos_cores(grupo)
            # Configurar ubicación y zoom del mapa
            latitud = df_nodos['latitud'].mean()
            longitud = df_nodos['longitud'].mean()
            if df_nodos is not None:  # Verificar que df_nodos no sea None
                num_filas = len(df_nodos)
                df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')
                latitud = latitud
                longitud = longitud
            else:
                num_filas = 0      
                df_nodos_json = None
                latitud = latitud
                longitud = longitud

            zoom = 9
            latitud = round(latitud,3)
            longitud = round(longitud,3)
            mapa_location = [latitud,longitud]
            graph_data = nx.node_link_data(G)
            df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')

        elif aggregation == 'agregador_nal':
            # Lógica para mostrar agregadores
            df_nodos,G,latitud, longitud = obtener_mapa_nodos_agregadores(grupo)
            # Configurar ubicación y zoom del mapa
            latitud = df_nodos['latitud'].mean()
            longitud = df_nodos['longitud'].mean()
            if df_nodos is not None:  # Verificar que df_nodos no sea None
                num_filas = len(df_nodos)
                df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')
                latitud = df_nodos['latitud'].mean()
                longitud = df_nodos['longitud'].mean()
            else:
                num_filas = 0      
                df_nodos_json = None
                latitud = latitud
                longitud = longitud

            if num_filas < 3:
                zoom = 20
            elif num_filas >2 and num_filas< 6:
                zoom = 18
            elif num_filas > 5 and num_filas  < 13:
                zoom = 15
            elif num_filas > 12 and num_filas  < 18:
                zoom = 12
            else:
                zoom = 10
            latitud = round(latitud,3)
            longitud = round(longitud,3)
            mapa_location = [latitud,longitud]
            graph_data = nx.node_link_data(G)
            df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')

        elif aggregation == 'agregadores_wom':
            # Lógica para mostrar agregadores
            df_nodos,G,latitud,longitud = obtener_mapa_nodos_agregadores_wom(grupo)
            # Configurar ubicación y zoom del mapa

            if df_nodos is not None:  # Verificar que df_nodos no sea None
                num_filas = len(df_nodos)
                df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')
            else:
                num_filas = 0      
                df_nodos_json = None

            if num_filas < 3:
                zoom = 20
            elif num_filas >2 and num_filas< 6:
                zoom = 18
            elif num_filas > 5 and num_filas  < 13:
                zoom = 15
            elif num_filas > 12 and num_filas  < 18:
                zoom = 12
            else:
                zoom = 10
            latitud = round(latitud,3).mean()
            longitud = round(longitud,3).mean()
            mapa_location = [latitud,longitud]         
            graph_data = nx.node_link_data(G)  
            
        elif aggregation == 'total':
            # Lógica para mostrar total
            df_nodos,G = obtener_mapa_nodos_total()
            # Configurar ubicación y zoom del mapa
            latitud = df_nodos['latitud'].mean()
            longitud = df_nodos['longitud'].mean()
            num_filas = len(df_nodos)
            zoom = zoom_init
            mapa_location = [latitud,longitud]
            graph_data = nx.node_link_data(G)
            df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')

        elif aggregation == 'cluster':
            df_nodos,G = obtener_mapa_nodos_cluster(grupo)
            # Configurar ubicación y zoom del mapa
            latitud = df_nodos['latitud'].mean()
            longitud = df_nodos['longitud'].mean()
            num_filas = len(df_nodos)
            if num_filas < 3:
                zoom = 20
            elif num_filas >2 and num_filas< 6:
                zoom = 18
            elif num_filas > 5 and num_filas  < 13:
                zoom = 15
            elif num_filas > 12 and num_filas  < 18:
                zoom = 12
            else:
                zoom = 10
            mapa_location = [latitud,longitud]
            graph_data = nx.node_link_data(G)
            df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')   
        elif aggregation == 'ciudades_agr':
            df_nodos,G,latitud,longitud = obtener_mapa_nodos_ciudades(grupo)

            num_filas = len(df_nodos)
            if num_filas < 3:
                zoom = 20
            elif num_filas >2 and num_filas< 6:
                zoom = 18
            elif num_filas > 5 and num_filas  < 13:
                zoom = 15
            elif num_filas > 12 and num_filas  < 18:
                zoom = 12
            else:
                zoom = 10
            mapa_location = [latitud,longitud]
            graph_data = nx.node_link_data(G)
            df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')           

        else: 
            df_nodos_json = None
            graph_data = None
            mapa_location = location_init
            zoom = zoom_init
        if df_nodos_json == None:
            container = f"No hay datos de sitios para la selección: {grupo}"
        else: 
            container = "Buscando sitios.."

        # Retornar el HTML del mapa actualizado y la figura combinada
        return df_nodos_json, graph_data, container, grupo, False, True, mapa_location,zoom
 
    @app.callback(
        [Output('store-nodos', 'data', allow_duplicate=True), 
         Output('store-grafo', 'data', allow_duplicate=True), 
         Output(component_id='test_tx', component_property='children',allow_duplicate=True), 
         Output('sel-ant','data', allow_duplicate=True),
         Output("callback_active", "data", allow_duplicate=True),
         Output('update-map-button-manual', 'disabled',allow_duplicate=True),
         Output('mapLocation','data', allow_duplicate=True),
         Output('mapZoom','data', allow_duplicate=True)],
        Input('update-map-button-manual', 'n_clicks'),
        State('nodos_select_manual', 'value'),
        prevent_initial_call=True,  # Importante para evitar actualizaciones iniciales
        running=[(Output(component_id='update-map-button-manual', component_property='disabled'), True, False)] # Mientras el callback esté corriendo desactiva el botón
    )
    def update_nodos_manual(n_clicks, nodos_select):
        if n_clicks is None or nodos_select is None:  # Se debe presionar el boton para que se actualice el callback
            raise PreventUpdate
        
        df_nodos = nodos_totales[nodos_totales['site_name'].isin(nodos_select)]
        # Crear grafo
        G = nx.MultiGraph()
        G.clear
        G.add_nodes_from((row["site_name"], dict(row)) for _, row in df_nodos.iterrows())
        nodos_filtrados = set(G.nodes)
        mask_final = aristas_totales['node_a'].isin(nodos_filtrados) & aristas_totales['node_b'].isin(nodos_filtrados)
        aristas_filtradas = aristas_totales[mask_final]
        G.add_edges_from((row['node_b'], row['node_a'], dict(row)) for _, row in aristas_filtradas.iterrows())
        if  df_nodos.empty:
            df_nodos = None
            df_nodos_json = None
        else: 
            df_nodos_json = df_nodos.to_json(date_format='iso', orient='split')  
        zoom = 8
        latitud = df_nodos['latitud'].mean()
        longitud = df_nodos['longitud'].mean()
        mapa_location = [latitud,longitud]
        graph_data = nx.node_link_data(G)
        container = "Buscando sitios.."

        return df_nodos_json,graph_data,container,'manual', False, True, mapa_location, zoom

    @app.callback(  # Callback para generar el mapa
        [Output('reporte_max_avg_tx', 'disabled',allow_duplicate=True),
        Output('store-init', 'data'),
        Output("ciudades-panel", "children",allow_duplicate=True),
        Output('data-actual', 'data'),
        Output(component_id='test_tx', component_property='children',allow_duplicate=True),
        Output('last_processed_dates','data', allow_duplicate=True),
        Output('traffic-kpi','data', allow_duplicate=True),
        Output("callback_active", "data", allow_duplicate=True)],
        Input('store-nodos', 'data'),  # Escucha los cambios en el Store
        [State('store-init', 'data'),
        State("actual_tab", "active"),
        State('data-proyeccion', 'data'),
        State('time_tx', 'start_date'),
        State('time_tx', 'end_date'),
        State('sel-ant','data'),
        State(component_id='test_tx', component_property='children'),
        State('store_proyeccion','data'),
        State('last_processed_dates','data'),
        State('traffic-kpi','data')],
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
    )  
    def interface_states_update(data_nodos, state_init, mode_actual, traffic_actual_json, start_date, end_date, grupo, container, percent_proy, last_processed_dates, traffic_json):
        if state_init == False:
            today = datetime.today()
            date_end = today - timedelta(days=0)
            date_start = date_end - timedelta(days=16)
            # Actualizar las últimas fechas procesadas
            last_processed_dates['start_date'] = date_start.strftime("%Y-%m-%d")
            last_processed_dates['end_date'] = date_end.strftime("%Y-%m-%d")

            return  True, not state_init, dash.no_update, dash.no_update, dash.no_update,last_processed_dates,traffic_json_init,False
        
        if data_nodos:
            df_nodos = pd.read_json(io.StringIO(data_nodos), orient='split')
            df_traffic = pd.read_json(io.StringIO(traffic_json), orient='split')
            # Verificar si las fechas han cambiado
            dates_changed = (start_date < last_processed_dates['start_date'])
            if  dates_changed:
                try:
                    conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)
                    print("Se abrió conexión a DB")
                    df_traffic_ant = df_traffic
                    cursor = conn.cursor()
                    df_traffic = obtener_trafico(cursor, start_date,last_processed_dates['start_date'], df_traffic_ant)
                    traffic_json =  df_traffic.to_json(date_format='iso', orient='split')  
                    cursor.close()
                    conn.close()
                    print("Se cerró conexión a DB")
                    
                    # Actualizar las últimas fechas procesadas
                    last_processed_dates['start_date'] = start_date
                    last_processed_dates['end_date'] = end_date
                except psycopg2.Error as err:
                    print(f"Error connecting to database: {err}")
            else:
                pass
            
        
            if mode_actual == True:
                traffic_actual = df_traffic[df_traffic['site_name'].isin(df_nodos['site_name'])].copy()
                traffic_actual_json = traffic_actual.to_json(date_format='iso', orient='split') 
                estado = True
                panel = None
                btn_report_max = False
                container = f"Su selección es: {grupo},  en el rango de fechas {start_date} -> {end_date}"
                
            elif mode_actual == False:
                estado = False
                ciudades = df_nodos['city'].unique()
                btn_report_max = True
                container = f"Su selección es: {grupo}, como proyección usando el rango de fechas {start_date} -> {end_date}"
                ciudades_select = list(set(ciudades) & set(ciudades_principales))
                departamentos = df_nodos[~df_nodos['city'].isin(ciudades_select)]['department'].unique()
                input_options = ciudades_select + list(departamentos)
                panel = [
                    dbc.Collapse(
                        id="collapse-panel",
                        is_open=True,  # Cambiar según sea necesario
                        children=[
                            dbc.Row([
                                dbc.Col(html.Label(option), width=5),
                                dbc.Col(
                                    dcc.Input(
                                        type="number",
                                        min=-100,
                                        max=100,
                                        value=percent_proy.get(option, 0),  # Usa el valor si existe, 0 si no
                                        id={"type": "input-city", "index": option}  # ID dinámico
                                    ), width=2
                                )
                            ], className="mt-1") for option in input_options
                        ]
                    )
                ]
            return  btn_report_max, state_init, panel,traffic_actual_json, container,last_processed_dates,traffic_json, estado
        else: 
            raise PreventUpdate

    @app.callback(  # Callback para generar el mapa
        Output('tx-map', 'srcDoc'),
        Input('store-grafo', 'data'),  # Escucha los cambios en el Store
        State('sel-ant', 'data'),
        State('tx-map', 'srcDoc'),
        State('mapLocation','data'),
        State('mapZoom','data'),
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
    )
    def map_update(graph_json, grupo, currentMap, mapLocation,zoom):
        if graph_json:
            if isinstance(grupo, (int, float)):
                grupo = "Cluster: subnet_id" + str(grupo)
            G = nx.node_link_graph(graph_json)
            mapa = create_or_get_base_map(mapLocation,zoom)
            mapa = agregar_nodos_aristas_mapa(mapa,graph_json,G,grupo)
            return mapa._repr_html_()
        return currentMap
   
    # Callback para descargar reporte de KPIs de la selección actual dentro del rango de fechas
    @app.callback(
        Output(component_id='download_report_max_avg_tx', component_property='data'),
        Input(component_id='reporte_max_avg_tx', component_property='n_clicks'),
        State(component_id="time_tx", component_property='start_date'),
        State(component_id="time_tx", component_property='end_date'),
        State('data-report', 'data'),
        State('sel-ant', 'data'),
        State('granularidad_tx', 'value'),
        State("actual_tab", "active"),
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
    )
    def download_report_tx(boton, start_date, end_date, data, grupo_ant,interval,mode_actual):
        if boton is None:  # Se debe presionar el boton para que se actualice el callback
            raise PreventUpdate
        df_data = pd.read_json(io.StringIO(data), orient='split')
        max_bwn = df_data['BWn'].max()

        if max_bwn >= 1000:
            escala = 'Gbps'
            df_data['BWn'] = (df_data['BWn']/1000).round(2)
        else:
            escala = 'Mbps'
            df_data['BWn'] = df_data['BWn'].round(2)            
        df_data['time'] = pd.to_datetime(df_data['time']) 
        if interval == 'dia':
            # Agrupar datos por día
            df_result = (
                df_data
                .groupby(df_data['time'].dt.date)
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )

        elif interval == 'semana':
            # Agrupar datos por semana (usar inicio de semana como agrupador)
            df_result = (
                df_data
                .groupby(pd.Grouper(key='time', freq='W-Mon'))
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )
        elif interval == 'mes':
            # Agrupar datos por mes
            df_result = (
                df_data
                .groupby(pd.Grouper(key='time', freq='MS'))
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )
        else:
            # Agrupar datos por día
            df_result = (
                df_data
                .groupby(df_data['time'].dt.date)
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )

        df_result.columns = ['time', 'BWn_mean', 'BWn_max']
        df_result['BWn_mean'] = df_result['BWn_mean'].round(2)
        df_result.rename(columns={
            'BWn_mean': f'Traffic_mean({escala})',
            'BWn_max': f'Traffic_max({escala})'
        }, inplace=True)
        if mode_actual == True:
            file_name = f"Report_troughtput_{grupo_ant}_{start_date}-{end_date}.csv"
        else:
            file_name = f"Report_troughtput_{grupo_ant}_Proyectada.csv"
            df_result['time'] = pd.to_datetime(df_result['time']).dt.strftime('%m-%d')
        return dcc.send_data_frame(df_result.to_csv, file_name, index=False)

    # Callback para descargar reporte total
    @app.callback(
        Output(component_id='download_report_total', component_property='data'),
        Input(component_id='reporte_total', component_property='n_clicks'),
        State(component_id="time_tx", component_property='start_date'),
        State(component_id="time_tx", component_property='end_date'),
        State('traffic-kpi','data'),
        running=[(Output(component_id='reporte_total', component_property='disabled'), True, False)],
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
    )
    def download_report_total(boton, start_date, end_date, traffic_json):
        if boton is None:  # Se debe presionar el boton para que se actualice el callback
            raise PreventUpdate

        df_traffic = pd.read_json(io.StringIO(traffic_json), orient='split')
        df_traffic['time'] = pd.to_datetime(df_traffic['time']) 

        # Crear un buffer en memoria para almacenar el archivo Excel
        output = BytesIO()
        
        output = calculos_report_total(df_traffic,output,start_date,end_date) # Función que hace todos los calculos para las diferentes agrupaciones

        # Nombre del archivo de salida
        file_name = f"Report_total_{start_date}_{end_date}.xlsx"     
        # Enviar archivo como respuesta de descarga
        return dcc.send_bytes(output.getvalue(), file_name)
    
        # Callback para descargar reporte total
    
    @app.callback(
        [Output(component_id='download_report_total', component_property='data', allow_duplicate=True),
         Output('state-btn-report','data'),
         Output("ciudades-panel", "children",allow_duplicate=True)],
        [Input(component_id='reporte_total_proy', component_property='n_clicks'),Input('data-report-total-proyectado', 'data')],
        State(component_id="time_tx", component_property='start_date'),
        State(component_id="time_tx", component_property='end_date'),
        State('state-btn-report','data'),
        State('store_proyeccion','data'),
        running=[(Output(component_id='reporte_total_proy', component_property='disabled'), True, False), (Output(component_id='btn-guardar', component_property='disabled'), True, False)], # Mientras el callback esté corriendo desactiva el botón
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
    )
    def download_report_total_proyectada(boton,data_proyectada, start_date, end_date, estado_boton, percent_proy):
        if boton is None and estado_boton == False:  # Se debe presionar el boton para que se actualice el callback
            raise PreventUpdate
        if estado_boton == False:
            ciudades = nodos_totales['city'].unique()
            ciudades_select = list(set(ciudades) & set(ciudades_principales))
            departamentos = nodos_totales[~nodos_totales['city'].isin(ciudades_select)]['department'].unique()
            input_options = ciudades_select + list(departamentos)
            panel = [
                dbc.Collapse(
                    id="collapse-panel",
                    is_open=True,  # Cambiar según sea necesario
                    children=[
                        dbc.Row([
                            dbc.Col(html.Label(option), width=5),
                            dbc.Col(
                                dcc.Input(
                                    type="number",
                                    min=-100,
                                    max=100,
                                    value=percent_proy.get(option, 0),  # Usa el valor si existe, 0 si no
                                    id={"type": "input-city", "index": option}  # ID dinámico
                                ), width=2)
                        ], className="mt-1") for option in input_options
                    ]
                )
            ]
            
            return dash.no_update, not estado_boton, panel
        else:
            traffic_data = pd.read_json(io.StringIO(data_proyectada), orient='split')
            traffic_data['time'] = pd.to_datetime(traffic_data['time']) 
            # Crear un buffer en memoria para almacenar el archivo Excel
            output = BytesIO()

            output = calculos_report_total(traffic_data,output,start_date,end_date) # Función que hace todos los calculos para las diferentes agrupaciones
            
            # Nombre del archivo de salida
            file_name = f"Report_total_proyectada_data_{start_date}_{end_date}.xlsx"     
            # Enviar archivo como respuesta de descarga
            return dcc.send_bytes(output.getvalue(), file_name),not estado_boton, dash.no_update
    
    @app.callback( # Callback que genera el grafico general por hora
        Output('combined-graph', 'figure'),
        Input("callback_active", "data"),
        State('time_tx', 'start_date'),
        State('time_tx', 'end_date'),
        State("store-nodos", "data"),
        State("actual_tab", "active"),
        State('data-proyeccion', 'data'),
        State('data-actual', 'data'),
        prevent_initial_call=True
    )
    def update_graph_based_on_nodos(active,start_date,end_date,nodos_data,mode_actual, data_proyeccion, data_actual):
        if (nodos_data is None) or (not active):
            return create_initial_graph()

        # Convertir el json almacenado en un DataFrame utilizando StringIO
        df_nodos = pd.read_json(io.StringIO(nodos_data), orient='split')
        
        if mode_actual == True:
            if data_actual is None:
                return create_initial_graph()
            trafiic_actual = pd.read_json(io.StringIO(data_actual), orient='split')
            traffic = trafiic_actual
            traffic['time'] = pd.to_datetime(traffic['time']) 
        else:
            if data_proyeccion is None:
                return create_initial_graph()
            trafiic_proyectado = pd.read_json(io.StringIO(data_proyeccion), orient='split')
            traffic = trafiic_proyectado
            traffic['time'] = pd.to_datetime(traffic['time']) 
            
        # Obtener el tráfico basado en df_nodos (así como en tu lógica actual)
        df_traffic_cluster = obtener_trafico_nodos_full(traffic, df_nodos,start_date,end_date)
        max = df_traffic_cluster['BWn'].max()
        if max >= 1000:
            escala = "Gb"
            df_traffic_cluster['BWn'] = (df_traffic_cluster['BWn'] / 1000).round(2)
        else:
            escala = "Mb"
            df_traffic_cluster['BWn']=df_traffic_cluster['BWn'].round(2)

        # Crear la gráfica combinada con datos
        fig = go.Figure()
        if mode_actual == True:
            hovertemplate_graph=f"Date: %{{x|%Y-%m-%d %H:%M}}, %{{y:.2f}}{escala}"
        else:
            hovertemplate_graph=f"Date: %{{x|%m-%d %H:%M}}, %{{y:.2f}}{escala}"
        """""    
        fig.add_trace(go.Scatter(
            x=df_traffic_cluster['time'],
            y=df_traffic_cluster['Avg Rx Thrp Subnetwork'],
            mode='lines',
            name='Avg Rx Thrp Subnetwork',
            line=dict(color='lightgreen', width=3),
            hovertemplate=f"Date: %{{x|%Y-%m-%d %H:%M}}, %{{y:.2f}}{escala}"
        ))
        """
        fig.add_trace(go.Scatter(
            x=df_traffic_cluster['time'],
            y=df_traffic_cluster['BWn'],
            mode='lines',
            name='Throughput',
            line=dict(color=AMARILLO_ORO, width=3),
            hovertemplate=hovertemplate_graph
        ))

        fig.update_layout(
            title=dict(text='THROUGHPUT (Hora)', font=dict(size=24, color=MORADO_WOM, family='Arial Black')),
            xaxis=dict(
                title=dict(text='Tiempo', font=dict(size=18, color=MORADO_WOM, family='Arial Black')),
                showgrid=True, gridcolor=GRIS_CLARO, gridwidth=1,
                linecolor=MORADO_WOM, linewidth=2, 
                ticks='outside', tickfont=dict(family='Arial', size=12)
            ),
            yaxis=dict(
                title=dict(text=f'Valor ({escala}/s)', font=dict(size=18, color=MORADO_WOM, family='Arial Black')),
                showgrid=True, gridcolor=GRIS_CLARO, gridwidth=1,
                linecolor=MORADO_WOM, linewidth=2,
                ticks='outside', tickfont=dict(family='Arial', size=12)
            ),
            legend=dict(
                orientation='h',  # Horizontal
                yanchor='top',
                y=-0.2,  # Posición debajo de la gráfica
                xanchor='left',
                x=0,  # Alineado a la izquierda
                font=dict(size=16),  # Letra grande
                bgcolor='rgba(255,255,255,0.5)',  # Fondo semitransparente
                bordercolor='Black',
                borderwidth=1
            ),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=60, r=30, t=50, b=100)  # Aumentado el margen inferior para la leyenda
        )

        return fig  
    
    @app.callback( # Callback que encuentra el maximo y lo actualiza en el layout, además genera el gráfico de maximo y average
        [Output('bwn-value', 'children'),
         Output('max_fecha', 'children'), 
         Output(component_id='max_avg_graf', component_property='figure'),
         Output('data-report', 'data')],
        Input("callback_active", "data"),
        State('time_tx', 'start_date'),
        State('time_tx', 'end_date'),
        State('granularidad_tx', 'value'),
        State("store-nodos", "data"),
        State("actual_tab", "active"),
        State('data-proyeccion', 'data'),
        State('data-actual', 'data'),
        prevent_initial_call=True
    )
    def update_max_bwn(active, start_date, end_date, interval, nodos_data, mode_actual, data_proyeccion, data_actual):
        if (nodos_data is None) or (not active):
            void_fig = go.Figure(data=[go.Scatter(x=[], y=[])])
            return "0 Mbps", '', void_fig, None
        def format_bandwidth(value):
            return round(value / 1000, 2)  # Convertir a Gbps y redondear a 2 decimales

        def obtener_escala(value):
            if value >= 1000:
                return value/1000, "Gbps"
            else:
                return value, "Mbps"

        if mode_actual == True:
            if data_actual is None:
                void_fig = go.Figure(data=[go.Scatter(x=[], y=[])])
                return "0 Mbps", '', void_fig, None
            trafiic_actual = pd.read_json(io.StringIO(data_actual), orient='split')
            traffic = trafiic_actual
            traffic['time'] = pd.to_datetime(traffic['time']) 
        else:
            if data_proyeccion is None:
                void_fig = go.Figure(data=[go.Scatter(x=[], y=[])])
                return "0 Mbps", '', void_fig, None
            trafiic_proyectado = pd.read_json(io.StringIO(data_proyeccion), orient='split')
            traffic = trafiic_proyectado
            traffic['time'] = pd.to_datetime(traffic['time']) 
            
        # Convertir el json a DataFrame
        df_nodos = pd.read_json(io.StringIO(nodos_data), orient='split')
        
        # Obtener el tráfico
        df_traffic_cluster = obtener_trafico_nodos_full(traffic, df_nodos, start_date, end_date)
        
        # Asegurarse de que la columna 'BWn' sea numérica
        df_traffic_cluster['BWn'] = pd.to_numeric(df_traffic_cluster['BWn'], errors='coerce')
        
        # Obtener el valor máximo y su fecha
        max_bwn = df_traffic_cluster['BWn'].max()
        fecha_max = df_traffic_cluster.loc[df_traffic_cluster['BWn'].idxmax(), 'time']
        
        max_bwn,escala = obtener_escala(max_bwn)
        # Formatear la fecha en español
        fecha_max_label = fecha_max.strftime("%H:%M, %d de %B de %Y")
        for mes_en, mes_es in meses.items():
            fecha_max_label = fecha_max_label.replace(mes_en, mes_es)
        if mode_actual == True:
            hovertemplate_avg = f"Avg: %{{y:.1f}} {escala}<br>Date: %{{x|%Y-%m-%d}}"
            hovertemplate_max = f"Max: %{{y:.1f}} {escala} <br>Date: %{{x|%Y-%m-%d}}"
        else:
            fecha_max_label = 'Proyección con los poorcentajes ingresados.'
            hovertemplate_avg = f"Avg: %{{y:.1f}} {escala}<br>Date: %{{x|%m-%d}}"
            hovertemplate_max = f"Max: %{{y:.1f}} {escala} <br>Date: %{{x|%m-%d}}"

        #Se convierte los datos en formato json con el objetivo de ponerlos en el layout y leerse desde otro punto 
        data_report_json = df_traffic_cluster.to_json(date_format='iso', orient='split')

        # Convertir valores a números
        if escala == 'Gbps':
            df_traffic_cluster['BWn'] = (df_traffic_cluster['BWn']/1000).round(2)
        else:
            df_traffic_cluster['BWn'] = df_traffic_cluster['BWn'].round(2)
        df_traffic_cluster['time'] = pd.to_datetime(df_traffic_cluster['time']) 

        if interval == 'dia':
            # Agrupar datos por día
            df_result = (
                df_traffic_cluster
                .groupby(df_traffic_cluster['time'].dt.date)
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )
        elif interval == 'semana':
            # Agrupar datos por semana (usar inicio de semana como agrupador)
            df_result = (
                df_traffic_cluster
                .groupby(pd.Grouper(key='time', freq='W-Mon'))
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )
        elif interval == 'mes':
            # Agrupar datos por mes
            df_result = (
                df_traffic_cluster
                .groupby(pd.Grouper(key='time', freq='MS'))
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )
        else:
            # Agrupar datos por día
            df_result = (
                df_traffic_cluster
                .groupby(df_traffic_cluster['time'].dt.date)
                .agg({
                    'BWn': ['mean', 'max']
                })
                .reset_index()
            )

        df_result.columns = ['time', 'BWn_mean', 'BWn_max']
        
        # Convertir columnas a los tipos correctos
        yDataAvg = pd.to_numeric(df_result["BWn_mean"],errors='coerce')
        yDataMax = pd.to_numeric(df_result["BWn_max"], errors='coerce')
        df_result["time"] = pd.to_datetime(df_result["time"], errors='coerce')
        
        # Crear figura
        fig_bh = go.Figure()
        # Agregar barras para Avg
        fig_bh.add_trace(go.Bar(
            x=df_result["time"].dt.date,
            y=yDataAvg,
            name="Avg",
            marker=dict(color=MORADO_WOM),
            hovertemplate=hovertemplate_avg
        ))

        # Agregar barras para Max
        fig_bh.add_trace(go.Bar(
            x=df_result["time"],
            y=yDataMax,
            name="Max",
            marker=dict(color=MORADO_CLARO),
            hovertemplate=hovertemplate_max
        ))

        # Configurar el diseño
        fig_bh.update_layout(
            title={
                'text': "Max and Avg Throughput",
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            xaxis_title="Date",
            yaxis_title=f"Capacity ({escala})",
            barmode='group',
            legend=dict(
                orientation="h",
                yanchor="top",
                xanchor="center",
                x=0.85
            ),
            margin=dict(l=25, r=20, t=20, b=10),  # Aumenta el margen inferior para acomodar la leyenda
            yaxis=dict(
                rangemode='tozero',
                autorange=True,
                tickformat='.1f'
            ),
        )
        return f"{round(max_bwn,2)} {escala}",fecha_max_label,fig_bh,data_report_json  # Mostrar redondeado y sin decimales

    @app.callback(
        [Output('select_node', 'options'), 
        Output('select_node', 'value')],  # Agregar output para el valor seleccionado
        Input('store-nodos', 'data')
    )
    def update_node(nodos_data):
        if nodos_data is None:
            return [], None

        # Convertir el json almacenado en un DataFrame utilizando StringIO
        df_nodos = pd.read_json(io.StringIO(nodos_data), orient='split')
        nodes = df_nodos['site_name']
        nodes = sorted(nodes)
        options = [{'label': node, 'value': node} for node in nodes]
        return options, options[0]['value'] if len(options) == 1 else None

    @app.callback(
        [Output('select_grupo', 'options'), 
        Output('select_grupo', 'value')],  # Agregar output para el valor seleccionado
        Input('aggregation_tx', 'value')
    )
    def update_grupo_options(aggregation):
        # Retorna opciones de grupo según la agregación seleccionada
        if aggregation == 'agregador_nal':
            agregadores_ext = agregadores_fibra[agregadores_fibra['tipo'] == 'agregador']
            agregadores = agregadores_ext['agr_name'].unique()
            agregadores = sorted(agregadores) 
            options = [{'label': agregador, 'value': agregador} for agregador in agregadores]
            return options, options[0]['value'] if len(options) == 1 else None
        elif aggregation == 'agregadores_wom':
            agregadores_wom = agregadores_fibra[agregadores_fibra['tipo'].isin(['agregador_wom', 'pre_agregador_wom'])]
            agregadores = agregadores_wom['agr_name'].unique()
            agregadores = np.append(agregadores, ['RED FO BOG', 'RED FO CAL','RED FO MED','CLI AGR Chipichape-RT3H1','CLI AGR Parcelaciones Pance C-RT3H1'])
            agregadores = sorted(agregadores) 
            options = [{'label': agregador, 'value': agregador} for agregador in agregadores]
            return options, options[0]['value'] if len(options) == 1 else None
        elif aggregation == 'core':
            cores = nodos_cores['agr_name'].unique()
            cores = np.append(cores, ['NE8000-X8-BOGOTA','NE8000-X8-MEDELLIN'])
            cores = sorted(cores)
            options = [{'label': core, 'value': core} for core in cores]
            return options, options[0]['value'] if len(options) == 1 else None
        elif aggregation == 'cluster':
            # Filtrar nodos donde 'carrier_tx' no sea 'WOM MW'
            clusters = nodos_totales[nodos_totales['carrier_tx'] != 'WOM MW']
            # Ordenar por la columna 'subnet_id'
            clusters = clusters.sort_values(by='subnet_id').reset_index(drop=True)
            subnet_ids = clusters['subnet_id']
            # Crear las opciones para el dropdown
            options = [{'label': f"{row['subnet_id']}_{row['site_name']}", 'value': row['subnet_id']} for _, row in clusters.iterrows() if row['subnet_id'] in subnet_ids]
            # Retornar las opciones y el valor predeterminado
            return options, options[0]['value'] if len(options) == 1 else None
        elif aggregation == 'ciudades_agr':
            ciudades = agregadores_fibra['agr_city'].unique()
            ciudades = sorted(ciudades)
            options = [{'label': ciudades, 'value': ciudades} for ciudades in ciudades]
            return options, options[0]['value'] if len(options) == 1 else None
        elif aggregation == 'total':
            options = [{'label': 'Red TX total', 'value': 'total_tx'}]
            return options, 'total_tx'  # Selección automática ya que solo hay una opción
        return [], None
    
