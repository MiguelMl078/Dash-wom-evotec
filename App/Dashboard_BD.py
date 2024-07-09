import dash
from dash import Dash, dcc, html, Input, Output, State, callback, Patch, no_update, ctx
from dash.exceptions import PreventUpdate

# Librerías adicionales de Dash
# import dash_leaflet as dl # Para hacer mapas con Leaflet
import dash_bootstrap_components as dbc # Para el Layout
from dash_bootstrap_templates import load_figure_template # Parte del layout, configura plantillas para todas las figuras
import dash_daq as daq # Para otros componentes, en este caso para un medido de nivel con aguja

import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2 # Para consulta a base de datos PostgreSQL
from psycopg2 import sql
# from unidecode import unidecode # Libreria para eliminar acentos y poder hace condicionales tranquilo
from datetime import datetime, timedelta, date
import datetime as dt
import numpy as np
# import json

# Scripts adicionales
import DBcredentials

#----------- Constantes -----------#
# Colores hexadecimal
MORADO_WOM = "#641f85"
MORADO_OSCURO = "#031e69"
MORADO_CLARO = "#cab2cd"
GRIS = "#8d8d8d"
MAGENTA = "#bb1677"
MAGENTA_OPACO = "#ac4b78"



#---------- Funciones Globales ----------#
# def to_float(n): # Función para convertir a float y convierte los NaN a 0
#     try:
#         return float(n)
#     except ValueError:
#         print (ValueError)
#         return 0

def to_int(n):
    # Esta función hace un casting a entero pero en dado caso que haya un NaN lo convierte en un o, pues mientras
    # una variable tipo float soporta NaN, un tipo int no lo hace
    try:
        return int(n)
    except ValueError:
        print (ValueError)
        return 0

def comprobacion_localidad(row): # Función para verificar si el código de localidad es correcto
    # La finalidad de esta función es evitar agrupar las celdas que contienen un error en su codigo DANE de 
    # localidades en la base de datos

    localidad_code = str(row["dwh_dane_cod_localidad"]) # Se extrae casilla con codigo DANE de localidad
    municipio_code = str(row["dane_code"]) # Se extrae casilla con codigo DANE de municipio
    
    # Verificar si el código del municipio está contenido al inicio del código de localidad, eso porque el
    # codigo de localidad se compone del codigo de municipio con numeros adicionales al final
    if localidad_code.startswith(municipio_code):
        return True
    else:
        return False
    
def area_metro(cell_name):
    # Esta función define la relación que hay entre el identificador de la celda y el area metropolitana a la
    # cual pertenece. Recibe nombre de celda, obtiene su identificador y devuelve el area metropolitana.

    # Diccionario relación identificador con areas metropolitanas
    areas_metropolitanas = {
        'ARM': 'Armenia AM',
        'CARM': 'Armenia AM',
        'AMB': 'Barranquilla AM',
        'BQL': 'Barranquilla AM',
        'CBQL': 'Barranquilla AM',
        'BTA': 'Bogota AM',
        'CBT': 'Bogota AM',
        'CBTA': 'Bogota AM',
        'AMS': 'Bucaramanga AM',
        'BUC': 'Bucaramanga AM',
        'CBUC': 'Bucaramanga AM',
        'CCLI': 'Cali AM',
        'CLI': 'Cali AM',
        'CAR': 'Cartagena AM',
        'CCAR': 'Cartagena AM',
        'AMC': 'Cucuta AM',
        'CCUC': 'Cucuta AM',
        'CUC': 'Cucuta AM',
        'CMAN': 'Manizales AM',
        'MAN': 'Manizales AM',
        'AMA': 'Medellin AM',
        'CMED': 'Medellin AM',
        'MED': 'Medellin AM',
        'CPER': 'Pereira AM',
        'CRI': 'Pereira AM',
        'PER': 'Pereira AM',
        'AMV': 'Valledupar AM',
        'CVDP': 'Valledupar AM',
        'VDP': 'Valledupar AM'
        }
    
    identificador_ciudad = cell_name.split()[0]  # Asume que el identificador de ciudad es la primera palabra
    
    # Retorna area metropolitana correspondiente según identificador, si no hay coincidencias retorna "Sin AM"
    return areas_metropolitanas.get(identificador_ciudad, 'Sin AM') 
    
def query_geodata(): 
    # Función para hacer query desde la base de datos de info geográfica. Luego hace el procesamiento correspondiente
    # para hacer el dataframe funcional

    try:
        conn = psycopg2.connect(**DBcredentials.BD_GEO_PARAMS) # Crear conexión a la base de datos

        # Crear un cursor
        cur = conn.cursor()

        query = """SELECT dwh_cell_name_wom, dwh_banda, dwh_sector, dwh_latitud, dwh_longitud, cluster_key, cluster_nombre, dwh_localidad, dwh_dane_cod_localidad, dane_nombre_mpio, dane_code, dane_code_dpto, dane_nombre_dpt, wom_regional 
                FROM bodega_analitica.roaming_cell_dim 
                WHERE dwh_operador_rat = 'WOM 4G' LIMIT 100000"""

        cur.execute(query) # Ejecutar la consulta
        datos = cur.fetchall() # Almacenar todas las filas de la consulta en esta variable
        columnas = [desc[0] for desc in cur.description]  # Obtener los nombres de las columnas

        cur.close() # Cerrar cursor

        df_geo = pd.DataFrame(datos, columns=columnas) # Creo dataframe

        df_geo = df_geo.dropna(subset="dwh_cell_name_wom")

        # Corrijo la columna que contiene el nombre de las celdas para que cuadre con los nombres de los informes
        df_geo["dwh_cell_name_wom"] = df_geo["dwh_cell_name_wom"].str.upper() # Todo a mayusculas
        df_geo["node_name"] = df_geo["dwh_cell_name_wom"]

        # Concatenar las columnas, reemplazando "B4" con "AWS" cuando sea necesario
        df_geo["dwh_cell_name_wom"] = np.where(df_geo["dwh_banda"] == "B4", # Cuando se cumpla esta condición
                                                df_geo["dwh_cell_name_wom"] + "_AWS_" + df_geo["dwh_sector"].astype(str), # Se aplica este fragmento
                                                df_geo["dwh_cell_name_wom"] + "_" + df_geo["dwh_banda"].astype(str) + "_" + df_geo["dwh_sector"].astype(str)) # Else
        df_geo = df_geo.drop_duplicates(subset=["dwh_cell_name_wom"]) # Elimino los nombres exactamente iguales
        df_geo["sector"] = df_geo["dwh_sector"].apply(lambda x: 1 if x in [1,4,7] else (2 if x in [2,5,8] else (3 if x in [3,6,9] else 4))) # Creación de columna "sector" para logica de agregación por sectores. Se agrupa según el id de sector
        df_geo["sector_name"] = df_geo["node_name"] + ": " + df_geo["sector"].astype(str)
        df_geo['AM'] = df_geo['dwh_cell_name_wom'].apply(area_metro) # Función para columna de area metropolitana

        return df_geo

    except Exception as e:
        print("Error al crear dataframe geográfico: ", e)
        return pd.DataFrame() # retorna dataframe vacio
    
    finally: # El bloque finally se ejcuta siempre sin importar si hubo o no excepción
        conn.close() # Cerrar conexión




#---------- Iniciar App ----------#
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css" # Hoja de estilo para los Dash Core Components
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.PULSE, dbc_css]) # Importo tema desde bootstrap
load_figure_template("pulse") # Función para que todos los gráficos tengan esta plantilla



#---------- Importar Datos ----------#
# Llamado a la función que lee y organiza datos geográficos
df_geo = query_geodata()

# Lectura de archivo que contiene las localidades
localidades = gpd.read_file("Localidades Crowdsourcing 2023/Crowdwourcing 2023/Localidades Finales mayo 18 v2.TAB")

# Leer el archivo GeoJSON de clusters con Geopandas
clusters = gpd.read_file("Clusterizacion.geojson")

# Leer el archivo GeoJSON de municipios con Geopandas
municipios = gpd.read_file("co_2018_MGN_MPIO_POLITICO.geojson")

# Leer el archivo GeoJSON de areas metropolitanas con Geopandas
areas_metro = gpd.read_file("AreasMetro.geojson")

# Leer el archivo GeoJSON de departamentos con Geopandas
departamentos = gpd.read_file("co_2018_MGN_DPTO_POLITICO.geojson")

# Leer el archivo GeoJSON de regionales con Geopandas
regionales = gpd.read_file("Regional_test.geojson")




#---------- App layout ----------#
app.layout = dbc.Container([ # El layout sigue un estilo html
    # Todo tiene una organización tipo tabla con filas y columnas
    dbc.Row([ # Fila
        dbc.Col([ # Columna dentro de la fila
            html.H1("Herramienta: DashWOM" ),
        ], width=12, className="bg-primary text-white p-2 mb-2 text-center")
    ]
    ),

    dbc.Row([ # Nueva fila, siempre va a estar debajo de la fila anterior
        dbc.Col([
            dbc.Card([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Agregación Geográfica"),
                        dcc.Dropdown(id="aggregation",
                                    options=[
                                        {"label": "Celda", "value": "celda"},
                                        {"label": "Sector", "value": "sector"},
                                        {"label": "Estación Base", "value": "EB"},
                                        {"label": "Cluster", "value": "cluster"},
                                        {"label": "Localidad", "value": "localidad"},
                                        {"label": "Municipio", "value": "municipio"},
                                        {"label": "Área Metropolitana", "value": "AM"},
                                        {"label": "Departamento", "value": "departamento"},
                                        {"label": "Regional", "value": "regional"},
                                        {"label": "Total", "value": "total"}],
                                    value="total",
                                    clearable=False,
                                    )
                    ], width=6),
                    dbc.Col([
                        dbc.Label("Selección"),
                        dcc.Dropdown(
                            id="select",
                            placeholder="Selecciona un punto o polígono",
                            value="Total de la red"
                                    ),
                    ], width=6)
                ], style={"height": "45%"}),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Tiempo"),
                        dcc.DatePickerRange(id="time",
                                            display_format='YYYY-MM-DD',
                                )
                    ], width=6),
                    dbc.Col([
                        dbc.Label("Granularidad"),
                        dcc.Dropdown(id="time_agg",
                                    options=[
                                        {"label": "Hora", "value": "hora"},
                                        {"label": "Día", "value": "dia"},
                                        {"label": "Semana", "value": "semana"},
                                        {"label": "Mes", "value": "mes"}],
                                    value="dia",
                                    clearable=False,
                                )
                    ], width=6)
                ], style={"height": "45%"}, align="center"),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(children="Reporte de celdas", id="reporte", n_clicks=0),
                    ], width=3),
                    dbc.Col([
                        dcc.Loading(children=dcc.Download(id="download_report"), color=MORADO_WOM),
                    ], width=1),
                    dbc.Col([
                        dbc.Button(children="Buscar", id="solicitar", n_clicks=0)
                    ], width={"size":2,"offset":6}), # Offset de 6 casillas para que quede bien a la derecha
                ], style={"height": "10%"}, align="center") # justify=end para que quede al final de la columna
                
            ], body=True, style={"height": "100%"})
        ], width=8, align="center", style={"height": "95%"}),
        
        dbc.Col([
            dbc.Card([
                daq.Gauge(
                    id="gauge",
                    label="Capacidad",
                    color={"gradient":True,"ranges":{"green":[0,50],"yellow":[50,80],"red":[80,100]}},
                    min=0,
                    max=100,
                    value=0,
                    showCurrentValue=True,
                    # size=100,
                    style={"height": "100%"}
                    )
            ], body=True, style={"height": "100%"})
        ], width=4, align="center", style={"height": "95%"})
    ], style={"height": "50%"}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dcc.Loading( # Componente para mostrar estado de carga
                    children=[html.Div(id="test", children="Hola, haz una selección", style={"height": "100%"})],
                    color="white"
                )
            ], className="bg-primary text-white p-2 mb-2 text-center", style={"height": "100%"})
        ], width=12, align="center", style={"height": "100%"})
    ], justify="center", style={"height": "8%"}),

    dbc.Row([
        dbc.Col([
            dbc.Card([ # dcc.Loadingo para que cargue mientras un callback está actualizando su estado
                dcc.Loading([dcc.Graph(id="map", # Si no le pongo los corchetes el mapa no ocupa todo su espacio destinado
                                      style={"height": "100%"}
                                      )], color=MORADO_WOM, type="circle",
                                      overlay_style={"visibility":"hidden", "height":"100%"}) # Para que mientras cargue se ponga borroso y se ajuste al tamaño de la tarjeta
            ], style={"height": "100%"})
        ], width=6, style={"height": "100%"}),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dcc.Graph(id="bh", style={"height": "100%"})
                    ], style={"height": "100%"})
                ], width=6, style={"height": "100%"}),
                dbc.Col([
                    dbc.Card([
                        dcc.Graph(id="PRB", style={"height": "100%"})
                    ], style={"height": "100%"})
                ], width=6, style={"height": "100%"}),
            ], style={"height": "50%"}, align="center"),
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dcc.Graph(id="traffic", style={"height": "100%"})
                    ], style={"height": "50%"})
                ], width=6, style={"height": "100%"}),
                dbc.Col([
                    dbc.Card([
                        dcc.Graph(id="user_exp", style={"height": "100%"})
                    ], style={"height": "50%"})
                ], width=6, style={"height": "100%"}),
            ], style={"height": "100%"}, align="center")
        ], width=6, style={"height": "100%"})
    ], style={"height": "80%"}),

    dbc.Row([
        dbc.Col([
            dcc.Dropdown(id="select_graph",
                        options=[
                            {"label": "Active Users", "value": "BH"},
                            {"label": "PRB Occupation", "value": "PRB"},
                            {"label": "Traffic", "value": "Traffic"},
                            {"label": "User Experience", "value": "u_exp"}],
                        # placeholder="Select a KPI"
                        value="PRB",
                        clearable=False,
                        ),
        ], width=6, align="center"),

        dbc.Col([
            dbc.Button(id="update_kpi", n_clicks=0, children="Update Map"),
            dbc.Button(id="fullscreen", n_clicks=0, children="Full Screen", style={"margin-left": "5%"}),
            dbc.Button(id="download", n_clicks=0, children="Download", style={"margin-left": "5%"}),
            dcc.Download(id="download_file"),
        ], width=6, align="center"),
    ]),

    dbc.Row([
        dbc.Col([
            html.Div(id="map_text", children="No se ha seleccionado KPI en el mapa", style={"height": "100%"}),
        ], width=6, align="center")
    ]),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id="graph_test", style={"height": "100%"}),
        ], style={"height": "100%"})
    ], style={"height": "100%"}),

    ],
    className="dbc",
    fluid=True, # Para que se los componenetes se adapten segun la pantalla
    style={"height": "100vh"}
)



#---------- Callbacks ----------#
# callback para actualizar las fechas dinámicamente cada vez que se carga la aplicación
@app.callback(
    Output('time', 'start_date'),
    Output('time', 'end_date'),
    Input('time', 'id')  # Usamos un Input ficticio para que el callback se llame al cargar la app
)
def update_date_range(_):
    today = datetime.today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=30)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d") # strftime convierte a string con el formato definido como parametro



# Callback para poner en blanco los gráficos con el cambio de agregación
@callback(
        Output(component_id='test', component_property='children', allow_duplicate=True),
        Output(component_id="gauge", component_property="value", allow_duplicate=True),
        Output(component_id='traffic', component_property='figure', allow_duplicate=True),
        Output(component_id='user_exp', component_property='figure', allow_duplicate=True),
        Output(component_id='bh', component_property='figure', allow_duplicate=True),
        Output(component_id='PRB', component_property='figure', allow_duplicate=True),

        Input(component_id="aggregation", component_property='value'),
        prevent_initial_call=True # Para que no me genere la alerta de salida duplicada
)
def funct(input):
    container = f"Selecciona un punto o polígono"
    void_fig = go.Figure(data=[go.Scatter(x=[], y=[])]) # Figura vacia

    return container, None, void_fig, void_fig, void_fig, void_fig # Se actualiza mapa, salido de texto y se retornan gráficos vacios




# Callback para generar las opciones de marcador o poligono según la agregación
@callback(
    Output(component_id='select', component_property='options'),
    Input(component_id="aggregation", component_property='value')
)
def update_dropdown(input):
    if input == "celda":
        options_df = df_geo["dwh_cell_name_wom"].copy() # Genero copia del df únicamente de la columna que contiene el nombre de las celdas
        options_df = options_df.dropna().sort_values() # Elimino valores nulos y los organizo
        options = [{'label': i, 'value': i} for i in options_df]

    elif input == "sector":
        options_df = df_geo["sector_name"].drop_duplicates().copy() # Genero copia del df de las columnas con el nombre del nodo y su sector
        options_df = options_df.dropna().sort_values() # Elimino nombres duplicados y organizo por mismo nombre
        options = [{'label': i, 'value': i} for i in options_df]

    elif input == "EB":
        options_df = df_geo["node_name"].drop_duplicates().copy() # Genero copia de las columnas con el nombre de nodos quitando los duplicados
        options_df = options_df.dropna().sort_values() # Elimino casillas nulas y organizo
        options = [{'label': i, 'value': i} for i in options_df]

    elif input == "cluster":
        options_df = df_geo["cluster_key"].drop_duplicates().copy() # Genero copia unicamente de la columna "Cluster" y elimino los duplicados con .drop_duplicates
        options_df = options_df.dropna().sort_values()
        options = [{'label': i, 'value': i} for i in options_df]
    
    elif input == "localidad":
        options_df = df_geo[["dwh_localidad", "dwh_dane_cod_localidad", "dane_nombre_mpio", "dane_code"]].copy() # Copia solo columnas requeridas
        options_df = options_df.dropna() # Elimino filas que contenga algun valor nulo

        filas_correctas = options_df.apply(comprobacion_localidad, axis=1) # Aplicar función para comprobar localidades a cada fila del df
        options_df = options_df[filas_correctas] # Filtrar filas con códigos de localidad correctas
        options_df = options_df.drop_duplicates(subset=["dwh_dane_cod_localidad"])

        options_df["CoLoc"] = options_df["dane_nombre_mpio"] + ": " + options_df["dwh_localidad"] + " " + options_df["dwh_dane_cod_localidad"].astype(str) # Nueva columna con nombre único de localidad
        options_df = options_df.sort_values(by=["CoLoc"]) # Organizo por nombre único
        options = [{'label': row["CoLoc"], 'value': row["dwh_dane_cod_localidad"]} for index, row in options_df.iterrows()]

    elif input == "municipio":
        # Tomo código DANE porque a partir del código es que funciona la lógica de los municipios
        options_df = df_geo[["dane_code","dane_nombre_mpio"]].copy() # El parametro dentro de .drop_duplicates es para que considere solo las filas duplicadas según el código
        options_df = options_df.dropna() # Elimino filas que contenga algun valor nulo
        options_df = options_df.drop_duplicates(subset=["dane_code"]) # Elimino duplicados
        options_df["CoMpo"] = options_df["dane_nombre_mpio"] + " " + options_df["dane_code"].apply(str) # Sumo nombre de municipio con código para poder generar las opciones
        options_df = options_df.sort_values(by=["CoMpo"]) # Organizo por nombre único
        options = [{'label': row["CoMpo"], 'value': row["dane_code"]} for index, row in options_df.iterrows()]

    elif input == "AM":
        options_df = df_geo["AM"].drop_duplicates().copy() # El parametro dentro de .drop_duplicates es para que considere solo las filas duplicadas según el código
        options_df = options_df[options_df != "Sin AM"] # Mantener filas donde el valor de la columna AM sea diferente a "Sin AM"
        options_df = options_df.dropna().sort_values()
        options = [{'label': i, 'value': i} for i in options_df]

    elif input == "departamento":
        options_df = df_geo[["dane_code_dpto","dane_nombre_dpt"]].copy() # El parametro dentro de .drop_duplicates es para que considere solo las filas duplicadas según el código
        options_df = options_df.dropna() # Elimino filas que contenga algun valor nulo
        options_df = options_df.drop_duplicates(subset=["dane_code_dpto"]) # Elimino duplicados
        options_df["CoDpto"] = options_df["dane_nombre_dpt"] + " " + options_df["dane_code_dpto"].apply(str) # Sumo nombre de municipio con código para poder generar las opciones
        options_df = options_df.sort_values(by=["CoDpto"]) # Organizo
        options = [{'label': row["CoDpto"], 'value': row["dane_code_dpto"]} for index, row in options_df.iterrows()]

    elif input == "regional":
        options_df = df_geo["wom_regional"].drop_duplicates().copy() # El parametro dentro de .drop_duplicates es para que considere solo las filas duplicadas según el código
        options_df = options_df.dropna().sort_values()
        options = [{'label': i, 'value': i} for i in options_df]

    elif input == "total":
        options = [{'label': 'Total de la red', 'value': "Total de la red"}]
    
    return options



# Callback para realizar la selección
@callback(
        Output(component_id='select', component_property='value'),
        Input(component_id='map', component_property='clickData')
)
def make_selection(input):
    if input is None:
        raise PreventUpdate
    
    print(input)
    selected = input['points'][0]["customdata"][0] # Accedo a la información que mandé en el mapa

    return selected



# Callback para realizar zoom en el mapa según la selección
@callback(
        Output(component_id='map', component_property='figure', allow_duplicate=True), # Voy a usar está misma salida en otro callback
        Input(component_id='select', component_property='value'),
        State(component_id="aggregation", component_property='value'),
        prevent_initial_call=True # Para que no me genere la alerta de salida duplicada
)
def make_zoom(input, agg):
    print("Input en función makezooom: ", input)
    if input is None:
        raise PreventUpdate # No modifica ninguna salida

    zoom = 14
    # # Coordenadas Ecotek
    # lat_mean = 4.6837
    # lon_mean = -74.0566

    if agg == "celda":
        auxdf = df_geo[["dwh_cell_name_wom","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["dwh_cell_name_wom"] == input] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "sector":
        auxdf = df_geo[["sector_name","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["sector_name"] == input] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "EB":
        auxdf = df_geo[["node_name","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["node_name"] == input] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean() # Promedio de coordenadas de todas las celdas que componen el grupo
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "cluster":
        zoom = 13
        auxdf = df_geo[["cluster_key","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["cluster_key"] == input] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "localidad":
        zoom = 12
        auxdf = df_geo[["node_name","dwh_dane_cod_localidad","dwh_latitud","dwh_longitud","dane_code"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["dwh_dane_cod_localidad"] == to_int(input)] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas

        filas_correctas = auxdf.apply(comprobacion_localidad, axis=1) # Aplicar función para comprobar localidades a cada fila del df
        auxdf = auxdf[filas_correctas] # Filtrar filas con códigos de localidad correctas

        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "municipio":
        zoom = 10
        auxdf = df_geo[["dane_code","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["dane_code"] == to_int(input)] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "AM":
        zoom = 10
        auxdf = df_geo[["AM","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["AM"] == input] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "departamento":
        zoom = 8
        auxdf = df_geo[["dane_code_dpto","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["dane_code_dpto"] == to_int(input)] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "regional":
        zoom = 6
        auxdf = df_geo[["wom_regional","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["wom_regional"] == input] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "total":
        raise PreventUpdate # No modifica ninguna salida

    if auxdf.empty:
        raise PreventUpdate # No modifica ninguna salida si no encuentra coincidencias
    
    patched_figure = Patch() # Patch para actualizar el atributo de una figura sin tener que crear la de nuevos
    patched_figure['layout']['mapbox']['zoom'] = zoom # Ruta para modificar el zoom
    patched_figure['layout']['mapbox']['center']['lat'] = lat_mean # Ruta para modificar atributo de latitud
    patched_figure['layout']['mapbox']['center']['lon'] = lon_mean # Ruta para modificar atributo de longitud


    return patched_figure


# Callback para descargar reporte de KPIs de cada celda dentro del rango de fechas
@callback(
        Output(component_id='download_report', component_property='data'),

        Input(component_id='reporte', component_property='n_clicks'),

        State(component_id="time", component_property='start_date'),
        State(component_id="time", component_property='end_date'),
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
)
def download_report(boton, start_date, end_date):
    if boton is None:  # Se debe presionar el boton para que se actualice el callback
        raise PreventUpdate
    try:
        # Conectarse a la base de datos
        conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)
        
        # Crear un cursor
        cur = conn.cursor()

        # Realizar consulta a la base de datos PostgreSQL dentro del rango de fechas seleccionado
        cur = conn.cursor()
        cur.execute("""SELECT "Date","BH","cell_name","avg_users_BH","daily_max_users","max_users_hour","PRBusage_BH_DL","PRBusage_BH_UL","traffic_bh(GB)","traffic_avg(GB)","traffic_total(GB)","uexp_BH(Mbps)"
                    FROM "ran_kpi_cell" 
                    WHERE "Date" BETWEEN %s AND %s""", (start_date, end_date,))
        rows = cur.fetchall()
        columnas = ["Date","BH","cell_name","avg_users_BH","daily_max_users","max_users_hour","PRBusage_BH_DL","PRBusage_BH_UL","traffic_bh(GB)","traffic_avg(GB)","traffic_total(GB)","uexp_BH(Mbps)"]
        df = pd.DataFrame(rows, columns=columnas)

        if df.empty: # Si el dataframe está vacio se levanta una excepción
            raise Exception("No se han retornado datos. Dataframe vacio")
        
        file_name = f"KPI_Report_{start_date}-{end_date}.csv"
        return dcc.send_data_frame(df.to_csv, file_name, index=False)
        
    except Exception as e:
        print("Error al momento de descargar reporte de KPIs: ", e)
        raise PreventUpdate
    
    finally:
        cur.close()
        conn.close()








#------------------------------------------ FUNCIONES CALLBACK GENERACIÓN DE GRÁFICOS ----------------------------------------------------------#
    

def bh(data, column): # Calculo BH(hora pico) por día
    data = data.copy()
    bh_day = data.groupby(data['Timestamp'].dt.date)[column].idxmax() # Agrupo los datos por fecha y luego encuentro los indices que contienen los valores de tráfico maximos (BH)
    bh_df = data.loc[bh_day, ['Timestamp', column]] # Creo un nuevo df con las horas pico por día y unicamente con las columnas de tiempo y tráfico

    return bh_df

def graph_BH(bh_df_avg, bh_df_max):
    fig = go.Figure() # Crea una figura vacía
    bh_df_avg = bh_df_avg.reset_index(drop=True)
    bh_df_avg['Date'] = bh_df_avg['Timestamp'].dt.date # Vuelvo a crear columna Date y Time que se habían perdido con el fin de gráficar respecto a la fecha y mostrar la hora
    bh_df_avg['Time'] = bh_df_avg['Timestamp'].dt.strftime('%H:%M') # Formato para que solo sea Hora y Minuto
    fig.add_trace(go.Bar(x=bh_df_avg["Date"], y=bh_df_avg["L.Traffic.ActiveUser.DL.Avg"], name="Avg", text=bh_df_avg["Time"], marker=dict(color=MORADO_WOM)))

    bh_df_max = bh_df_max.reset_index(drop=True)
    bh_df_max['Date'] = bh_df_max['Timestamp'].dt.date # Vuelvo a crear columna Date y Time que se habían perdido con el fin de gráficar respecto a la fecha y mostrar la hora
    bh_df_max['Time'] = bh_df_max['Timestamp'].dt.strftime('%H:%M') # Formato para que solo sea Hora y Minuto
    fig.add_trace(go.Bar(x=bh_df_max["Date"], y=bh_df_max["L.Traffic.ActiveUser.DL.Max"], name="Max", text=bh_df_max["Time"], marker=dict(color=MORADO_CLARO)))
    return fig

def PRB_usg(data, bh_df):
    data = data[data["Timestamp"].isin(bh_df["Timestamp"])].copy() # Genero copia del df de datos unicamente de las casillas dentro del BH
    prb_df = data[["Timestamp", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"]] # Solo columnas necesarias
    prb_df = prb_df.reset_index(drop=True)

    prb_df["DL_PRB_usage"] = (prb_df["L.ChMeas.PRB.DL.Used.Avg"] / prb_df["L.ChMeas.PRB.DL.Avail"]) * 100 # Cálculo de % ocupación en downlink y guardado en nueva columna
    prb_df["UL_PRB_usage"] = (prb_df["L.ChMeas.PRB.UL.Used.Avg"] / prb_df["L.ChMeas.PRB.UL.Avail"]) * 100 # # Cálculo de % ocupación en uplink y guardado en nueva columna

    return prb_df

def bit_to_GB(bit):
    gbyte = bit / (8*10**9)
    return gbyte

def graph_prb(prb_df):
    fig_prb = go.Figure() # Crea una figura vacía
    fig_prb.add_trace(go.Scatter(x=prb_df["Timestamp"], y=prb_df["DL_PRB_usage"], mode='lines', name='Downlink', line=dict(color=MORADO_WOM)))
    fig_prb.add_trace(go.Scatter(x=prb_df["Timestamp"], y=prb_df["UL_PRB_usage"], mode='lines', name='Uplink', line=dict(color=MAGENTA)))
    return fig_prb

def traffic(data, bh_df):
    trff_df = data.copy()

    trff_avg_df = trff_df.groupby(trff_df["Timestamp"].dt.date)["L.Thrp.bits.DL(bit)"].mean().reset_index() # Promedio del tráfico de cada hora del día
    trff_avg_df["L.Thrp.bits.DL(bit)"] = trff_avg_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversion de bit a GB

    trff_sum_df = trff_df.groupby(trff_df["Timestamp"].dt.date)["L.Thrp.bits.DL(bit)"].sum().reset_index() # Suma del tráfico de cada hora del día
    trff_sum_df["L.Thrp.bits.DL(bit)"] = trff_sum_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversion de bit a GB

    # Calculo de tráfico en BH
    trff_bh = data[data["Timestamp"].isin(bh_df["Timestamp"])].copy() # Genero copia del df de datos unicamente de las casillas dentro del BH
    trff_bh = trff_bh[["Timestamp", "L.Thrp.bits.DL(bit)"]] # Solo columnas necesarias
    trff_bh["L.Thrp.bits.DL(bit)_BH"] = trff_bh["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversión de bit a GB
    trff_bh = trff_bh.reset_index(drop=True) # Reiniciar indices sin insertar indices antiguos en columna nueva

    return trff_avg_df, trff_sum_df, trff_bh

def graph_trff(trff_avg_df, trff_sum_df, trff_bh):
    fig_trff = go.Figure() # Crea una figura vacía
    fig_trff.add_trace(go.Bar(x=trff_sum_df["Timestamp"], y=trff_sum_df["L.Thrp.bits.DL(bit)"], name='Total_Traffic', yaxis="y2", marker=dict(color=MORADO_CLARO)))
    fig_trff.add_trace(go.Scatter(x=trff_avg_df["Timestamp"], y=trff_avg_df["L.Thrp.bits.DL(bit)"], mode='lines', name='Avg_Traffic', line=dict(color=MAGENTA))) # Añado linea de tráfico al día
    fig_trff.add_trace(go.Scatter(x=trff_bh["Timestamp"], y=trff_bh["L.Thrp.bits.DL(bit)_BH"], mode='lines', name='Traffic_BH', line=dict(color=MORADO_WOM))) # Agrega la segunda línea a la misma figura
    return fig_trff

def user_exp(data, bh_df):
    data = data[data["Timestamp"].isin(bh_df["Timestamp"])].copy()
    user_exp_df = data[["Timestamp","L.Thrp.bits.DL(bit)", "L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]] # Solo columnas necesarias

    user_exp_df = user_exp_df.reset_index(drop=True)
    user_exp_df["User_Exp"] = ((user_exp_df["L.Thrp.bits.DL(bit)"]-user_exp_df["L.Thrp.bits.DL.LastTTI(bit)"]) / (user_exp_df["L.Thrp.Time.DL.RmvLastTTI(ms)"])) / 1024 # Calculo user experience
    
    return user_exp_df
    
def convert_timestamp(timestamp_str):
    try:
        # Intentar convertir el Timestamp a datetime directamente
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Si falla, asumir que la hora es 00:00:00 y agregar ese componente
        return datetime.strptime(timestamp_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    
def query_to_df(seleccion, geo_agregacion, start_date, end_date):
    try:
        conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)

        # Realizar consulta a la base de datos PostgreSQL dentro del rango de fechas seleccionado
        cur = conn.cursor()

        if geo_agregacion == "celda":
            cur.execute("""SELECT "Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_cell" 
                    WHERE UPPER("Cell_name") = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        
        elif geo_agregacion == "sector":
            cur.execute("""SELECT "Timestamp","sector_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_sector" 
                    WHERE UPPER("sector_name") = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","sector_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        
        elif geo_agregacion == "EB":
            cur.execute("""SELECT "Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_node" 
                    WHERE UPPER("node_name") = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        
        elif geo_agregacion == "cluster":
            cur.execute("""SELECT "Timestamp","cluster_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_cluster" 
                    WHERE "cluster_name" = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","cluster_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        
        elif geo_agregacion == "localidad":
            cur.execute("""SELECT "Timestamp","localidad_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_localidad" 
                    WHERE "localidad_dane_code" = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","localidad_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        
        elif geo_agregacion == "municipio":
            cur.execute("""SELECT "Timestamp","municipio_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_municipio" 
                    WHERE "municipio_dane_code" = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","municipio_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]

        elif geo_agregacion == "AM":
            cur.execute("""SELECT "Timestamp","am_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_am" 
                    WHERE "am_name" = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","am_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]

        elif geo_agregacion == "departamento":
            cur.execute("""SELECT "Timestamp","dpto_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_departamento" 
                    WHERE "dpto_dane_code" = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","dpto_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]

        elif geo_agregacion == "regional":
            cur.execute("""SELECT "Timestamp","regional_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_regional" 
                    WHERE "regional_name" = %s
                    AND DATE("Timestamp") BETWEEN %s AND %s""", (seleccion, start_date, end_date))
            columnas = ["Timestamp","regional_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]

        elif geo_agregacion == "total":
            cur.execute("""SELECT "Timestamp","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)" 
                    FROM "ran_1h_total" 
                    WHERE DATE("Timestamp") BETWEEN %s AND %s""", (start_date, end_date))
            columnas = ["Timestamp","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]

        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columnas)
        df = df.sort_values(by="Timestamp")
        cur.close()

        return df
    
    except Exception as e:
        print("Error al obtener información de la selección: ", e)
        return pd.DataFrame()
    
    finally:
        conn.close() # Siempre se va a cerrar la conexión sin importar si hubo excepción o no. Buena práctica por si hay un error

# Callback para gráficar la selección del usuario a partir de la agregación
@callback(
        Output(component_id='test', component_property='children'),
        Output(component_id="gauge", component_property="value"),
        Output(component_id='traffic', component_property='figure'),
        Output(component_id='user_exp', component_property='figure'),
        Output(component_id='bh', component_property='figure'),
        Output(component_id='PRB', component_property='figure'),
        Output(component_id='graph_test', component_property='figure'),

        Input(component_id='solicitar', component_property='n_clicks'),

        State(component_id="aggregation", component_property='value'),
        State(component_id='select', component_property='value'),
        State(component_id="time_agg", component_property='value'),
        State(component_id="time", component_property='start_date'),
        State(component_id="time", component_property='end_date'),

        running=[(Output(component_id='solicitar', component_property='disabled'), True, False)] # Mientras el callback esté corriendo desactiva el botón
)
def update_graphs(boton, geo_agg, selected_cell, time_agg, start_date, end_date):
    if boton is None:  # Se debe presionar el boton para que se actualice el callback
        raise PreventUpdate
    
    if (start_date is None) and (end_date is None): # Esta condición solo se cumple en el primer llamado de la app
        today = datetime.today()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=30)
        end_date = end_date.strftime("%Y-%m-%d") # Paso a string
        start_date = start_date.strftime("%Y-%m-%d")
    
    if selected_cell is None:
        container = "Por favor haz una selección"
        return container, no_update, no_update, no_update, no_update, no_update, no_update # Solo se actualiza la salida de texto, el resto se queda igual

    container = f"Su selección es: {selected_cell} en el rango de fechas {start_date} -> {end_date}"

    data = query_to_df(selected_cell, geo_agg, start_date, end_date) # Función que hace la consulta a la base de datos
    if data.empty:
        container = f"No hay datos para su selección {selected_cell} en el rango de fechas {start_date} - {end_date}"
        fig = go.Figure(data=[go.Scatter(x=[], y=[])]) # Figura vacia
        return container, None, fig, fig, fig, fig, fig # Se actualiza salido de texto y se retornan gráficos vacios

    if time_agg == "hora":
        # Graficar por hora
        bh_df = data[["Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.Traffic.ActiveUser.DL.Max"]].copy()
        bh_df['Date'] = bh_df['Timestamp'].dt.date # Vuelvo a crear columna Date y Time que se habían perdido con el fin de gráficar respecto a la fecha y mostrar la hora
        bh_df['Time'] = bh_df['Timestamp'].dt.strftime('%H:%M') # Formato para que solo sea Hora y Minuto
        fig_bh = go.Figure() # Crea una figura vacía
        fig_bh.add_trace(go.Scatter(x=bh_df["Timestamp"], y=bh_df["L.Traffic.ActiveUser.DL.Avg"], mode="lines", name="Avg"))
        fig_bh.add_trace(go.Scatter(x=bh_df["Timestamp"], y=bh_df["L.Traffic.ActiveUser.DL.Max"], mode="lines", name="Max"))

        # Ocupación PRB por hora
        prb_df = data[["Timestamp", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"]].copy()# Solo columnas necesarias
        prb_df = prb_df.reset_index(drop=True)
        prb_df["DL_PRB_usage"] = (prb_df["L.ChMeas.PRB.DL.Used.Avg"] / prb_df["L.ChMeas.PRB.DL.Avail"]) * 100 # Cálculo de % ocupación en downlink y guardado en nueva columna
        prb_df["UL_PRB_usage"] = (prb_df["L.ChMeas.PRB.UL.Used.Avg"] / prb_df["L.ChMeas.PRB.UL.Avail"]) * 100 # # Cálculo de % ocupación en uplink y guardado en nueva columna
        fig_prb = graph_prb(prb_df)
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Gráfica de tráfico
        trff_df = data[["Timestamp","L.Thrp.bits.DL(bit)"]].copy() # Solo columnas necesarias
        trff_df["L.Thrp.bits.DL(bit)"] = trff_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # De bits a GB
        fig_trff = go.Figure(data=go.Scatter(x=trff_df["Timestamp"], y=trff_df["L.Thrp.bits.DL(bit)"], mode="lines", name="Traffic"))

        # Gráfica de user experience
        user_exp_df = data[["Timestamp","L.Thrp.bits.DL(bit)", "L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]].copy() # Solo columnas necesarias
        user_exp_df = user_exp_df.reset_index(drop=True)
        user_exp_df["User_Exp"] = ((user_exp_df["L.Thrp.bits.DL(bit)"]-user_exp_df["L.Thrp.bits.DL.LastTTI(bit)"]) / (user_exp_df["L.Thrp.Time.DL.RmvLastTTI(ms)"])) / 1024 # Calculo user experience
        fig_uexp = go.Figure(data=go.Scatter(x=user_exp_df["Timestamp"], y=user_exp_df["User_Exp"], mode="lines", name="U_exp"))
    
    else: # Si es una agregación temporal diferente de hora

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados

        # Calculo y grafica de tráfico
        trff_avg_df, trff_sum_df, trff_bh = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        user_exp_df = user_exp(data, bh_df)

        if time_agg == "semana":
            # BH(Hora pico)
            # Agrupar los datos por semana y calcular los promedios
            agg_bh_df = bh_df.resample('W-Mon', on='Timestamp').mean().reset_index() # W-Mon significa que la semana empieza el lunes
            agg_bh_df_max = bh_df_max.resample('W-Mon', on='Timestamp').mean().reset_index()

            fig_bh = go.Figure() # Crea una figura vacía
            fig_bh.add_trace(go.Bar(x=agg_bh_df["Timestamp"], y=agg_bh_df["L.Traffic.ActiveUser.DL.Avg"], name="Avg", marker=dict(color=MORADO_WOM)))
            fig_bh.add_trace(go.Bar(x=agg_bh_df_max["Timestamp"], y=agg_bh_df_max["L.Traffic.ActiveUser.DL.Max"], name="Max", marker=dict(color=MORADO_CLARO)))
            print("Usuarios semana agregado")

            # Ocupación PRB
            agg_prb_df = prb_df.resample('W-Mon', on='Timestamp').mean().reset_index()
            fig_prb = graph_prb(agg_prb_df)
            print("Ocupacion PRB semana agregado")

            # Trafico
            trff_avg_df["Timestamp"] = pd.to_datetime(trff_avg_df['Timestamp']) # Convierto a timestamp de nuevo porque me estaba generando un error
            trff_sum_df["Timestamp"] = pd.to_datetime(trff_sum_df['Timestamp'])
            agg_trff_df = trff_avg_df.resample('W-Mon', on='Timestamp').mean().reset_index()
            agg_trff_sum_df = trff_sum_df.resample('W-Mon', on='Timestamp').sum().reset_index()
            agg_trff_df_bh = trff_bh.resample('W-Mon', on='Timestamp').mean().reset_index()
            fig_trff = graph_trff(agg_trff_df, agg_trff_sum_df, agg_trff_df_bh)
            print("Trafico semana agregado")

            # User experience
            agg_uexp_df = user_exp_df.resample('W-Mon', on='Timestamp').mean().reset_index()
            fig_uexp = go.Figure(data=go.Scatter(x=agg_uexp_df["Timestamp"], y=agg_uexp_df["User_Exp"], mode="lines", name="U_exp"))
            print("User experience semana agregado")

        elif time_agg == "mes":
            # BH(Hora pico)
            # Agrupar los datos por semana y calcular los promedios
            agg_bh_df = bh_df.resample('MS', on='Timestamp').mean().reset_index() # MS significa inicio de mes (calendar month begin)
            agg_bh_df_max = bh_df_max.resample('MS', on='Timestamp').mean().reset_index()

            fig_bh = go.Figure() # Crea una figura vacía
            fig_bh.add_trace(go.Bar(x=agg_bh_df["Timestamp"], y=agg_bh_df["L.Traffic.ActiveUser.DL.Avg"], name="Avg", marker=dict(color=MORADO_WOM)))
            fig_bh.add_trace(go.Bar(x=agg_bh_df_max["Timestamp"], y=agg_bh_df_max["L.Traffic.ActiveUser.DL.Max"], name="Max", marker=dict(color=MORADO_CLARO)))
            print("Usuarios mes agregado")

            # Ocupación PRB
            agg_prb_df = prb_df.resample('MS', on='Timestamp').mean().reset_index()

            fig_prb = graph_prb(agg_prb_df)
            print("Ocupacion PRB mes agregado")
            
            # Trafico
            trff_avg_df["Timestamp"] = pd.to_datetime(trff_avg_df['Timestamp']) # Convierto a timestamp de nuevo porque me estaba generando un error
            trff_sum_df["Timestamp"] = pd.to_datetime(trff_sum_df['Timestamp'])
            agg_trff_df = trff_avg_df.resample('MS', on='Timestamp').mean().reset_index()
            agg_trff_sum_df = trff_sum_df.resample('MS', on='Timestamp').sum().reset_index()
            agg_trff_df_bh = trff_bh.resample('MS', on='Timestamp').mean().reset_index()

            fig_trff = graph_trff(agg_trff_df, agg_trff_sum_df, agg_trff_df_bh)
            print("Trafico mes agregado")

            # User experience
            agg_uexp_df = user_exp_df.resample('MS', on='Timestamp').mean().reset_index()

            fig_uexp = go.Figure(data=go.Scatter(x=agg_uexp_df["Timestamp"], y=agg_uexp_df["User_Exp"], mode="lines", name="U_exp"))
            print("User experience mes agregado")

        else:
            # Gráficas por día
            fig_bh = graph_BH(bh_df, bh_df_max) # Graficar BH
            fig_prb = graph_prb(prb_df) # Graficar PRBs
            fig_trff = graph_trff(trff_avg_df, trff_sum_df, trff_bh) # Graficar tráfico
            fig_uexp = go.Figure(data=go.Scatter(x=user_exp_df["Timestamp"], y=user_exp_df["User_Exp"], mode="lines", name="U_exp")) # Graficar user experience

    # Arreglar estilos de gráficos
    fig_trff.update_layout( {
        "margin": {"l": 0, "r": 10, "b": 0, "t": 40}, 
        # "autosize": True
         })
    fig_uexp.update_layout( {
        "margin": {"l": 0, "r": 10, "b": 0, "t": 40}, 
        # "autosize": True
         })
    fig_bh.update_layout( {
        "margin": {"l": 0, "r": 10, "b": 0, "t": 40}, 
        # "autosize": True
         })
    fig_prb.update_layout( {
        "margin": {"l": 0, "r": 10, "b": 0, "t": 40}, 
        # "autosize": True
         })

    # Actualiza el diseño de la figura para mover la leyenda
    fig_bh.update_layout(title="Max and avg users in BH", xaxis_title="Date", yaxis_title="Users",
                  legend=dict(orientation="h")
                  )
    # Actualiza la información que se muestra al pasar el mouse
    fig_bh.update_traces(hovertemplate="<b>Date</b>: %{x}<br><b>Users</b>: %{y:.2f} users")

    # Actualiza la información que se muestra al pasar el mouse
    fig_trff.update_traces(hovertemplate="<b>Date</b>: %{x}<br><b>Traffic</b>: %{y:.2f} GB")
    # Actualiza el diseño de la figura para mover la leyenda
    fig_trff.update_layout(title="DL Data Traffic", xaxis_title="Date",
                           yaxis=dict(title="Average and BH Traffic (GB)",
                                      titlefont=dict(color=MORADO_WOM), # Color de la descripción del eje
                                      tickfont=dict(color=MORADO_WOM), # Color de los valores del eje
                                      overlaying="y2" # Atributo para mostrar el eje y1 sobre el y2
                                      ),
                           yaxis2=dict(
                               title='Total Traffic (GB)',
                               side='right',
                               ),
                            legend=dict(x=-0.1,
                                        orientation='h' # Leyendas se ubican horizontalmente
                                        ),
                               )
    
    # Actualiza la información que se muestra al pasar el mouse
    fig_uexp.update_traces(hovertemplate="<b>Date</b>: %{x}<br><b>Value</b>: %{y:.2f} Mbps")
    # Actualiza el diseño de la figura para mover la leyenda
    fig_uexp.update_layout(title="User Experience in BH", xaxis_title="Date", yaxis_title="User experience (Mbps)")

    # Actualiza la información que se muestra al pasar el mouse
    fig_prb.update_traces(hovertemplate="<b>Date</b>: %{x}<br><b>Usage</b>: %{y:.2f} %")
    # Actualiza el diseño de la figura para mover la leyenda
    fig_prb.update_layout(title="PRB occupation in BH", xaxis_title="Date", yaxis_title="Usage in %",
                  legend=dict(orientation="h"
                            #   , yanchor="bottom", y=1.02, xanchor="right", x=1
                              ))
    
    fig_fullscreen = go.Figure(data=[go.Scatter(x=[], y=[])]) # Figura fullscreen queda vacia cada vez que se haga una selección

    return container, gauge_value, fig_trff, fig_uexp, fig_bh, fig_prb, fig_fullscreen








#--------------------------------------------------------- FUNCIONES CALLBACK QUE MUESTRA KPIs EN MAPA ------------------------------------------------------#
def map_query(start_date, end_date, kpi, geo_agg, name_column):
    table_mapping = { # Opciones para completar la consulta segun agregación seleccionada
        'celda': 'ran_1h_cell',
        'sector': 'ran_1h_sector',
        'EB': 'ran_1h_node',
        'cluster': 'ran_1h_cluster',
        'localidad': 'ran_1h_localidad',
        'municipio': 'ran_1h_municipio',
        'AM': 'ran_1h_am',
        'departamento': 'ran_1h_departamento',
        'regional': 'ran_1h_regional',
        'total': 'ran_1h_total'
    }

    table_name = table_mapping[geo_agg] # Elección para completar la consulta según agregación

    try:
        # Conectarse a la base de datos
        conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)

        # Crear cursor
        cur = conn.cursor()
        
        if kpi == "BH":
            if geo_agg == 'total':
                query = sql.SQL("""SELECT "Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.Traffic.ActiveUser.DL.Max"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(table_name))
                columns = ["Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.Traffic.ActiveUser.DL.Max"]
            else:
                query = sql.SQL("""SELECT "Timestamp", {}, "L.Traffic.ActiveUser.DL.Avg", "L.Traffic.ActiveUser.DL.Max"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(name_column), sql.Identifier(table_name))
                columns = ["Timestamp", name_column, "L.Traffic.ActiveUser.DL.Avg", "L.Traffic.ActiveUser.DL.Max"]
        
        elif kpi == "PRB":
            if geo_agg == 'total':
                query = sql.SQL("""SELECT "Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(table_name))
                columns = ["Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"]
            else:
                query = sql.SQL("""SELECT "Timestamp", {}, "L.Traffic.ActiveUser.DL.Avg", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(name_column), sql.Identifier(table_name))
                columns = ["Timestamp", name_column, "L.Traffic.ActiveUser.DL.Avg", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"]
        
        elif kpi == "Traffic":
            if geo_agg == 'total':
                query = sql.SQL("""SELECT "Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.UL(bit)"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(table_name))
                columns = ["Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.UL(bit)"]
            else:
                query = sql.SQL("""SELECT "Timestamp", {}, "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.UL(bit)"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(name_column), sql.Identifier(table_name))
                columns = ["Timestamp", name_column, "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.UL(bit)"]

        elif kpi == "u_exp":
            if geo_agg == 'total':
                query = sql.SQL("""SELECT "Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(table_name))
                columns = ["Timestamp", "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]
            else:
                query = sql.SQL("""SELECT "Timestamp", {}, "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"
                                FROM {}
                                WHERE DATE("Timestamp") BETWEEN %s AND %s""").format(sql.Identifier(name_column), sql.Identifier(table_name))
                columns = ["Timestamp", name_column, "L.Traffic.ActiveUser.DL.Avg", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]

        # print("Consulta :", query.as_string(cur))
        
        cur.execute(query, (start_date, end_date))
        print("Consulta para mostrar KPI en el mapa exitosa")
        rows = cur.fetchall()
        
        df = pd.DataFrame(rows, columns=columns)
        df = df.sort_values(by="Timestamp")
        cur.close() # Se cierra el cursor
        print("Se ha creado el dataframe de la consulta para mostrar el KPI")

        return df

    except Exception as e:
        print("Error al conectar con la base de datos: ", e)
        return pd.DataFrame()
    
    finally:
        conn.close()

# Callback para visualizar KPIs sobre el mapa
@callback(
        Output(component_id='map', component_property='figure',), #  allow_duplicate=True
        Output(component_id='map_text', component_property='children'),

        Input(component_id='update_kpi', component_property='n_clicks'),
        Input(component_id="aggregation", component_property='value'),

        State(component_id='select_graph', component_property='value'),
        State(component_id="time", component_property='start_date'),
        State(component_id="time", component_property='end_date'),
        # prevent_initial_call=True # Para que no me genere la alerta de salida duplicada
)
def map_kpi(boton, agg, kpi, start_date, end_date):

    # Definir las configuraciones de zoom y centro del mapa una vez
    map_layout = dict(zoom=5, center={"lat": 4.6837, "lon": -74.0566})
    
    if agg == "total": # Acción por si se elige agregación total
        data = {}
        fig = px.scatter_mapbox(data, 
                                zoom=map_layout["zoom"], center=map_layout["center"]
                                )
        fig.update_layout(mapbox_style="carto-positron",
                    margin={"r":0,"t":0,"l":0,"b":0},
                    )
        
        warning = f"Los datos para la totalidad de la red se muestran en las gráficas de la derecha"
        return fig, warning
    
    start_date = datetime.strptime(start_date, "%Y-%m-%d") # Paso a formato datetime
    end_date = datetime.strptime(end_date, "%Y-%m-%d") # Paso a formato datetime
    date_diff = (end_date - start_date).days
    print("Diferencia de fechas: ", date_diff)
    
    # Condicional para acotar el rango de la consulta por agregación geográfica
    if agg in ["celda","sector"]:
        if date_diff > 3: # Si el rango seleccionado es mayor a 3 días
            start_date = end_date - timedelta(days=2) # Resto dos días para así tomar el dia seleccionado y los dos anteriores
    
    elif agg == "EB":
        if date_diff > 7: # Si el rango seleccionado es mayor a 7 días
            start_date = end_date - timedelta(days=6) # Resto dos días para así tomar el dia seleccionado y los dos anteriores

    elif agg in ["cluster","localidad","municipio"]:
        if date_diff > 21: # Si el rango seleccionado es mayor a 21 días
            start_date = end_date - timedelta(days=20) # Resto dos días para así tomar el dia seleccionado y los dos anteriores
    
    elif agg in ["AM","deparamento","regional"]: # Para AM, Departamento y regional
        if date_diff > 120: # Si el rango seleccionado es mayor a 120 días
            start_date = end_date - timedelta(days=119) # Resto dos días para así tomar el dia seleccionado y los dos anteriores

    # Paso de datetime a string
    end_date = end_date.strftime("%Y-%m-%d")
    start_date = start_date.strftime("%Y-%m-%d")
    

    text = f"Los datos para la visualización del KPI van desde {start_date} hasta {end_date}"

    name_column_mapping = { # Opciones para completar la consulta y agregar datos según agregación
        'celda': 'Cell_name',
        'sector': 'sector_name',
        'EB': 'node_name',
        'cluster': 'cluster_name',
        'localidad': 'localidad_dane_code',
        'municipio': 'municipio_dane_code',
        'AM': 'am_name',
        'departamento': 'dpto_dane_code',
        'regional': 'regional_name',
        'total': None  # No hay columna específica para 'total'
    }
    data_column = name_column_mapping[agg]

    df = map_query(start_date, end_date, kpi, agg, data_column) # Llamado a la función que hace la consulta

    if df.empty: # Acción por si el dataframe está vacio
        warning = f"No hay datos para la fecha {date}"
        return no_update, warning

    # Encontrar hora pico (BH)
    df['date'] = df['Timestamp'].dt.date # Crear columna con días
    bh_day = df.groupby([data_column, 'date'])["L.Traffic.ActiveUser.DL.Avg"].idxmax() # Agrupo los datos por nombre de celda y luego encuentro las horas donde los usuarios son máximos
    bh_df = df.loc[bh_day] # Creo un nuevo df con las horas pico por día
    
    if kpi == "BH":
        graph_column = "L.Traffic.ActiveUser.DL.Avg" # Guardo la columna de filtrado porque con ella es que se grafican colores en el mapa

        bh_df = bh_df[[data_column,graph_column]].copy() # Copia del df solo con estas columnas
        bh_df = bh_df.groupby(data_column)[graph_column].mean().reset_index() # Se promedia por nombre de marcador o polígono

        color_scale = [MORADO_CLARO,MAGENTA_OPACO,MAGENTA,MORADO_WOM,MORADO_OSCURO]
        kpi_range = None

    elif kpi == "PRB":
        bh_df = bh_df.copy()
        bh_df["DL_PRB_usage"] = (bh_df["L.ChMeas.PRB.DL.Used.Avg"] / bh_df["L.ChMeas.PRB.DL.Avail"]) * 100 # Cálculo de % ocupación en downlink y guardado en nueva columna
        bh_df["UL_PRB_usage"] = (bh_df["L.ChMeas.PRB.UL.Used.Avg"] / bh_df["L.ChMeas.PRB.UL.Avail"]) * 100 # Cálculo de % ocupación en uplink y guardado en nueva columna
        graph_column = "DL_PRB_usage"
        bh_df = bh_df.groupby(data_column)[graph_column].mean().reset_index()
        # Definir la escala de color personalizada
        color_scale = [
            (0, 'green'),    # Verde para valor inferior
            (0.5, 'yellow'),   # Amarillo para valores medios
            (1, 'red')       # Rojo para valores superior
        ]
        kpi_range = [0,100] # Rango a mostrar en la barra de escala

    elif kpi == "Traffic":
        bh_df = bh_df.copy()
        # print("df pre traffic\n", bh_df)
        bh_df["L.Thrp.bits.DL(GB)"] = bh_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversión de bit a GB
        graph_column = "L.Thrp.bits.DL(GB)"
        bh_df = bh_df.groupby(data_column)[graph_column].mean().reset_index()
        # print("total avg trff:\n", bh_df)

        color_scale = [MORADO_CLARO,MAGENTA_OPACO,MAGENTA,MORADO_WOM,MORADO_OSCURO]
        kpi_range = None

    elif kpi == "u_exp":
        bh_df = bh_df.copy()
        bh_df["User_Exp"] = ((bh_df["L.Thrp.bits.DL(bit)"]-bh_df["L.Thrp.bits.DL.LastTTI(bit)"]) / (bh_df["L.Thrp.Time.DL.RmvLastTTI(ms)"])) / 1024 # Calculo user experience
        graph_column = "User_Exp"
        bh_df = bh_df.groupby(data_column)[graph_column].mean().reset_index()

        # Definir la escala de color discreta según la definición de la empresa
        color_scale = [
            (0, "darkred"), (1, "darkred"),
            (1, "red"), (2.5, "red"),
            (2.5, "orange"), (3.3, "orange"),
            (3.3, "yellow"), (5, "yellow"),
            (5, "green"), (9.99999, "green"),
            (9.99999, "lime"), (12, "lime")
        ]
        # Convertir la escala a un formato adecuado para plotly
        color_scale = [(i / 12, col) for i, col in color_scale]
        kpi_range = [0,12]
    
    else:
        print("Hubo un error en la selección del KPI")
        raise PreventUpdate
    
    # Condicional para gráficar la agregación geográfica seleccionada
    if agg == "celda":
        cells = df_geo.drop_duplicates(subset=["dwh_cell_name_wom"]).copy() # Df con nombres únicos de celda
        bh_df[data_column] = bh_df[data_column].str.upper() # Valores a mayusculas
        df_merged = cells.merge(bh_df, how="left", left_on="dwh_cell_name_wom", right_on="Cell_name") # Merge según nombre de celdas

        fig = px.scatter_mapbox(df_merged, lat="dwh_latitud", lon="dwh_longitud",
                                color=graph_column,
                                color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                # size=graph_column, # No se utiliza size porque no toma valores nulos, tendría que borrar muchas celdas
                                zoom=map_layout["zoom"], center=map_layout["center"],
                                hover_name="dwh_cell_name_wom",
                                custom_data="dwh_cell_name_wom"
                                )
    
    elif agg == "sector":
        sectores = df_geo.drop_duplicates(subset=["sector_name"]).copy() # Df con nombres únicos de sector
        df_merged = sectores.merge(bh_df, how="left", left_on="sector_name", right_on="sector_name")

        fig = px.scatter_mapbox(df_merged, lat="dwh_latitud", lon="dwh_longitud",
                                color=graph_column,
                                color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                # size=graph_column,
                                zoom=map_layout["zoom"], center=map_layout["center"],
                                hover_name="sector_name",
                                custom_data="sector_name"
                                )

    elif agg == "EB":
        nodos = df_geo.drop_duplicates(subset=["node_name"]).copy() # Df con nombres únicos de nodo
        df_merged = nodos.merge(bh_df, how="left", left_on="node_name", right_on="node_name")

        fig = px.scatter_mapbox(df_merged, lat="dwh_latitud", lon="dwh_longitud",
                                color=graph_column,
                                color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                zoom=map_layout["zoom"], center=map_layout["center"],
                                hover_name="node_name",
                                custom_data="node_name"
                                )

    elif agg == "cluster":
        df_merged = clusters.merge(bh_df, how="left", left_on="key", right_on="cluster_name")
        fig = px.choropleth_mapbox(df_merged, geojson=df_merged.geometry, locations=df_merged.index,
                                   color=graph_column,
                                   color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                   zoom=map_layout["zoom"], center=map_layout["center"],
                                   opacity=0.5,
                                   hover_name="key",
                                   custom_data="key"
                                   )
    
    elif agg == "localidad":
        localidades["Localidad"] = localidades["Localidad"].astype(int) # Todas las columnas del df quedaron como string cuando se leyó el GeoJSON, por lo que toca hacer casting para el merge
        df_merged = localidades.merge(bh_df, how="left", left_on="Localidad", right_on="localidad_dane_code")
        fig = px.choropleth_mapbox(df_merged, geojson=df_merged.geometry, locations=df_merged.index,
                                   color=graph_column,
                                   color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                   zoom=map_layout["zoom"], center=map_layout["center"],
                                   opacity=0.5,
                                   hover_name="Nombre_localidad",
                                   hover_data="Localidad",
                                   custom_data="Localidad"
                                   )
    
    elif agg == "municipio":
        municipios["MPIO_CCNCT"] = municipios["MPIO_CCNCT"].astype(int) # Todas las columnas del df quedaron como string cuando se leyó el GeoJSON, por lo que toca hacer casting para el merge
        df_merged = municipios.merge(bh_df, how="left", left_on="MPIO_CCNCT", right_on="municipio_dane_code")
        fig = px.choropleth_mapbox(df_merged, geojson=df_merged.geometry, locations=df_merged.index,
                                   color=graph_column,
                                   color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                   zoom=map_layout["zoom"], center=map_layout["center"],
                                   opacity=0.5,
                                   hover_name="MPIO_CNMBR",
                                   hover_data="MPIO_CCNCT",
                                   custom_data="MPIO_CCNCT"
                                   )
    
    elif agg == "AM":
        df_merged = areas_metro.merge(bh_df, how="left", left_on="AM", right_on="am_name")
        fig = px.choropleth_mapbox(df_merged, geojson=df_merged.geometry, locations=df_merged.index,
                                   color=graph_column,
                                   color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                   zoom=map_layout["zoom"], center=map_layout["center"],
                                   opacity=0.5,
                                   hover_name="AM",
                                   custom_data="AM"
                                   )
        
    elif agg == "departamento":
        departamentos["DPTO_CCDGO"] = departamentos["DPTO_CCDGO"].astype(int) # Todas las columnas del df quedaron como string cuando se leyó el GeoJSON, por lo que toca hacer casting para el merge
        df_merged = departamentos.merge(bh_df, how="left", left_on="DPTO_CCDGO", right_on="dpto_dane_code")
        fig = px.choropleth_mapbox(df_merged, geojson=df_merged.geometry, locations=df_merged.index,
                                   color=graph_column,
                                   color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                   zoom=map_layout["zoom"], center=map_layout["center"],
                                   opacity=0.5,
                                   hover_name="DPTO_CNMBR",
                                   hover_data="DPTO_CCDGO",
                                   custom_data="DPTO_CCDGO"
                                   )
    
    elif agg == "regional":
        df_merged = regionales.merge(bh_df, how="left", left_on="DPTO_REGIONAL", right_on="regional_name")
        fig = px.choropleth_mapbox(df_merged, geojson=df_merged.geometry, locations=df_merged.index,
                                   color=graph_column,
                                   color_continuous_scale=color_scale, range_color=kpi_range, # Definir rango escala de color
                                   zoom=map_layout["zoom"], center=map_layout["center"],
                                   opacity=0.5,
                                   hover_name="DPTO_REGIONAL",
                                   custom_data="DPTO_REGIONAL"
                                   )

    # Condicional para definir estilo del gráfico según KPI a mostrar
    if kpi == "BH":
        # Personalizar la barra de colores
        fig.update_layout(
            coloraxis_colorbar=dict(
                title="Users BH",
            ),
            mapbox_style='carto-positron',
            margin={"r":0,"t":0,"l":0,"b":0}
        )

    elif kpi == "PRB":
        # Personalizar la barra de colores
        fig.update_layout(
            coloraxis_colorbar=dict(
                title="PRB usage BH",
                ticksuffix=" %"
            ),
            mapbox_style='carto-positron',
            margin={"r":0,"t":0,"l":0,"b":0}
        )

    elif kpi == "Traffic":
        # Personalizar la barra de colores
        fig.update_layout(
            coloraxis_colorbar=dict(
                title="Traffic BH",
                ticksuffix=" GB"
            ),
            mapbox_style='carto-positron',
            margin={"r":0,"t":0,"l":0,"b":0}
        )

    elif kpi == "u_exp":
        # Personalizar la barra de colores
        fig.update_layout(
            coloraxis_colorbar=dict(
                title="User Experience BH",
                tickvals=[0, 1, 2.5, 3.3, 5, 10, 12],
                ticktext=['0 Mbps', '1 Mbps', '2.5 Mbps', '3.3 Mbps', '5 Mbps', '10 Mbps', '>10 Mbps'],
                ticks="outside"
            ),
            mapbox_style='carto-positron',
            margin={"r":0,"t":0,"l":0,"b":0}
        )

    return fig, text


# Callback para mostrar la gráfica en pantalla grande
@callback(
        Output(component_id='graph_test', component_property='figure', allow_duplicate=True),

        Input(component_id='fullscreen', component_property='n_clicks'),

        State(component_id='select_graph', component_property='value'),
        State(component_id='traffic', component_property='figure'),
        State(component_id='user_exp', component_property='figure'),
        State(component_id='bh', component_property='figure'),
        State(component_id='PRB', component_property='figure'),
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
        )
def full_screen(boton, seleccion, traff, uexp, bh, prb):
    # print(f"La seleccion exacta es: ->{seleccion}<-")
    if seleccion == "BH":
        fig = bh
    elif seleccion == "PRB":
        fig = prb
    elif seleccion == "Traffic":
        fig = traff
    elif seleccion == "u_exp":
        fig = uexp
    else:
        raise PreventUpdate

    return fig


def extraer_claves(keys_needed, data_dict):# Función para extraer las llaves necesarias
    return {key: data_dict[key] for key in keys_needed if key in data_dict}

# Callback para descargar datos de cada gráfico
@callback(
        Output(component_id='download_file', component_property='data'),

        Input(component_id='download', component_property='n_clicks'),

        State(component_id='select_graph', component_property='value'),
        State(component_id='traffic', component_property='figure'),
        State(component_id='user_exp', component_property='figure'),
        State(component_id='bh', component_property='figure'),
        State(component_id='PRB', component_property='figure'),
        prevent_initial_call=True # Evitar el primer llamado automatico que hace dash
        )
def download_graph_data(boton, seleccion, traff, uexp, bh, prb):
    if seleccion == "BH":
        fig = bh # Información de la figura seleccionada
        keys = ["name","x","y","text","type"] # Información de la gráfica que se quiere descargar
        # Diccionario de cada trazo en la gráfica
        dict1 = extraer_claves(keys, fig["data"][0])
        dict2 = extraer_claves(keys, fig["data"][1])
        # Creamos un DataFrame a partir de los datos
        trazo1 = pd.DataFrame(dict1)
        trazo2 = pd.DataFrame(dict2)
        df = pd.concat([trazo1, trazo2]).reset_index(drop=True)
        file_name = f"Graph_ActiveUsers_data.csv"

    elif seleccion == "PRB":
        fig = prb
        keys = ["name","x","y","type"] # Información de la gráfica que se quiere descargar
        # Diccionario de cada trazo en la gráfica
        dict1 = extraer_claves(keys, fig["data"][0])
        dict2 = extraer_claves(keys, fig["data"][1])
        # Creamos un DataFrame a partir de los datos
        trazo1 = pd.DataFrame(dict1)
        trazo2 = pd.DataFrame(dict2)
        df = pd.concat([trazo1, trazo2]).reset_index(drop=True)
        file_name = f"Graph_PRBusage_data.csv"

    elif seleccion == "Traffic":
        fig = traff
        keys = ["name","x","y","type"] # Información de la gráfica que se quiere descargar
        # Diccionario de cada trazo en la gráfica
        dict1 = extraer_claves(keys, fig["data"][0])
        dict2 = extraer_claves(keys, fig["data"][1])
        dict3 = extraer_claves(keys, fig["data"][2])
        # Creamos un DataFrame a partir de los datos
        trazo1 = pd.DataFrame(dict1)
        trazo2 = pd.DataFrame(dict2)
        trazo3 = pd.DataFrame(dict3)
        df = pd.concat([trazo1, trazo2, trazo3]).reset_index(drop=True)
        file_name = f"Graph_Traffic_data.csv"

    elif seleccion == "u_exp":
        fig = uexp
        keys = ["name","x","y","type"] # Información de la gráfica que se quiere descargar
        # Diccionario de cada trazo en la gráfica
        dict1 = extraer_claves(keys, fig["data"][0])
        df = pd.DataFrame(dict1)
        file_name = f"Graph_UserExperience_data.csv"

    else:
        raise PreventUpdate
    print("fig in callback:\n", fig["data"])

    return dcc.send_data_frame(df.to_csv, file_name, index=False)

if __name__ == '__main__':
    app.run(debug=True)