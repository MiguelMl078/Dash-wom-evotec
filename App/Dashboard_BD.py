import dash
from dash import Dash, dcc, html, Input, Output, State, callback, Patch, no_update
from dash.exceptions import PreventUpdate

# Librerías adicionales de Dash
import dash_leaflet as dl # Para hacer mapas con Leaflet
import dash_bootstrap_components as dbc # Para el Layout
from dash_bootstrap_templates import load_figure_template # Parte del layout, configura plantillas para todas las figuras
import dash_daq as daq # Para otros componentes, en este caso para un medido de nivel con aguja

import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2 # Para consulta a base de datos PostgreSQL
from unidecode import unidecode # Libreria para eliminar acentos y poder hace condicionales tranquilo
from datetime import datetime
import datetime as dt
import numpy as np
import json

#---------- Funciones ----------#
def to_float(n): # Función para convertir a float y convierte los NaN a 0
    try:
        return float(n)
    except ValueError:
        print (ValueError)
        return 0

def to_int(n):
    try:
        return int(n)
    except ValueError:
        print (ValueError)
        return 0
    
def query(): # Función para hacer query desde la base de datos de Sergio
    # Conectar a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host="10.40.111.106",
        database="analytics_prod",
        user="evotec",
        password="3v0t3c",
        port="5432"
    )

    # Crear un cursor
    cur = conn.cursor()

    query = """SELECT dwh_cell_name_wom, dwh_banda, dwh_sector, dwh_latitud, dwh_longitud, cluster_key, cluster_nombre, dwh_localidad, dwh_dane_cod_localidad, dane_nombre_mpio, dane_code, dane_code_dpto, dane_nombre_dpt, wom_regional 
            FROM bodega_analitica.roaming_cell_dim 
            WHERE dwh_operador_rat = 'WOM 4G' LIMIT 100000"""
    # query = """SELECT cell_name, site_name, sector_id, band, vendor, latitude, longitude, region, department, city, locality, dane_code, address, cluster
    #     FROM bodega_analitica.cell_dim LIMIT 100000"""

    # # Leer datos de la base de datos PostgreSQL
    # df_geo = pd.read_sql(query, conn)

    cur.execute(query) # Ejecutar la consulta
    datos = cur.fetchall() # Almacenar todas las filas de la consulta en esta variable
    columnas = [desc[0] for desc in cur.description]  # Obtener los nombres de las columnas
    cur.close()
    conn.close() # Cerrar conexión

    df_geo = pd.DataFrame(datos, columns=columnas) # Creo dataframe 

    return df_geo

def filtrado(cells, df):
    cells = cells["dwh_cell_name_wom"] # Extraigo solo los nombre porque es lo único que necesito
    data = df[df["Cell_name"].str.upper().isin(cells)].copy() # Genero copia del df de datos unicamente de las celdas cuyo nombre está en el df extraido anteriormente
    return data

def bh(data, column):
     # Calculo BH(hora pico) por día
    datos_avg = data.groupby("Timestamp")[column].sum().reset_index() # df con average data sumada por hora
    # print("datos_avg:\n", datos_avg)
    bh_day = datos_avg.groupby(datos_avg['Timestamp'].dt.date)[column].idxmax() # Agrupo los datos por fecha y luego encuentro los indices que contienen los valores de tráfico maximos (BH)
    bh_df = datos_avg.loc[bh_day, ['Timestamp', column]] # Creo un nuevo df con las horas pico por día y unicamente con las columnas de tiempo y tráfico
    # print("bh_df:\n", bh_df)
    bh_df['Date'] = bh_df['Timestamp'].dt.date # Vuelvo a crear columna Date y Time que se habían perdido con el fin de gráficar respecto a la fecha y mostrar la hora
    bh_df['Time'] = bh_df['Timestamp'].dt.strftime('%H:%M') # Formato para que solo sea Hora y Minuto

    return bh_df

def graph_BH(bh_df_avg, bh_df_max):
    fig = go.Figure() # Crea una figura vacía
    fig.add_trace(go.Bar(x=bh_df_avg["Date"], y=bh_df_avg["L.Traffic.ActiveUser.DL.Avg"], name="Avg", text=bh_df_avg["Time"]))
    fig.add_trace(go.Bar(x=bh_df_max["Date"], y=bh_df_max["L.Traffic.ActiveUser.DL.Max"], name="Max", text=bh_df_max["Time"]))

    return fig

def PRB_usg(data, bh_df):
    data = data[data["Timestamp"].isin(bh_df["Timestamp"])].copy() # Genero copia del df de datos unicamente de las casillas dentro del BH
    data = data[["Timestamp", "Cell_name", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"]] # Solo columnas necesarias
    # print("PRB OCCUP: ")
    # print(data)
    # print("Las sumatorias")
    # print(data.groupby("Timestamp")["L.ChMeas.PRB.DL.Used.Avg"].sum().reset_index())
    # print(data.groupby("Timestamp")["L.ChMeas.PRB.DL.Avail"].sum().reset_index())

    # Agrupa los datos por Timestamp y suma los valores de PRBs utilizados y disponibles en Downlink y Uplink
    prb_df = data.groupby("Timestamp").agg({
        "L.ChMeas.PRB.DL.Used.Avg": "sum",  # Suma de PRBs utilizados en Downlink
        "L.ChMeas.PRB.DL.Avail": "sum",     # Suma de PRBs disponibles en Downlink
        "L.ChMeas.PRB.UL.Used.Avg": "sum",  # Suma de PRBs utilizados en Uplink
        "L.ChMeas.PRB.UL.Avail": "sum"      # Suma de PRBs disponibles en Uplink
    }).reset_index()
    # print(prb_df)
    prb_df["DL_PRB_usage"] = (prb_df["L.ChMeas.PRB.DL.Used.Avg"] / prb_df["L.ChMeas.PRB.DL.Avail"]) * 100 # Cálculo de % ocupación en downlink y guardado en nueva columna
    prb_df["UL_PRB_usage"] = (prb_df["L.ChMeas.PRB.UL.Used.Avg"] / prb_df["L.ChMeas.PRB.UL.Avail"]) * 100 # # Cálculo de % ocupación en uplink y guardado en nueva columna
    # print(prb_df)

    return prb_df

def bit_to_GB(bit):
    gbyte = bit / (8*10**9)
    return gbyte

def traffic(data, bh_df):
    fig_trff = go.Figure() # Crea una figura vacía
    trff_df = data.groupby("Timestamp")["L.Thrp.bits.DL(bit)"].sum().reset_index() # Suma del tráfico de todas las celdas por hora
    # print("SUM:")
    # print(trff_df)
    trff_df = trff_df.groupby(trff_df["Timestamp"].dt.date)["L.Thrp.bits.DL(bit)"].mean().reset_index() # Promedio del tráfico de cada hora del día
    trff_df["L.Thrp.bits.DL(bit)"] = trff_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversion de bit a GB
    # print("AVG:")
    # print(trff_df)
    fig_trff.add_trace(go.Scatter(x=trff_df["Timestamp"], y=trff_df["L.Thrp.bits.DL(bit)"], mode='lines', name='Traffic')) # Añado linea de tráfico al día
    # Calculo de tráfico en BH
    trff_bh = data[data["Timestamp"].isin(bh_df["Timestamp"])].copy() # Genero copia del df de datos unicamente de las casillas dentro del BH
    trff_bh = trff_bh[["Timestamp", "Cell_name", "L.Thrp.bits.DL(bit)"]] # Solo columnas necesarias
    trff_bh["L.Thrp.bits.DL(bit)_BH"] = trff_bh["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversión de bit a GB
    # print("BH:")
    # print(trff_bh)
    trff_bh = trff_bh.groupby("Timestamp")["L.Thrp.bits.DL(bit)_BH"].sum().reset_index() # df con max data sumada
    # print("POST:")
    # print(trff_bh)
    fig_trff.add_trace(go.Scatter(x=trff_bh["Timestamp"], y=trff_bh["L.Thrp.bits.DL(bit)_BH"], mode='lines', name='Traffic_BH')) # Agrega la segunda línea a la misma figura
    return fig_trff

def user_exp(data, bh_df):
    data = data[data["Timestamp"].isin(bh_df["Timestamp"])].copy()
    data = data[["Timestamp", "Cell_name", "L.Thrp.bits.DL(bit)", "L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]] # Solo columnas necesarias

    # Agrupa los datos por Timestamp y suma los valores de columnas que hacen parte de la ecuación
    user_exp_df = data.groupby("Timestamp").agg({
        "L.Thrp.bits.DL(bit)": "sum",  # Suma throughput en DL
        "L.Thrp.bits.DL.LastTTI(bit)": "sum",     # Suma de variable para el cálculo
        "L.Thrp.Time.DL.RmvLastTTI(ms)": "sum",  # Suma de variable para el cálculo
    }).reset_index()
    # print(prb_df)
    user_exp_df["User_Exp"] = ((user_exp_df["L.Thrp.bits.DL(bit)"]-user_exp_df["L.Thrp.bits.DL.LastTTI(bit)"]) / (user_exp_df["L.Thrp.Time.DL.RmvLastTTI(ms)"])) / 1024 # Calculo user experience
    fig_uexp = px.line(user_exp_df, x="Timestamp", y="User_Exp", title="User Experience in BH")
    return fig_uexp

def convert_timestamp(timestamp_str):
    try:
        # Intentar convertir el Timestamp a datetime directamente
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Si falla, asumir que la hora es 00:00:00 y agregar ese componente
        return datetime.strptime(timestamp_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")


#---------- Iniciar App ----------#
# app = dash.Dash(__name__)
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css" # Hoja de estilo para los Dash Core Components
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.PULSE, dbc_css]) # Importo tema desde bootstrap
load_figure_template("pulse") # Función para que todos los gráficos tengan esta plantilla

#---------- Importar Datos ----------#
# Ruta de la carpeta donde estan alojados los archivos de datos que serán reemplazados por las BD
BD = "C:/Users/roberto.cuervo.WOMCOL/OneDrive - WOM Colombia/Documentos/Progra_Tests/Python/BD/"

#### Lectura de datos de tráfico
# df = pd.read_csv(BD+"KPIs BH(KPI Analysis Result).csv"
#                  , usecols=["Time", "eNodeB Name", "Cell Name", "L.Traffic.ActiveUser.Dl.Avg", "L.Traffic.ActiveUser.DL.Max"] # Filtrado de columnas estrictamente necesarias
#                  , dtype={"Time":str, "eNodeB Name":str, "Cell Name":str, "L.Traffic.ActiveUser.Dl.Avg":str, "L.Traffic.ActiveUser.DL.Max":int} # Hacer explicito tipo de dato con el fin de optimizar
#                  )
# df = pd.read_csv(BD+"Raw_Data.csv")
df = pd.read_csv("C:/Users/roberto.cuervo.WOMCOL/OneDrive - WOM Colombia/Documentos/Progra_Tests/Python/15-28_abril.csv") # Prueba ultima semana
# df["Timestamp"] = df["Date"] + ' ' + df["Time"] # Creo nueva columna uniendo fecha y hora
# df["Timestamp"] = pd.to_datetime(df["Timestamp"]) # Convierto dicha columna a formato datetime para hacer filtrado temporal
df['Timestamp'] = df['Timestamp'].apply(convert_timestamp)

#### Leer datos geográficos
# df_geo = pd.read_csv(BD+"Baseline_BD.csv")
df_geo = query()
# Corrijo la columna que contiene el nombre de las celdas para que cuadre con los nombres de los informes
df_geo["dwh_cell_name_wom"] = df_geo["dwh_cell_name_wom"].str.upper() # Todo a mayusculas
df_geo["node_name"] = df_geo["dwh_cell_name_wom"]
# df_geo["dwh_cell_name_wom"] = df_geo["dwh_cell_name_wom"] + "_" + df_geo["dwh_banda"].apply(str) + "_" + df_geo["dwh_sector"].apply(str)
# Concatenar las columnas, reemplazando "B4" con "AWS" cuando sea necesario
df_geo["dwh_cell_name_wom"] = np.where(df_geo["dwh_banda"] == "B4", # Cuando se cumpla esta condición
                                        df_geo["dwh_cell_name_wom"] + "_AWS_" + df_geo["dwh_sector"].astype(str), # Se aplica este fragmento
                                        df_geo["dwh_cell_name_wom"] + "_" + df_geo["dwh_banda"].astype(str) + "_" + df_geo["dwh_sector"].astype(str)) # Else
df_geo = df_geo.drop_duplicates(subset=["dwh_cell_name_wom"]) # Elimino los nombres exactamente iguales
df_geo["sector"] = df_geo["dwh_sector"].apply(lambda x: 1 if x in [1,4,7] else (2 if x in [2,5,8] else (3 if x in [3,6,9] else 4))) # Creación de columna "sector" para logica de agregación por sectores. Se agrupa según el id de sector
df_geo["sector_name"] = df_geo["node_name"] + ": " + df_geo["sector"].astype(str)

# df_geo["dane_code"] =  pd.to_numeric(df_geo["dane_code"], downcast="unsigned") # La columna se convierte a flotante
# df_geo["address"] =  pd.to_numeric(df_geo["address"], downcast="unsigned") # La columna se convierte a flotante
# df_geo["dane_code"] =  df_geo["dane_code"].astype('Int64') # La clase 'Int64' permite manejar valores nulos
# df_geo["address"] =  df_geo["address"].astype('Int64') # La clase 'Int64' permite manejar valores nulos

# df_geo = df_geo.dropna(subset="cell_name") # Elimino filas vacias
# df_geo = df_geo.drop_duplicates(subset=["cell_name"]) # Elimino los nombres exactamente iguales
# df_geo = df_geo.rename(columns={"site_name":"node_name"}) # Renombro columna
# df_geo["cell_name"] = df_geo["cell_name"].str.upper() # Todo a mayusculas
# df_geo["node_name"] = df_geo["node_name"].str.upper() # Todo a mayusculas
# df_geo["sector"] = df_geo["sector_id"].apply(lambda x: 1 if x in [1,4,7] else (2 if x in [2,5,8] else (3 if x in [3,6,9] else 4))) # Creación de columna "sector" para logica de agregación por sectores. Se agrupa según el id de sector
# df_geo["sector_name"] = df_geo["node_name"] + ": " + df_geo["sector"].apply(str) # Creo un key name para cada sector
# df_geo["dane_code_dpto"] = df_geo["dane_code"] / 1000 # Divido entre mil para mover el punto decimal 3 casillas a la izquierda
# df_geo["dane_code_dpto"] = df_geo["dane_code_dpto"].astype('Int64') # Convierto a entero para no tener en cuenta lo que está despues del punto decimal


# Lectura de archivo que contiene las localidades
localidades = gpd.read_file("Localidades Crowdsourcing 2023/Crowdwourcing 2023/Localidades Finales mayo 18 v2.TAB")

# Leer el archivo GeoJSON de clusters con Geopandas
clusters = gpd.read_file("Clusterizacion.geojson")

# Leer el archivo GeoJSON de municipios con Geopandas
municipios = gpd.read_file("co_2018_MGN_MPIO_POLITICO.geojson")

# Leer el archivo GeoJSON de departamentos con Geopandas
departamentos = gpd.read_file("co_2018_MGN_DPTO_POLITICO.geojson")

# Leer el archivo GeoJSON de regionales con Geopandas
regionales = gpd.read_file("Regional_test.geojson")


##################### PRUEBA DASH-LEAFLET ######################################
# # Convertir el DataFrame a un GeoDataFrame
# gdf = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(df_geo.Longitude, df_geo.Latitude))
# # Convertir el DataFrame de Geopandas a un GeoJSON
# geojson_data = dl.GeoJSON(data=gdf.__geo_interface__, cluster=True)

#---------- App layout ----------#
app.layout = dbc.Container([

    dbc.Row([
        dbc.Col([
            html.H1("Herramienta: DashWOM" ),
        ], width=12, className="bg-primary text-white p-2 mb-2 text-center")
    ]
    ),

    dbc.Row([
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
                                        {"label": "Departamento", "value": "departamento"},
                                        {"label": "Regional", "value": "regional"},
                                        {"label": "Total", "value": "total"}],
                                    value="celda",
                                    clearable=False,
                                    )
                    ], width=6),
                    dbc.Col([
                        dbc.Label("Selección"),
                        dcc.Dropdown(
                            id="select",
                            placeholder="Selecciona un punto o polígono"
                                    ),
                    ], width=6)
                ], style={"height": "50%"}),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Tiempo"),
                        dcc.DatePickerRange(id="time",
                                            display_format='YYYY-M-D',
                                            start_date_placeholder_text="Seleccione",
                                            end_date=datetime.now().date(),
                                            # min_date_allowed=date(2021, 1, 1),
                                )
                    ], width=6),
                    dbc.Col([
                        dbc.Label("Granularidad"),
                        dcc.Dropdown(id="time_agg",
                                    options=[
                                        {"label": "Hora", "value": "celda"},
                                        {"label": "Día", "value": "sector"},
                                        {"label": "Semana", "value": "EB"},
                                        {"label": "Mes", "value": "cluster"}],
                                    value="celda"
                                )
                    ], width=6)
                ], style={"height": "50%"}, align="center")
                
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
    ], style={"height": "30%"}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                html.Div(id="test",
                        children="Hola, haz una selección",
                        style={"height": "100%"}
                        )
            ], className="bg-primary text-white p-2 mb-2 text-center", style={"height": "100%"})
        ], width=12, align="center", style={"height": "100%"})
    ], justify="center", style={"height": "8%"}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.Spinner(children=dcc.Graph(id="map", style={"height": "100%"}))
                # dcc.Graph(id="map", style={"height": "100%"})
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
            dcc.Dropdown(id="KPI",
                        options=[
                            {"label": "Bussy Hour", "value": "BH"},
                            {"label": "PRB Occupation", "value": "PRB"},
                            {"label": "Traffic", "value": "Traffic"},
                            {"label": "User Experience", "value": "u_exp"}],
                        placeholder="Select a KPI"
                        ),
        ], width=3),

        dbc.Col([
            dbc.Button(id="update_kpi", n_clicks=0, children="Select")
        ], width=3)
    ]),

    dcc.Graph(id="graph_test", style={"height": "100%"}),

    # dl.Map([
    #     dl.TileLayer(),
    #     geojson_data
    # ],
    # id="mapis",
    # style={"width": "100%", "height": "100vh", "margin": "auto", "display": "block"},
    # center=[4.6837, -74.0566],  # Centrar el mapa en el promedio de las ubicaciones de los marcadores
    # zoom=10
    # )

    ],
    className="dbc",
    fluid=True,
    style={"height": "100vh"}
)



#---------- Callbacks ----------#
# Callback para actualizar la capa del mapa según la agregación que desee ver el usuario
@callback(
    Output(component_id='map', component_property='figure'),
    Input(component_id="aggregation", component_property='value'),
)
def update_map(input):
    px.set_mapbox_access_token("pk.eyJ1IjoicmFjdWVydm8iLCJhIjoiY2x2ZDBmcGF6MG92ejJpbTlyN3Q4d2tndyJ9.PFbAy_UzSktBPGnS23pU9A") # Mi public acces token de Mapbox
    if input is None:
        raise PreventUpdate
    # Definir las configuraciones de zoom y centro del mapa una vez
    map_layout = dict(zoom=5, center={"lat": 4.6837, "lon": -74.0566})
    # try:
    if input == "celda":
        fig = px.scatter_mapbox(df_geo, lat="dwh_latitud", lon="dwh_longitud",
                                zoom=map_layout["zoom"], center=map_layout["center"],
                                hover_name="dwh_cell_name_wom",
                                custom_data="dwh_cell_name_wom"
                            )
        #fig.update_traces(cluster=dict(enabled=True))
    
    elif input == "sector":
        sectores = df_geo.drop_duplicates(subset=["sector_name"]).copy() # Copia del df eliminando sectores iguales
        fig = px.scatter_mapbox(sectores, lat="dwh_latitud", lon="dwh_longitud",
                                color="sector",
                                zoom=map_layout["zoom"], center=map_layout["center"],
                                hover_name="sector_name",
                                custom_data="sector_name"
                            )

    elif input == "EB":
        aux_df = df_geo.drop_duplicates(subset=["node_name"]).copy()
        fig = px.scatter_mapbox(aux_df, lat="dwh_latitud", lon="dwh_longitud",
                                zoom=map_layout["zoom"], center=map_layout["center"],
                                hover_name="node_name",
                                custom_data="node_name"
                            )
        # # Habilita la clusterización de puntos agregando una nueva capa de datos de tipo 'scattermapbox'
        # fig.add_scattermapbox(lat=aux_df['dwh_latitud'], lon=aux_df['dwh_longitud'], mode='markers', marker={'size': 10})

        # # Actualiza las propiedades de la figura para habilitar la clusterización
        # fig.update_traces(marker=dict(size=10), selector=dict(type='scattermapbox'))

    elif input == "cluster":
        fig = px.choropleth_mapbox(clusters, geojson=clusters.geometry, locations=clusters.index,
                        color='Traf_Data_Act',
                        zoom=map_layout["zoom"], center=map_layout["center"],
                        opacity=0.5,
                        hover_name="key",
                        custom_data="key"
                        )
    
    elif input == "localidad":
        fig = px.choropleth_mapbox(localidades, geojson=localidades.geometry, locations=localidades.index,
                        zoom=map_layout["zoom"], center=map_layout["center"],
                        opacity=0.5,
                        hover_name="Nombre_localidad",
                        hover_data="Localidad",
                        custom_data="Localidad"
                        )
    
    elif input == "municipio":
        fig = px.choropleth_mapbox(municipios, geojson=municipios.geometry, locations=municipios.index,
                        zoom=map_layout["zoom"], center=map_layout["center"],
                        opacity=0.5,
                        hover_name="MPIO_CNMBR",
                        hover_data="MPIO_CCNCT",
                        custom_data="MPIO_CCNCT"
                        )
        
    elif input == "departamento":
        fig = px.choropleth_mapbox(departamentos, geojson=departamentos.geometry, locations=departamentos.index,
                        zoom=map_layout["zoom"], center=map_layout["center"],
                        opacity=0.5,
                        hover_name="DPTO_CNMBR",
                        hover_data="DPTO_CCDGO",
                        custom_data="DPTO_CCDGO"
                        )
    
    elif input == "regional":
        fig = px.choropleth_mapbox(regionales, geojson=regionales.geometry, locations=regionales.index,
                        zoom=map_layout["zoom"], center=map_layout["center"],
                        opacity=0.5,
                        hover_name="DPTO_REGIONAL",
                        custom_data="DPTO_REGIONAL"
                        )

    elif input == "total":
        data = {}
        fig = px.scatter_mapbox(data, 
                                zoom=map_layout["zoom"], center=map_layout["center"]
                                )
        
    #     else:
    #         raise ValueError("Opción de agregación no valida")
        
    # except Exception as e:
    #     print(f"Error al actualizar el mapa: {str(e)}")

    fig.update_layout(mapbox_style="open-street-map",
                    margin={"r":0,"t":0,"l":0,"b":0},
                    )

    return fig



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
        options_df = df_geo[["dwh_localidad", "dwh_dane_cod_localidad", "dane_nombre_mpio"]].drop_duplicates(subset=["dwh_dane_cod_localidad"]).copy() # Copia solo columnas requeridas
        options_df = options_df.dropna() # Elimino filas que contenga algun valor nulo
        options_df["CoLoc"] = options_df["dane_nombre_mpio"] + ": " + options_df["dwh_localidad"] + " " + options_df["dwh_dane_cod_localidad"].astype(str) # Nueva columna con nombre único de localidad
        options_df = options_df.sort_values(by=["CoLoc"]) # Organizo por nombre único
        options = [{'label': row["CoLoc"], 'value': row["dwh_dane_cod_localidad"]} for index, row in options_df.iterrows()]

    elif input == "municipio":
        # Tomo código DANE porque a partir del código es que funciona la lógica de los municipios
        options_df = df_geo[["dane_code","dane_nombre_mpio"]].drop_duplicates(subset=["dane_code"]).copy() # El parametro dentro de .drop_duplicates es para que considere solo las filas duplicadas según el código
        options_df = options_df.dropna() # Elimino filas que contenga algun valor nulo
        options_df["CoMpo"] = options_df["dane_nombre_mpio"] + " " + options_df["dane_code"].apply(str) # Sumo nombre de municipio con código para poder generar las opciones
        options_df = options_df.sort_values(by=["CoMpo"]) # Organizo por nombre único
        options = [{'label': row["CoMpo"], 'value': row["dane_code"]} for index, row in options_df.iterrows()]

    elif input == "departamento":
        options_df = df_geo[["dane_code_dpto","dane_nombre_dpt"]].drop_duplicates(subset=["dane_code_dpto"]).copy() # El parametro dentro de .drop_duplicates es para que considere solo las filas duplicadas según el código
        options_df = options_df.dropna() # Elimino filas que contenga algun valor nulo
        options_df["CoDpto"] = options_df["dane_nombre_dpt"] + " " + options_df["dane_code_dpto"].apply(str) # Sumo nombre de municipio con código para poder generar las opciones
        options_df = options_df.sort_values(by=["CoDpto"]) # Organizo
        options = [{'label': row["CoDpto"], 'value': row["dane_code_dpto"]} for index, row in options_df.iterrows()]

    elif input == "regional":
        options_df = df_geo["wom_regional"].drop_duplicates().copy() # El parametro dentro de .drop_duplicates es para que considere solo las filas duplicadas según el código
        options_df = options_df.dropna().sort_values()
        options = [{'label': i, 'value': i} for i in options_df]

    elif input == "total":
        options = {'label': 'Total de la red', 'value': "Total de la red"}
    
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

    if selected.isdigit(): # Si el valor es completamente compuesto de dígitos
        return to_int(selected) # Retorno como entero
    else:
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
    
    # auxdf = auxdf[auxdf.eq(input).any(axis=1)] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
    # print(auxdf)
    # lat_mean = auxdf["dwh_latitud"].astype(float).mean()
    # lon_mean = auxdf["dwh_longitud"].astype(float).mean()

    zoom = 14

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
        auxdf = df_geo[["dwh_dane_cod_localidad","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["dwh_dane_cod_localidad"] == to_int(input)] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
        lat_mean = auxdf["dwh_latitud"].astype(float).mean()
        lon_mean = auxdf["dwh_longitud"].astype(float).mean()
    elif agg == "municipio":
        zoom = 10
        auxdf = df_geo[["dane_code","dwh_latitud","dwh_longitud"]].copy() # Genero copia del df
        auxdf = auxdf[auxdf["dane_code"] == to_int(input)] # En el df busco la celda que concuerde con el valor del dropdown y guardo todas las filas
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

    patched_figure = Patch() # Patch para actualizar el atributo de una figura sin tener que crear la de nuevos
    patched_figure['layout']['mapbox']['zoom'] = zoom # Ruta para modificar el zoom
    patched_figure['layout']['mapbox']['center']['lat'] = lat_mean # Ruta para modificar atributo de latitud
    patched_figure['layout']['mapbox']['center']['lon'] = lon_mean # Ruta para modificar atributo de longitud


    return patched_figure



# Callback para gráficar la selección del usuario a partir de la agregación
@callback(
        Output(component_id='test', component_property='children'),
        Output(component_id="gauge", component_property="value"),
        Output(component_id='traffic', component_property='figure'),
        Output(component_id='user_exp', component_property='figure'),
        Output(component_id='bh', component_property='figure'),
        Output(component_id='PRB', component_property='figure'),
        Output(component_id='graph_test', component_property='figure'),
        State(component_id="aggregation", component_property='value'),
        Input(component_id='select', component_property='value')
)
def update_graphs(agg, input):
    if input is None:
        raise PreventUpdate
    
    selected_cell = input
    print("selección función graphs: ", selected_cell)
    print(type(selected_cell))
    #selected_cell = input['points'][0]["customdata"][0]
    #name = input['points'][0]["hovertext"]
    container = "Su selección es: {}".format(selected_cell)

    if(agg == "celda"): # Si se eligió una agregación de celda
        data = df[df["Cell_name"].str.upper() == selected_cell].copy() # Copia del df unicamente con las filas cuyo valor en la columna "Cell_name" concuerde con la celda que definí
        # Gráficar tráfico máximo y promedio
        # fig_uexp = px.line(data, x="Timestamp", y="L.Traffic.ActiveUser.DL.Max", title="Max Traffic by active users by cell")
        # fig_test = px.line(data, x="Timestamp", y="L.Traffic.ActiveUser.DL.Avg", title="Avg Traffic by active users by cell")

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)
        # fig_bh = px.bar(bh_df, x="Date", y="L.Traffic.ActiveUser.DL.Avg", title="BH by day", text="Time") # Graficar la hora pico por día y su valor correspondiente

        # Calculo ocupación PRBs
        prb_df = data[data["Timestamp"].isin(bh_df["Timestamp"])].copy() # Genero copia del df de datos unicamente de las casillas dentro del BH
        prb_df = prb_df[["Timestamp", "Cell_name", "L.ChMeas.PRB.DL.Avail", "L.ChMeas.PRB.DL.Used.Avg", "L.ChMeas.PRB.UL.Avail", "L.ChMeas.PRB.UL.Used.Avg"]] # Solo columnas necesarias
        # print("PRB OCCUP: ")
        # print(prb_df)
        prb_df["DL_PRB_usage"] = (prb_df["L.ChMeas.PRB.DL.Used.Avg"] / prb_df["L.ChMeas.PRB.DL.Avail"]) * 100
        prb_df["UL_PRB_usage"] = (prb_df["L.ChMeas.PRB.UL.Used.Avg"] / prb_df["L.ChMeas.PRB.UL.Avail"]) * 100
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")

        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")["L.Traffic.ActiveUser.DL.Avg"].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x="Timestamp", y="L.Traffic.ActiveUser.DL.Avg", title='Average users')

    
    elif(agg == "sector"):
        cells = df_geo[df_geo["sector_name"] == selected_cell].copy() # Genero dataframe solo con las celdas dentro del sector seleccionado
        data = filtrado(cells, df)

        # Gráficar tráfico máximo y promedio
        # datos_max = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Max'].sum().reset_index() # df con max data sumada
        # fig_uexp = px.line(datos_max, x='Timestamp', y='L.Traffic.ActiveUser.DL.Max', title='Max Traffic by active users by cell')
        # datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        # fig_trff = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Avg Traffic by active users by cluster')

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        # print("PRB usage:\n", prb_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')


    elif(agg == "EB"): # Si se eligio agregación de Estacion Base
        cells = df_geo[df_geo["node_name"] == selected_cell].copy() # Genero dataframe solo con las celdas dentro del nodo
        data = filtrado(cells, df)

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')

    elif(agg == "cluster"): # Si se eligió una agregación de cluster
        cells = df_geo[df_geo["cluster_key"] == selected_cell].copy() # Genero dataframe solo con las celdas dentro del cluster
        data = filtrado(cells, df)

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')

    elif(agg == "localidad"):
        cells = df_geo[df_geo["dwh_dane_cod_localidad"] == to_int(selected_cell)].copy() # Genero dataframe solo con las celdas dentro de la localidad
        data = filtrado(cells, df)

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')

    elif(agg == "municipio"): # Si se eligió una agregación de municipio
        cells = df_geo[df_geo["dane_code"] == to_int(selected_cell)].copy() # Genero dataframe solo con las celdas dentro del municipio
        data = filtrado(cells, df)

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')


    elif(agg == "departamento"): # Si se eligió una agregación de municipio
        cells = df_geo[df_geo["dane_code_dpto"] == to_int(selected_cell)].copy() # Genero dataframe solo con las celdas dentro del departamento
        data = filtrado(cells, df)

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')

    elif(agg == "regional"):
        cells = df_geo[df_geo["wom_regional"].str.lower() == unidecode(selected_cell.lower())].copy() # Genero dataframe solo con las celdas dentro del departamento
        data = filtrado(cells, df)

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)

        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')

    elif(agg == "total"):
        data = df.copy()
        # data["L.Traffic.ActiveUser.DL.Avg"] = data["L.Traffic.ActiveUser.DL.Avg"].apply(to_float) # Casting de string a float para esta columna

        # Calculo BH(hora pico) por día
        bh_df = bh(data, "L.Traffic.ActiveUser.DL.Avg")
        bh_df_max = bh(data, "L.Traffic.ActiveUser.DL.Max")
        fig_bh = graph_BH(bh_df, bh_df_max)


        # Calculo ocupación PRBs
        prb_df = PRB_usg(data, bh_df)
        fig_prb = px.line(prb_df, x="Timestamp", y=["DL_PRB_usage","UL_PRB_usage"], title="PRB usage in BH")
        gauge_value = prb_df["DL_PRB_usage"].mean() # Se saca el promedio de ocupación de PRBs de todos los días calculados
        print("gauge value: ", gauge_value)

        # Calculo y grafica de tráfico
        fig_trff = traffic(data, bh_df)

        # Calculo y gráfica de experiencia de usuario
        fig_uexp = user_exp(data, bh_df)

        # Gráfico de prueba
        datos_avg = data.groupby("Timestamp")['L.Traffic.ActiveUser.DL.Avg'].sum().reset_index() # df con average data sumada
        # fig_test = px.line(datos_avg, x='Timestamp', y='L.Traffic.ActiveUser.DL.Avg', title='Average users')
        fig_test = fig_bh

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
    # Actualiza el diseño de la figura para mover la leyenda
    fig_bh.update_layout(title="Max and avg users in BH", xaxis_title="Date", yaxis_title="Users",
                  legend=dict(orientation="h"
                            #   , yanchor="bottom", y=1.02, xanchor="right", x=1
                              ))
    fig_prb.update_layout( {
        "margin": {"l": 0, "r": 10, "b": 0, "t": 40}, 
        # "autosize": True
         })
    fig_prb.update_layout(legend=dict(    # Personalizar el diseño de la leyenda
        orientation="h",  # Orientación horizontal
        # yanchor="bottom",
        # y=1.02,  # Ajuste la posición vertical de la leyenda
        # xanchor="right",
        # x=1
    ))

    # Actualiza la información que se muestra al pasar el mouse
    fig_trff.update_traces(hovertemplate="<b>Date</b>: %{x}<br><b>Traffic</b>: %{y:.2f} GB")
    # Actualiza el diseño de la figura para mover la leyenda
    fig_trff.update_layout(title="Traffic and Traffic_BH", xaxis_title="Date", yaxis_title="Traffic (GB)",
                  legend=dict(orientation="h"
                            #   , yanchor="bottom", y=1.02, xanchor="right", x=1
                              ))

    return container, gauge_value, fig_trff, fig_uexp, fig_bh, fig_prb, fig_test

if __name__ == '__main__':
    app.run(debug=True)