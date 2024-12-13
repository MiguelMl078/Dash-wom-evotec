# Importación de librerías necesarias para la aplicación Dash
import dash
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from ran_dashboard import ran_layout, ran_callbacks, run_global_ran
from tx_dashboard import tx_layout, register_tx_callbacks, run_global_tx
from datetime import datetime
import math

#----------- Constantes -----------#
# Definición de colores en formato hexadecimal para usarlos en el diseño
MORADO_WOM = "#641f85"
MORADO_OSCURO = "#031e69"
MORADO_CLARO = "#cab2cd"
GRIS = "#8d8d8d"
MAGENTA = "#bb1677"
MAGENTA_OPACO = "#ac4b78"
date_global = datetime(2024, 12, 12, 8, 0)
#---------- Iniciar App ----------#
# Se define la aplicación Dash con estilos Bootstrap
# `suppress_callback_exceptions=True` permite cargar callbacks de componentes no inicializados

dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.PULSE, dbc_css], suppress_callback_exceptions=True)
load_figure_template("pulse")


#---------- App layout ----------#
# Diseño principal del contenedor con un `dcc.Loading` para mostrar un indicador de carga
app.layout = dbc.Container([
    dcc.Store(id='initialize-trigger', data=True),  # Almacén para el trigger de inicialización
    dcc.Loading(
        id="loading-panel",
        children=[
                dcc.Store(id='initialize-layout', data=False),  # Almacén para controlar la inicialización del layout
            ],
        color=MORADO_WOM,
        style={
            'position': 'fixed',  # Fija el símbolo en la pantalla
            'top': '50%',         # Alineación vertical
            'left': '50%',        # Alineación horizontal
            'transform': 'translate(-50%, -50%)',  # Centra el símbolo respecto al eje
            'zIndex': 1000        # Asegura que esté en la parte superior de otros elementos
        }
    ),
    # Título principal
    dbc.Row([
        dbc.Col([
            html.H1("Herramienta: DashWOM", className="text-center"),
        ], width={"size": 12}, className="bg-primary text-white p-2 mb-2")
    ]),
    
    # Contenedor de carga con diseño centrado
    html.Div(id='div-layout')
], className="dbc", fluid=True, style={"height": "100vh", "padding": "0"})

#---------- Callbacks de Inicialización ----------#
@app.callback(
    dash.Output('initialize-trigger', 'data'),
    dash.Output('initialize-layout', 'data'),
    dash.Input('initialize-trigger', 'data'),
)
def initialize_globals(trigger):
    if trigger:  # Solo se ejecuta si el trigger está activo
        # Ejecuta las funciones globales necesarias antes de cargar el layout, solo si han pasado 2 horas desde la aultima actualización
        global date_global
        date_actual = datetime.now()
        diff = (date_actual - date_global).total_seconds() / 3600
        if math.floor(diff) >= 2:
            run_global_ran()
            run_global_tx()
            date_global = date_actual
        
        return False, True # Desactiva el trigger después de la inicialización

# Callback para manejar cambios en el layout
@app.callback(
    dash.Output('initialize-layout', 'data', allow_duplicate=True),
    dash.Output('div-layout', 'children', allow_duplicate=True),
    dash.Input('initialize-layout', 'data'),
    prevent_initial_call=True
)
def update_layout(trigger):
    if trigger:
        layout = html.Div([
            dcc.Tabs(
                id='tabs', 
                value='RAN',
                children=[
                    dcc.Tab(
                        label='RAN', 
                        value='RAN',
                        children=[
                            dbc.Card(dbc.CardBody([
                                html.Div(ran_layout, className="mt-2")
                            ]), className="mb-3")
                        ],
                        style={'fontWeight': 'bold', 'fontSize': '16px'},
                        selected_style={'fontWeight': 'bold', 'fontSize': '18px', 'color': 'black'}
                    ),
                    dcc.Tab(
                        label='TX', 
                        value='TX',
                        children=[
                            dbc.Card(dbc.CardBody([
                                html.Div(tx_layout, className="mt-2")
                            ]), className="mb-3")
                        ],
                        style={'fontWeight': 'bold', 'fontSize': '16px'},
                        selected_style={'fontWeight': 'bold', 'fontSize': '18px', 'color': 'black'}
                    )
                ],
                className="mb-0"
            ),
        ], style={"paddingTop": "0px", "marginBottom": "0px"})
        
        return False, layout

# Registrar callbacks de otros módulos
ran_callbacks(app)
register_tx_callbacks(app)

#---------- Ejecutar App ----------#
if __name__ == '__main__':
    app.run_server(debug=True)
    #app.run(host='10.40.11.108', port=8050)
