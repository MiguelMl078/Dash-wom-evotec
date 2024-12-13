# BD PostgreSQL info de celdas
# Aqui está contenida las tablas de información geográgica por celda
# name: analytics_prod
# schema: bodega_analitica
BD_GEO_PARAMS = {
    "host":"10.40.111.106",
    "database":"analytics_prod",
    "user":"evotec",
    "password":"3v0t3c",
    "port":"5432"
}

# BD PostgreSQLde desarrollo de Sergio
# Aqui está contenida las tablas según agregación geográfica para el funcionamieto de la app
# name: analytics_dev
# schema: public
BD_DATA_PARAMS = {
    "host":"10.40.111.100",
    "database":"analytics_dev",
    "user":"reobertocuervo",
    "password":"w0m_2024*",
    "port":"5432"
}

# BD MySQL de desempeño para topología TX
BD_DESEMPENO_PARAMS = {
    "user":"planeacion_tec",
    "password":"W0m3r5+",
    "host":"10.40.111.42",
    "port":3306,
    "database":"collector_qa"
}