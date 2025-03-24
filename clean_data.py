import sqlite3
import pandas as pd


def clean_data(table):
    """
    Clean data from mexico_comercio_exterior.sql
    """
        
    meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    df = pd.read_sql(f"SELECT * FROM {table}", conn)

    # Agregar la columna 'year' usando mask y ffill
    df['year'] = df['Año'].mask(df['Año'].isin(meses)).ffill()

    # Filtrar las filas donde 'Año' no sea igual a 'year'
    df = df[df['Año'].isin(meses)].copy()

    # Crear la columna 'mes' con índice del mes correspondiente
    df['mes'] = df['Año'].map(lambda x: meses.index(x) + 1)
    df = df.rename(columns={'Año': 'Mes Nombre', 
                            'year': 'Año',
                            'mes': 'Mes'})

    df = df[['Año', 'Mes', 'Exportaciones', 'Importaciones',  'Comercio Total',  'Balanza Comercial']]
    return df




conn = sqlite3.connect('data/mexico_comercio_exterior.db')
cur = conn.cursor()


# consulta todas las tablas que tienen el sufijo bcm_e
cur.execute("SELECT name FROM sqlite_master where type = 'table' and name like 'bcm_e_%'")

tables = cur.fetchall()

for table in tables:
    
    table_name = table[0]
    df = clean_data(table_name)
    df.to_sql(f"clean_{table_name}", conn, if_exists='replace', index=False)










