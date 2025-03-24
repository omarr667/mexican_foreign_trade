import sqlite3
import pandas as pd



conn = sqlite3.connect('data/mexico_comercio_exterior.db')
cur = conn.cursor()



# consulta todas las tablas que tienen el sufijo bcm_e
cur.execute("SELECT * FROM sqlite_master where type = 'table' and name like 'bcm_e_%'")

tables = cur.fetchall()

table = tables[0][1]

meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
df = pd.read_sql(f"SELECT * FROM {table}", conn)

years = []
year = ''
for index, row in df.iterrows():
    if row['Año'] not in meses:
        year = row["Año"]
    years.append(year)

df['year'] = years

df = df[df['year'] != df['Año']]







def clean_data(df):
    """
    Clean data from mexico_comercio_exterior.sql
    """
    










