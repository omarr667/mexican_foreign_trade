import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO 
import sqlite3
import os
import requests
from pathlib import Path


conn = sqlite3.connect('data/mexico_comercio_exterior.sql')
cur = conn.cursor()

# Abrir el archivo HTML
with open("insumos/code_countries.html") as fp:
    soup = BeautifulSoup(fp, 'html.parser')

# Encontrar todos options
options = soup.find_all('option')

countries = {
    option['value']: option.text
    for option in options
    if option['value']!= '0'
}
df_countries = pd.DataFrame.from_dict(countries, orient='index', columns=['country'])
df_countries.reset_index(inplace=True)
df_countries.rename(columns={'index': 'code'}, inplace=True)
df_countries.to_sql('countries', conn, if_exists='replace', index=False)




# Definir los tipos de series de tiempo
type_time_serie = {
    'bc_e': 'Balanza comercial anual',
    'bcm_e': 'Balanza comercial mensual',
    'ppm_e': 'Principales productos de importación',
    'ppx_e': 'Principales productos de exportación'
}

df_type_time_serie = pd.DataFrame.from_dict(type_time_serie, orient='index', columns=['type_time_serie'])
df_type_time_serie.reset_index(inplace=True)
df_type_time_serie.rename(columns={'index': 'code'}, inplace=True)
df_type_time_serie.to_sql('type_time_serie', conn, if_exists='replace', index=False)



# Create base directory for raw data
raw_data_path = Path('data/raw')
raw_data_path.mkdir(parents=True, exist_ok=True)

# Create directories for each type of time series
for ts_type in type_time_serie:
    (raw_data_path / ts_type).mkdir(exist_ok=True)


for country_code, country_name in countries.items():
    for ts_code, ts_name in type_time_serie.items():
        try:
            url = f"http://187.217.44.197/sic_php/pages/estadisticas/mexico/{country_code}{ts_code}.html"
            
            # Request with timeout and error handling
            try:
                response = requests.get(url, timeout=30)
                response.encoding = 'ISO-8859-1'
                response.raise_for_status()  # Raises an HTTPError for bad responses
            except requests.RequestException as e:
                print(f"Failed to fetch data for {country_name} ({ts_name}): {str(e)}")
                continue

            # Save HTML file
            file_path = raw_data_path / ts_code / f"{country_code}_{country_name}.html"
            try:
                with open(file_path, "w", encoding='utf-8') as f:
                    f.write(response.text)
            except IOError as e:
                print(f"Failed to save HTML file for {country_name} ({ts_name}): {str(e)}")
                continue

            # Process data only if response was successful
            try:
                df_list = pd.read_html(StringIO(response.text), header=0)
                if not df_list:
                    print(f"No tables found in HTML for {country_name} ({ts_name})")
                    continue
                
                df_write = df_list[0]
                table_name = f'{ts_code}_{country_code}'
                df_write.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"Successfully processed data for {country_name} ({ts_name})")
                
            except Exception as e:
                print(f"Error processing data for {country_name} ({ts_name}): {str(e)}")
                
        except Exception as e:
            print(f"Unexpected error for {country_name} ({ts_name}): {str(e)}")
            continue