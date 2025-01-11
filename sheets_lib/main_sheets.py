from datetime import date
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import time
import math


class GoogleSheets:

    def __init__(self,sheet_id):
        creds_file = "/home/snparada/Spacionatural/Libraries/sheets_lib/creds/gs_credentials.json"
        self.scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, self.scope)
        self.client = gspread.authorize(self.creds)
        self.sheet_id = sheet_id


    #CRUD
    def read_dataframe(self, page_name, dtype=None):
        # Abrir hoja y página especificada
        sheet = self.client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(page_name)

        # Convertir a DataFrame
        data = worksheet.get_all_records()

        # Si no hay registros, crea un DataFrame vacío con los encabezados
        if not data:
            headers = worksheet.row_values(1)  # Obtiene los valores de la primera fila
            df = pd.DataFrame(columns=headers)
        else:
            df = pd.DataFrame(data)

        # Convertir las columnas especificadas
        if dtype:
            for column, column_type in dtype.items():
                df[column] = df[column].astype(column_type)

        return df

    def update_all_data_by_dataframe(self, df, page_name):
        """
        Actualiza la página de Google Sheets con los datos del DataFrame usando set_with_dataframe.

        Parámetros:
            df (pd.DataFrame): DataFrame con los datos a actualizar.
            page_name (str): Nombre de la página donde se desea actualizar.
        """
        
        # Abrir hoja y página especificada
        sheet = self.client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(page_name)
        
        # Usar set_with_dataframe para actualizar la hoja de cálculo
        set_with_dataframe(worksheet, df, row=1, col=1, include_index=False, include_column_header=True, resize=False)

    def update_data(self, df, start_cell, end_cell, page_name):
        """
        Updates the spreadsheet with data from the DataFrame starting at the specified start cell and ending at the end cell.
        
        Parameters:
            df (pd.DataFrame): DataFrame containing the data to be updated.
            start_cell (str): Start cell, e.g., 'A1'.
            end_cell (str): End cell, e.g., 'D10'.
            page_name (str): Name of the page where the data is to be updated.
        """
        
        # Open the specified sheet and worksheet
        sheet = self.client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(page_name)
        
        # Create a range from the specified start and end cells
        cell_range = f"{start_cell}:{end_cell}"
        
        # Get the DataFrame values in the list of lists format
        values = df.values.tolist()

        import warnings

        # Suppress specific UserWarning from gspread
        warnings.filterwarnings("ignore", category=UserWarning, module="gspread.worksheet")
        
        # Update the specified range with the DataFrame values
        worksheet.update(cell_range, values)

    def update_data_by_columns(self, df, columns, page_name):
        """
        Actualiza la hoja de Google Sheets agregando las columnas especificadas del DataFrame al final de la última columna con datos, incluyendo los encabezados.

        Parámetros:
            df (pd.DataFrame): DataFrame con los datos a actualizar.
            columns (list): Lista de nombres de columnas en el DataFrame que se quieren agregar.
            page_name (str): Nombre de la página donde se desea actualizar.
        """
        
        # Abrir hoja y página especificada
        sheet = self.client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(page_name)

        # Obtener la última columna con datos
        last_col = len(worksheet.get_all_values()[0])

        # Iterar sobre las columnas especificadas y actualizar
        for i, column in enumerate(columns, start=1):
            # Verificar si la columna existe en el DataFrame
            if column in df.columns:
                # Preparar los valores de la columna, incluyendo el encabezado
                col_values = [[column]] + [[value] for value in df[column]]
                col_index = gspread.utils.rowcol_to_a1(1, last_col + i)
                worksheet.update(col_index, col_values)

    def update_cells_by_key(self, df, key_column_sheet, key_column_df, value_column_sheet, value_column_df, page_name):
        """
        Actualiza celdas en una hoja de cálculo basándose en una columna llave y una columna de valores.

        Parámetros:
            df (pd.DataFrame): DataFrame con los datos.
            key_column (str): Nombre de la columna que actúa como llave.
            value_column (str): Nombre de la columna con los valores a actualizar.
            page_name (str): Nombre de la página donde se desea actualizar.
        """

        # Abrir hoja y página especificada
        sheet = self.client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(page_name)

        # Crear un diccionario a partir del DataFrame para mapear key_column_df a value_column_df
        # Convertimos las llaves a str aquí
        update_dict = dict(zip(df[key_column_df].astype(str), df[value_column_df]))

        # Iterar sobre las filas del Google Sheets con enumerate para obtener el índice de la fila
        for row_index, row in enumerate(worksheet.get_all_values(), start=1):  # Se inicia en 1 porque las filas en Google Sheets comienzan desde 1
            # Convertimos key_value a str aquí
            key_value = str(row[ord(key_column_sheet) - ord('A')])

            if key_value in update_dict:
                # Determinar la columna objetivo en Google Sheets
                col_index = ord(value_column_sheet) - ord('A') + 1
                
                # Obtener el valor actual de la celda en esa posición
                current_value = row[col_index - 1]

                # Si el valor actual está vacío, entonces actualizamos
                if not current_value:
                    value_to_update = update_dict[key_value]
                    
                    if isinstance(value_to_update, date):  # Asegúrate de haber importado date desde datetime
                        value_to_update = value_to_update.strftime('%Y-%m-%d')  # Convierte la fecha a una cadena en formato YYYY-MM-DD
                    elif isinstance(value_to_update, float) and (math.isnan(value_to_update) or math.isinf(value_to_update)):
                        value_to_update = 0  # o cualquier otro valor que desees usar como reemplazo

                    worksheet.update_cell(row_index, col_index, value_to_update)
                    time.sleep(1)

    def delete_content_except_first_row(self, page_name):
        """
        Borra todo el contenido de la página especificada excepto la primera fila.

        Parámetros:
            page_name (str): Nombre de la página donde se desea borrar el contenido.
        """
        
        # Abrir hoja y página especificada
        sheet = self.client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(page_name)

        # Obtener el número total de filas y columnas
        total_rows = worksheet.row_count
        total_cols = worksheet.col_count

        # Si hay más de una fila, borramos el contenido desde la segunda fila en adelante
        if total_rows > 1:
            # Crear una lista de listas vacías con la misma estructura que los datos que deseamos borrar
            empty_data = [['' for _ in range(total_cols)] for _ in range(total_rows - 1)]

            # Actualizar las celdas con datos vacíos
            worksheet.update(f'A2:{gspread.utils.rowcol_to_a1(total_rows, total_cols)}', empty_data)


    #Otras Funciones
    def append_data_to_sheet(self, sheet_range, data):
        try:
            # Crea un servicio de Google Sheets con las credenciales proporcionadas
            service = build("sheets", "v4", credentials=self.creds)

            # Convierte los datos de la API en una lista de listas
            values = [data.columns.tolist()] + data.values.tolist()

            # Crea el cuerpo del mensaje en formato JSON
            body = {"values": values[1:]}

            # Agrega los datos a la hoja de cálculo
            result = (
                service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=self.sheet_id,
                    range=sheet_range,
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                )
                .execute()
            )
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None

    def list_worksheets(self):
        sheet = self.client.open_by_key(self.sheet_id)
        return [ws.title for ws in sheet.worksheets()]

    def extract_new_data(self, df_old, df_recent, key_column):
        df_old = df_old.copy()  # Copia para evitar SettingWithCopyWarning
        df_recent = df_recent.copy()  # Copia para evitar SettingWithCopyWarning

        # Asegurándonos de que la columna clave en ambos DataFrames sea de tipo string
        df_old[key_column] = df_old[key_column].astype(str)
        df_recent[key_column] = df_recent[key_column].astype(str)
        
        nuevos_registros = df_recent[~df_recent[key_column].isin(df_old[key_column])]
        return nuevos_registros.reset_index(drop=True)

    def highlight_duplicate_rows(self, page_name, columns_to_check):
        """
        Resalta las filas con valores duplicados en las columnas especificadas.

        Parámetros:
            page_name (str): Nombre de la página donde se desea actualizar.
            columns_to_check (list): Lista de nombres de columnas para verificar duplicados.
        """
        # Leer el DataFrame de la hoja de cálculo
        sheet_df = self.read_dataframe(page_name)

        # Identificar filas duplicadas en las columnas especificadas
        duplicate_rows = sheet_df[sheet_df.duplicated(subset=columns_to_check, keep='first')]

        # Cambiar el color de las filas duplicadas a rojo
        for row_number in duplicate_rows.index + 2:  # +2 porque el DataFrame es base 0 y hay un encabezado en la hoja de cálculo
            self.set_row_color(row_number, (0.9, 0.6, 0.6), page_name)

    def highlight_rows_based_on_value(self, page_name, column_name, value_to_check):
        """
        Resalta las filas donde una columna específica tiene un valor específico.

        Parámetros:
            page_name (str): Nombre de la página donde se desea actualizar.
            column_name (str): Nombre de la columna a verificar.
            value_to_check: Valor a buscar en la columna especificada.
        """
        # Leer el DataFrame de la hoja de cálculo
        sheet_df = self.read_dataframe(page_name)

        # Identificar filas donde la columna tiene el valor especificado
        matching_rows = sheet_df[sheet_df[column_name] == value_to_check]

        # Cambiar el color de las filas que coinciden a rojo pastel
        for row_number in matching_rows.index + 2:  # +2 porque el DataFrame es base 0 y hay un encabezado en la hoja de cálculo
            self.set_row_color(row_number, (0.9, 0.6, 0.6), page_name)


    #Funciones Auxiliares
    def set_row_color(self, row_number, color, page_name):
        """
        Cambia el color de una fila completa en una hoja de cálculo.

        Parámetros:
            row_number (int): Número de fila que se desea actualizar.
            color (tuple): Color en formato RGB, por ejemplo, (1, 0, 0) para rojo.
            page_name (str): Nombre de la página donde se desea actualizar.
        """

        # Abrir hoja y página especificada
        sheet = self.client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(page_name)

        # Convertir el número de fila y el color a formato adecuado para la API
        start_index = row_number - 1  # La API utiliza índices base 0
        end_index = row_number

        red, green, blue = color
        body = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": worksheet._properties['sheetId'],
                            "startRowIndex": start_index,
                            "endRowIndex": end_index
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": red,
                                    "green": green,
                                    "blue": blue
                                }
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor)"
                    }
                }
            ]
        }

        # Usar la API de nivel inferior para hacer la solicitud
        sheet.batch_update(body)
        time.sleep(2)