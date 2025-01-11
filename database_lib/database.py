from sqlalchemy import create_engine, Table, Column, Integer, Text, Float, Boolean, MetaData, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
import pandas as pd


class DB:
    def __init__(self):
        # Load database credentials
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        load_dotenv(env_path)
        
        # Create database connection
        conn_params = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        conn_string = (
            f"postgresql://{conn_params['user']}:{quote_plus(str(conn_params['password']))}"
            f"@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
            "?sslmode=require"
        )
        
        self.engine = create_engine(conn_string)
        self.metadata = MetaData()

        self.type_mapping = {
            'Integer': 'TEXT',
            'Text': 'TEXT',
            'Float': 'FLOAT',
            'Boolean': 'BOOLEAN',
            'vector(1536)': 'vector(1536)',
            'JSONB': 'JSONB'
        }

    def create_new_table(self, table_name, columns):
        """Creates a new pulso table for a specific project if it doesn't exist
        
        Args:
            table_name (str): Name of the table to create
            columns (list): List of column definitions. Each column should be a dictionary with:
                - name (str): Column name
                - type (str): Column type ('Integer', 'Text', 'Float', 'Boolean')
                - primary_key (bool, optional): If True, column is primary key. Default False
                - nullable (bool, optional): If True, column can be null. Default True
                
        Example:
            columns = [
                {'name': 'id', 'type': 'Integer', 'primary_key': True},
                {'name': 'grupo', 'type': 'Text', 'nullable': False},
                {'name': 'volumen_grupo', 'type': 'Integer'},
                {'name': 'urls', 'type': 'Text'}
            ]
        """
        # Map string types to SQLAlchemy types
        type_mapping = {
            'Integer': Integer,
            'Text': Text,
            'Float': Float,
            'Boolean': Boolean,
            'vector(1536)': None  # Special handling for vector type
        }
        
        # Create column definitions
        column_definitions = []
        for col in columns:
            if col['type'] not in type_mapping:
                raise ValueError(f"Invalid column type: {col['type']}. Must be one of: {list(type_mapping.keys())}")
            
            if col['type'] == 'vector(1536)':
                # Skip vector columns for now - we'll add them after table creation
                continue
            
            column_definitions.append(
                Column(
                    col['name'],
                    type_mapping[col['type']],
                    primary_key=col.get('primary_key', False),
                    nullable=col.get('nullable', True)
                )
            )
        
        # Define table
        pulso_table = Table(
            table_name,
            self.metadata,
            *column_definitions,
            schema='public'
        )
        
        try:
            # Drop table if exists to ensure correct structure
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()
            
            # Create table with updated structure
            self.metadata.create_all(self.engine)
            
            # Add vector columns separately
            with self.engine.connect() as conn:
                for col in columns:
                    if col['type'] == 'vector(1536)':
                        conn.execute(text(f"""
                            ALTER TABLE {table_name} 
                            ADD COLUMN {col['name']} vector(1536)
                        """))
                conn.commit()

            print(f"Table {table_name} created successfully with the updated structure")
            return True
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
            return False
    
    def create_new_columns(self, table_name, columns):
        """
        Añade nuevas columnas a una tabla existente del proyecto.
        
        Args:
            project (str): Nombre del proyecto
            columns (list): Lista de definiciones de columnas. Cada columna debe ser un diccionario con:
                - name (str): Nombre de la columna
                - type (str): Tipo de columna ('Integer', 'Text', 'Float', 'Boolean')
                
        Example:
            columns = [
                {'name': 'raw_pain_points', 'type': 'Text'},
                {'name': 'sentiment_score', 'type': 'Float'},
                {'name': 'is_processed', 'type': 'Boolean'}
            ]
        """
        # Map string types to SQL types
        type_mapping = {
            'Integer': 'INTEGER',
            'Text': 'TEXT',
            'Float': 'FLOAT',
            'Boolean': 'BOOLEAN'
        }
        
        try:
            with self.engine.connect() as conn:
                for column in columns:
                    if column['type'] not in type_mapping:
                        raise ValueError(f"Invalid column type: {column['type']}. Must be one of: {list(type_mapping.keys())}")
                    
                    sql_type = type_mapping[column['type']]
                    conn.execute(text(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS {column['name']} {sql_type}
                    """))
                
                conn.commit()
                print(f"Columnas añadidas a la tabla {table_name} exitosamente")
                
        except Exception as e:
            print(f"Error al añadir columnas a la tabla {table_name}: {e}")

    def read_table_content_in_list(self, table_name, columns=None):
        """
        Lee el contenido de una tabla específica del proyecto
        
        Args:
            table_name (str): Nombre de la tabla
            columns (list, optional): Lista de nombres de columnas a seleccionar.
                                    Si es None, selecciona todas las columnas.
        
        Returns:
            list: Lista de diccionarios con los resultados
        """
        
        try:
            with self.engine.connect() as conn:
                # Construir la consulta según las columnas especificadas
                if columns:
                    columns_str = ", ".join(columns)
                    query = text(f"SELECT {columns_str} FROM {table_name}")
                else:
                    query = text(f"SELECT * FROM {table_name}")
                
                # Ejecutar la consulta y obtener resultados
                result = conn.execute(query)
                
                # Obtener los nombres de las columnas
                result_columns = result.keys()
                
                # Convertir resultados a lista de diccionarios
                rows = [dict(zip(result_columns, row)) for row in result]
                
                print(f"Se encontraron {len(rows)} registros en {table_name}")
                return rows
                
        except Exception as e:
            print(f"Error al leer la tabla {table_name}: {e}")
            return []

    def read_table_in_df(self, table_name, columns=None):
        """
        Lee la tabla del proyecto y devuelve un DataFrame de pandas.
        
        Args:
            project (str): Nombre del proyecto
            columns (list): Lista de columnas a seleccionar
        
        Returns:
            pandas.DataFrame: DataFrame con los resultados
        """        
        try:
            with self.engine.connect() as conn:
                if columns:
                    columns_str = ", ".join(columns)
                    query = text(f"SELECT {columns_str} FROM {table_name}")
                else:
                    query = text(f"SELECT * FROM {table_name}")
                
                df = pd.read_sql(query, conn)
                return df
                
        except Exception as e:
            print(f"Error al leer la tabla en DataFrame: {e}")
            return pd.DataFrame()

    def update_by_direct_query(self, table_name, sql_query, params=None):
        """
        Ejecuta una actualización en la tabla usando una consulta SQL directa.
        
        Args:
            table_name (str): Nombre de la tabla a actualizar
            sql_query (str): Consulta SQL de actualización (sin el UPDATE tabla_name)
            params (dict, optional): Diccionario con los parámetros para la consulta
        
        Examples:
            # Actualizar un solo registro
            db.update_by_direct_query(
                'mi_tabla',
                'SET columna1 = :valor WHERE id = :id',
                {'valor': 'nuevo_valor', 'id': 123}
            )
            
            # Actualizar múltiples columnas
            db.update_by_direct_query(
                'mi_tabla',
                'SET col1 = :val1, col2 = :val2 WHERE condicion = :cond',
                {'val1': 10, 'val2': 'texto', 'cond': True}
            )
        
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        try:
            with self.engine.connect() as conn:
                # Construir la consulta completa
                full_query = f"UPDATE {table_name} {sql_query}"
                
                # Ejecutar la actualización
                result = conn.execute(text(full_query), parameters=params or {})
                conn.commit()
                
                rows_affected = result.rowcount
                print(f"Se actualizaron {rows_affected} registros en {table_name}")
                return rows_affected > 0
                
        except Exception as e:
            print(f"Error al actualizar la tabla {table_name}: {e}")
            return False

    def delete_table(self, table_name):
        """
        Elimina una tabla completa de la base de datos.
        
        Args:
            table_name (str): Nombre de la tabla a eliminar
            
        Returns:
            bool: True si la eliminación fue exitosa, False en caso contrario
        """
        try:
            with self.engine.connect() as conn:
                condition = input(f"¿Estás seguro de que deseas eliminar la tabla {table_name}? (y/n)")
                if condition == 'y':
                    query = text(f"DROP TABLE IF EXISTS {table_name}")
                    conn.execute(query)
                    conn.commit()
                    print(f"Tabla {table_name} eliminada exitosamente")
                    return True
                
        except Exception as e:
            print(f"Error al eliminar la tabla {table_name}: {e}")
            return False

    def delete_table_rows(self, table_name, conditions=None):
        """
        Elimina filas de una tabla basado en condiciones específicas.
        
        Args:
            table_name (str): Nombre de la tabla
            conditions (dict, opcional): Diccionario con las condiciones de eliminación.
                Formato: {
                    'column': nombre_columna,
                    'operator': operador,
                    'value': valor
                }
                Operadores soportados: 
                - '=', '!=', '<', '>', '<=', '>=', 'IS NULL', 'IS NOT NULL', 'LIKE', 'IN'
                
        Ejemplos de uso:
            # Eliminar todos los registros
            db.delete_table_rows('mi_tabla')
            
            # Eliminar registros donde volumen_grupo sea menor a 100
            db.delete_table_rows('mi_tabla', {
                'column': 'volumen_grupo',
                'operator': '<',
                'value': 100
            })
            
            # Eliminar registros con urls vacías
            db.delete_table_rows('mi_tabla', {
                'column': 'urls',
                'operator': 'IS NULL'
            })
        
        Returns:
            bool: True si la eliminación fue exitosa, False en caso contrario
        """
        try:
            with self.engine.connect() as conn:
                base_query = f"DELETE FROM {table_name}"
                
                if conditions:
                    # Validar operador
                    valid_operators = ['=', '!=', '<', '>', '<=', '>=', 'IS NULL', 'IS NOT NULL', 'LIKE', 'IN']
                    operator = conditions.get('operator')
                    
                    if operator not in valid_operators:
                        raise ValueError(f"Operador no válido. Debe ser uno de: {valid_operators}")
                    
                    # Construir WHERE según el tipo de operador
                    if operator in ['IS NULL', 'IS NOT NULL']:
                        where_clause = f"WHERE {conditions['column']} {operator}"
                        query = text(f"{base_query} {where_clause}")
                        params = {}
                    elif operator == 'IN':
                        where_clause = f"WHERE {conditions['column']} IN :value"
                        query = text(f"{base_query} {where_clause}")
                        params = {'value': tuple(conditions['value'])}
                    else:
                        where_clause = f"WHERE {conditions['column']} {operator} :value"
                        query = text(f"{base_query} {where_clause}")
                        params = {'value': conditions['value']}
                
                else:
                    query = text(base_query)
                    params = {}
                
                result = conn.execute(query, params)
                conn.commit()
                
                print(f"Se eliminaron {result.rowcount} registros de {table_name}")
                return True
                
        except Exception as e:
            print(f"Error al eliminar registros de {table_name}: {e}")
            return False

    def delete_table_columns(self, table_name, columns):
        """
        Elimina una o más columnas de una tabla existente.
        
        Args:
            table_name (str): Nombre de la tabla
            columns (list): Lista de nombres de columnas a eliminar
                
        Ejemplo:
            # Eliminar una sola columna
            db.delete_table_columns('mi_tabla', ['columna_obsoleta'])
            
            # Eliminar múltiples columnas
            db.delete_table_columns('mi_tabla', ['col1', 'col2', 'col3'])
        
        Returns:
            bool: True si la eliminación fue exitosa, False en caso contrario
        """
        try:
            with self.engine.connect() as conn:
                for column in columns:
                    query = text(f"""
                        ALTER TABLE {table_name}
                        DROP COLUMN IF EXISTS {column}
                    """)
                    conn.execute(query)
                
                conn.commit()
                print(f"Columnas {', '.join(columns)} eliminadas exitosamente de la tabla {table_name}")
                return True
                
        except Exception as e:
            print(f"Error al eliminar columnas de la tabla {table_name}: {e}")
            return False

