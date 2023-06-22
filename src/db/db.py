import pandas as pd
from sqlalchemy import create_engine, text

class DBConnection:

    def __init__(self, db_uri:str):
        if db_uri:
            self.con = create_engine(db_uri).connect()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()

    def execute(self, query:str):
        return self.con.execute(text(query))
    
    def commit(self):
        self.con.commit()
    
    def table_exists(self, table_name:str):
        query = f"SHOW TABLES LIKE '{table_name}'"
        return len(self.execute(query=query).all())>0

    def write_dict_list_to_db(self, data:list[dict], table_name:str):
        df = pd.DataFrame(data)
        df['isActive'] = 1
        df = df.convert_dtypes()
        df = df.drop(df.dtypes[df.dtypes=='object'].index.to_list(), axis=1)
        df = df[df.objectID.notna()]
        df.to_sql(name=table_name, con=self.con, if_exists='append', index=False, method='multi')
        print(f"Wrote {len(df.index)} rows to {table_name}")
    
    def get_id_list_from_db(self, table_name:str, id_col:str):
        query = f"SELECT {id_col} FROM {table_name}"
        return [row[0] for row in self.execute(query=query)]

    def delete_from_db(self, table_name:str, id_col:str, id_list:list[int]):
        query = f"DELETE FROM {table_name} WHERE {id_col} IN ({','.join([str(id) for id in id_list])})"
        self.execute(query=query)
        self.commit()
        print(f"Deleted {len(id_list)} rows from {table_name}")
        print()

    def set_rows_inactive(self, table_name:str, id_col:str, id_list:list[int], active_col:str='isActive'):
        query = f"UPDATE {table_name} SET {active_col}=0 WHERE {id_col} IN ({','.join([str(id) for id in id_list])})"
        self.execute(query=query)
        self.commit()
        print(f"Set {len(id_list)} rows in {table_name} to inactive")
        print()

    def get_table(self, table_name:str):
        return pd.read_sql(table_name, con=self.con)
    
    def get_columns(self, table_name:str, columns:list[str]):
        return pd.read_sql(table_name, columns=columns,con=self.con)
    
    def get_distinct_columns(self, table_name:str, columns:list[str]):
        return pd.read_sql(f"SELECT DISTINCT {','.join(columns)} FROM {table_name}", con=self.con)
    
    def write_df_to_db(self, df:pd.DataFrame, table_name:str, use_index_as_pkey:bool=False, id_col:str=None, if_exists:str='replace'):
        if use_index_as_pkey:
            df.to_sql(name=table_name, con=self.con, if_exists=if_exists, index_label=id_col, method='multi')
        else:
            df.to_sql(name=table_name, con=self.con, if_exists=if_exists, index=False, method='multi')
        self.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({id_col});")
        self.commit()
        print(f"Created table {table_name} with {len(df.index)} rows")

