import pandas as pd
from db.db import DBConnection
from log.log import CleanLogger

class MetArtDataNormalizer:

    def __init__(self, db_uri:str, db_raw_table_name:str):
        self.db_uri = db_uri
        self.db_raw_table_name = db_raw_table_name

    def normalize(self, logger:CleanLogger=None):
        used_columns = {}
        used_dfs = []
        foreign_keys = []

        # Create object types table
        used_dfs.append(self.__create_one_key_table(columns={'objectName':'OBJECT_TYPE'}, table_name='OBJECT_TYPES', id_col='OBJECT_TYPE_ID'))
        used_columns['objectName'] = 'OBJECT_TYPE'
        foreign_keys.append('OBJECT_TYPE_ID')

        # Create departments table
        used_dfs.append(self.__create_one_key_table(columns={'department':'DEPARTMENT_NAME'}, table_name='DEPARTMENTS', id_col='DEPARTMENT_ID'))
        used_columns['department'] = 'DEPARTMENT_NAME'
        foreign_keys.append('DEPARTMENT_ID')

        # Create cultures table
        used_dfs.append(self.__create_one_key_table(columns={'culture':'CULTURE_NAME'}, table_name='CULTURES', id_col='CULTURE_ID'))
        used_columns['culture'] = 'CULTURE_NAME'
        foreign_keys.append('CULTURE_ID')

        # Create mediums table
        used_dfs.append(self.__create_one_key_table(columns={'medium':'MEDIUM_NAME'}, table_name='MEDIUMS', id_col='MEDIUM_ID'))
        used_columns['medium'] = 'MEDIUM_NAME'
        foreign_keys.append('MEDIUM_ID')

        # Create classifications table
        used_dfs.append(self.__create_one_key_table(columns={'classification':'CLASSIFICATION_NAME'}, table_name='CLASSIFICATIONS', id_col='CLASSIFICATION_ID'))
        used_columns['classification'] = 'CLASSIFICATION_NAME'
        foreign_keys.append('CLASSIFICATION_ID')

        # Create artists table
        artist_columns = {
            'artistDisplayName': 'ARTIST_NAME',
            'artistDisplayBio': 'ARTIST_BIO',
            'artistBeginDate': 'ARTIST_BIRTH_YEAR',
            'artistEndDate': 'ARTIST_DEATH_YEAR',
            'artistGender': 'ARTIST_GENDER',
            'artistNationality': 'ARTIST_NATIONALITY'
        }
        used_dfs.append(self.__create_one_key_table(columns=artist_columns, table_name='ARTISTS', id_col='ARTIST_ID'))
        used_columns.update(artist_columns)
        foreign_keys.append('ARTIST_ID')

        # Create object table
        self.__create_objects_table(used_columns=used_columns, used_dfs=used_dfs, foreign_keys=foreign_keys)

    def __create_one_key_table(self, columns:dict[str], table_name:str, id_col:str=None):
        with DBConnection(self.db_uri) as db:
            df = db.get_distinct_columns(
                table_name=self.db_raw_table_name, 
                columns=list(columns.keys())
            ).rename(columns=columns)
            db.write_df_to_db(df=df, table_name=table_name, use_index_as_pkey=True, id_col=id_col)

        df = df.reset_index(names=id_col)
        return df

    def __create_objects_table(self, used_columns:dict, used_dfs:list[pd.DataFrame], foreign_keys:list[str]):
        with DBConnection(self.db_uri) as db:
            used_columns.update({
                    'objectID': 'OBJECT_ID',
                    'title': 'OBJECT_TITLE',
                    'objectBeginDate': 'OBJECT_STARTED_YEAR',
                    'objectEndDate': 'OBJECT_FINISED_YEAR',
                    'metadataDate': 'LAST_UPDATED_DATE',
            })

            df = db.get_distinct_columns(
                table_name=self.db_raw_table_name, 
                columns=list(used_columns.keys())
            ).rename(
                columns=used_columns
            )

            for used_df in used_dfs:
                join_cols = [col for col in used_df.columns if col in df.columns]
                df = df.merge(used_df, how='left', on=join_cols)
            
            df = df[['OBJECT_ID', 'OBJECT_TITLE', 'OBJECT_STARTED_YEAR', 'OBJECT_FINISED_YEAR', 'LAST_UPDATED_DATE'] + foreign_keys]

            db.write_df_to_db(df=df, table_name='OBJECTS', id_col='OBJECT_ID')


