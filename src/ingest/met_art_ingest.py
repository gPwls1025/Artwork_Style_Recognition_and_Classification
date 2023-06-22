import time
import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
from db.db import DBConnection
from log.log import IngestLogger

class MetArtDataIngester:

    def __init__(self,
                 db_uri:str='', 
                 db_table_name:str='',
                 db_table_primary_key:str='',
                 ingest_batch_size:int = 1000, 
                 log_batch_size:int = 100,
                 last_date_ingested:str=''):
        if db_uri:
            self.db_uri = db_uri
        if db_table_name:
            self.db_table_name = db_table_name
        if db_table_primary_key:
            self.db_table_primary_key = db_table_primary_key
        if ingest_batch_size:
            self.ingest_batch_size = ingest_batch_size
        if log_batch_size:
            self.log_batch_size = log_batch_size
        if last_date_ingested:
            self.last_date_ingested = last_date_ingested
        else:
            self.last_date_ingested = (datetime.now()-timedelta.days(7)).strftime('%Y-%m-%d')
        

    def __get_object_ids(self):
        all_object_ids = requests.get('https://collectionapi.metmuseum.org/public/collection/v1/objects').json()['objectIDs']
        updated_object_ids = requests.get(f'https://collectionapi.metmuseum.org/public/collection/v1/objects?metadataDate={self.last_date_ingested}').json()['objectIDs']
        with DBConnection(self.db_uri) as db:
            db_object_ids = []
            if db.table_exists(self.db_table_name):
                db_object_ids = db.get_id_list_from_db(self.db_table_name, self.db_table_primary_key)
        
        object_ids_to_insert = list(set(all_object_ids) - set(db_object_ids) | set(updated_object_ids))
        object_ids_to_update = list(set(db_object_ids) & set(updated_object_ids))
        object_ids_to_delete = list(set(db_object_ids) - set(all_object_ids))

        return object_ids_to_insert, object_ids_to_update, object_ids_to_delete

    def ingest_objects(self, logger:IngestLogger=None):
        object_ids_to_insert, object_ids_to_update, object_ids_to_delete = self.__get_object_ids()

        print(f"Total objects to insert: {len(object_ids_to_insert)}")
        print(f"Total objects to update: {len(object_ids_to_update)}")
        print(f"Total objects to delete: {len(object_ids_to_delete)}")
        print()

        if logger:
            logger.log(f"Starting ingest with {len(object_ids_to_insert)-len(object_ids_to_update)} objects to insert, {len(object_ids_to_update)} objects to update, and {len(object_ids_to_delete)} objects to delete.")

        # Inactivate objects that are no longer in the Met API
        if object_ids_to_delete:
            with DBConnection(self.db_uri) as db:
                db.set_rows_inactive(table_name=self.db_table_name, id_col=self.db_table_primary_key, id_list=object_ids_to_delete, active_col='isActive')
        
        # Remove objects that need to be updated from the db
        if object_ids_to_update:
            with DBConnection(self.db_uri) as db:
                db.delete_from_db(table_name=self.db_table_name, id_col=self.db_table_primary_key, id_list=object_ids_to_update)

        # Ingest new or updated objects
        if object_ids_to_insert:
            asyncio.run(self.__ingest_objects_async(object_ids_to_insert))
        
        if logger:
            logger.log(f"Ingest complete!")


    async def __ingest_objects_async(self, object_ids:list[int]=[]):
        current = 0
        failed = 0
        exception = None

        print("Starting ingest...")
        print(f"Total objects to ingest: {len(object_ids)}")
        print(f"Batch size: {self.ingest_batch_size}")
        print(f"Batches needed: {len(object_ids)//self.ingest_batch_size + 1}")
        print()
        while current<len(object_ids) and failed<3:
            object_id_batch = object_ids[current:(current+self.ingest_batch_size)]
            try:
                await self.__ingest_batch(object_id_batch = object_id_batch, batch_num = current//self.ingest_batch_size + 1)
                current += self.ingest_batch_size
                print(f"Ingested {min(current, len(object_ids))} of {len(object_ids)} objects")
                print()
                failed = 0
            except Exception as e:
                failed += 1
                print(f"Failed to ingest batch of {len(object_id_batch)} objects from index {current}. Trying again.")
                exception = e
        if failed==3:
            print("Failed to ingest same batch 3 times. Stopping ingest.")
            raise exception
        else:
            print("Finished ingest.")
            print()


    async def __ingest_batch(self, object_id_batch:list[int], batch_num:int=0):
        results = []
        connector = aiohttp.TCPConnector(limit=70)

        print(f"Starting batch {batch_num}")

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            url = 'https://collectionapi.metmuseum.org/public/collection/v1/objects/{}'
            wait_time = 1/70

            added = 0
            for objectID in object_id_batch:
                time.sleep(wait_time)
                try:
                    tasks.append(asyncio.create_task(session.get(url.format(objectID), ssl=False)))
                    added += 1
                except:
                    print(f'Failed to hit API endpoint for object {objectID}')
                if not added%self.log_batch_size:
                    print(f"Added {added} of {len(object_id_batch)} API calls in batch {batch_num}")
            
            completed = 0
            for task in asyncio.as_completed(tasks):
                try:
                    response = await task
                    data = await response.json()
                    results.append(data)
                except:
                    data = {}
                if not len(results)%self.log_batch_size:
                    completed += self.log_batch_size
                    print(f"Completed {completed} of {len(object_id_batch)} API calls in batch {batch_num}")
            
            if results:
                await asyncio.to_thread(self.__write_to_db, results)
                results.clear()
            
        print(f"Finished batch {batch_num}")
        print()


    def __write_to_db(self, data):
        with DBConnection(db_uri=self.db_uri) as con:
            con.write_dict_list_to_db(data=data, table_name=self.db_table_name)

