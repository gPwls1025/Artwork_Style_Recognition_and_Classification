
def main():
    import os
    from datetime import datetime
    from dotenv import load_dotenv
    from ingest.met_art_ingest import MetArtDataIngester
    from ingest.log import IngestLogger

    load_dotenv()

    db_uri = os.getenv('MYSQL_URI')
    db_table_name = os.getenv('MYSQL_TABLE_NAME')
    db_table_primary_key = os.getenv('MYSQL_TABLE_PKEY')
    ingest_batch_size = int(os.getenv('INGEST_BATCH_SIZE'))
    log_batch_size = int(os.getenv('LOG_BATCH_SIZE'))
    log_file = os.getenv('LOG_FILE_NAME')

    logger = IngestLogger(log_file=log_file)
    last_date_ingested = logger.get_last_date_ingested()
    
    ingester = MetArtDataIngester(
        db_uri=db_uri,
        db_table_name=db_table_name,
        db_table_primary_key=db_table_primary_key,
        ingest_batch_size=ingest_batch_size,
        log_batch_size=log_batch_size,
        last_date_ingested=last_date_ingested
    )

    ingester.ingest_objects(logger=logger)


if __name__=="__main__":
    main()
