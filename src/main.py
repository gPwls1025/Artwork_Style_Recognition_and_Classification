
def main():
    import os
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

    with open(log_file, 'r') as f:
        last_line = f.readlines()[-1]
        last_date_ingested = last_line.split(' ')[0]

    logger = IngestLogger(log_file=log_file)
    
    ingester = MetArtDataIngester(
        db_uri=db_uri,
        db_table_name=db_table_name,
        db_table_primary_key=db_table_primary_key,
        ingest_batch_size=ingest_batch_size,
        log_batch_size=log_batch_size,
        last_date_ingested=last_date_ingested
    )

    logger.log(f'Starting ingest...')
    ingester.ingest_objects()
    logger.log(f'Ingest complete.')



if __name__=="__main__":
    main()
