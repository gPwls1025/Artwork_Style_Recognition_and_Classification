
def main():
    import os
    from datetime import datetime
    from dotenv import load_dotenv
    from ingest.met_art_ingest import MetArtDataIngester
    from log.log import IngestLogger
    from clean.met_art_normalize import MetArtDataNormalizer

    load_dotenv()

    db_uri = os.getenv('MYSQL_URI')
    db_raw_table_name = os.getenv('MYSQL_TABLE_NAME')
    db_table_primary_key = os.getenv('MYSQL_TABLE_PKEY')
    ingest_batch_size = int(os.getenv('INGEST_BATCH_SIZE'))
    log_batch_size = int(os.getenv('LOG_BATCH_SIZE'))
    log_file = os.getenv('LOG_FILE_NAME')

    if log_file:
        ingestLogger = IngestLogger(log_file=log_file)
        last_date_ingested = ingestLogger.get_last_date_ingested()
    else:
        ingestLogger = None
        last_date_ingested = ''
    
    ingester = MetArtDataIngester(
        db_uri=db_uri,
        db_table_name=db_raw_table_name,
        db_table_primary_key=db_table_primary_key,
        ingest_batch_size=ingest_batch_size,
        log_batch_size=log_batch_size,
        last_date_ingested=last_date_ingested
    )

    ingester.ingest_objects(logger=ingestLogger)

    normalizer = MetArtDataNormalizer(
        db_uri=db_uri,
        db_raw_table_name=db_raw_table_name
    )

    normalizer.normalize()


if __name__=="__main__":
    main()
