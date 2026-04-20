from common.snowflake_utils import execute_sql


def insert_run_log(
    run_id: str,
    source_name: str,
    run_type: str,
    topic_name: str,
    run_started_at: str,
    run_finished_at: str,
    status: str,
    records_fetched_from_api: int,
    records_published_to_kafka: int,
    pages_read: int,
    watermark_before: str | None,
    watermark_after: str | None,
    error_message: str | None,
):
    sql = """
    INSERT INTO INGESTION_RUN_LOG (
        RUN_ID, SOURCE_NAME, RUN_TYPE, TOPIC_NAME,
        RUN_STARTED_AT, RUN_FINISHED_AT, STATUS,
        RECORDS_FETCHED_FROM_API, RECORDS_PUBLISHED_TO_KAFKA,
        PAGES_READ, WATERMARK_BEFORE, WATERMARK_AFTER, ERROR_MESSAGE
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    execute_sql(sql, (
        run_id, source_name, run_type, topic_name,
        run_started_at, run_finished_at, status,
        records_fetched_from_api, records_published_to_kafka,
        pages_read, watermark_before, watermark_after, error_message
    ))


def get_last_watermark(source_name: str) -> str | None:
    sql = """
    SELECT LAST_WATERMARK_VALUE
    FROM INGESTION_SOURCE_STATE
    WHERE SOURCE_NAME = %s
    """
    cur = execute_sql(sql, (source_name,))
    row = cur.fetchone()
    return row[0] if row else None


def upsert_source_state(
    source_name: str,
    last_successful_refresh_at: str | None,
    last_watermark_value: str | None,
    last_run_status: str,
    last_known_total_records: int | None,
):
    sql = """
    MERGE INTO INGESTION_SOURCE_STATE t
    USING (
        SELECT %s AS SOURCE_NAME,
               %s AS LAST_SUCCESSFUL_REFRESH_AT,
               %s AS LAST_WATERMARK_VALUE,
               %s AS LAST_RUN_STATUS,
               %s AS LAST_KNOWN_TOTAL_RECORDS
    ) s
    ON t.SOURCE_NAME = s.SOURCE_NAME
    WHEN MATCHED THEN UPDATE SET
        LAST_SUCCESSFUL_REFRESH_AT = s.LAST_SUCCESSFUL_REFRESH_AT,
        LAST_WATERMARK_VALUE = s.LAST_WATERMARK_VALUE,
        LAST_RUN_STATUS = s.LAST_RUN_STATUS,
        LAST_KNOWN_TOTAL_RECORDS = s.LAST_KNOWN_TOTAL_RECORDS
    WHEN NOT MATCHED THEN INSERT (
        SOURCE_NAME, LAST_SUCCESSFUL_REFRESH_AT, LAST_WATERMARK_VALUE,
        LAST_RUN_STATUS, LAST_KNOWN_TOTAL_RECORDS
    )
    VALUES (
        s.SOURCE_NAME, s.LAST_SUCCESSFUL_REFRESH_AT, s.LAST_WATERMARK_VALUE,
        s.LAST_RUN_STATUS, s.LAST_KNOWN_TOTAL_RECORDS
    )
    """
    execute_sql(sql, (
        source_name,
        last_successful_refresh_at,
        last_watermark_value,
        last_run_status,
        last_known_total_records,
    ))