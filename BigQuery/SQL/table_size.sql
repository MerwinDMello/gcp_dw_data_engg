SELECT
  table_id, row_count, 
  DATETIME(TIMESTAMP_MILLIS(last_modified_time),"US/Central") as Modified_Date, 
  IF((size_bytes / POW(10,9)) < 0.01, 0, ROUND(size_bytes / POW(10,9), 2)) AS size_gb
FROM
  edwim.__TABLES__
  WHERE UPPER(TRIM(table_id)) NOT IN ('HIN_SECREF_FACILITY', 'PROVIDER_PRIVILEGE') 
  AND NOT ENDS_WITH(UPPER(TRIM(table_id)),'_BKP') AND NOT ENDS_WITH(UPPER(TRIM(table_id)),'_TEST');