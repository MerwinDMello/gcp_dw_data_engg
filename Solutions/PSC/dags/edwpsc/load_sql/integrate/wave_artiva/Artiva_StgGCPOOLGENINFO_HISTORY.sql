
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgGCPOOLGENINFO_HISTORY AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgGCPOOLGENINFO_HISTORY AS source
ON target.SnapShot_Date = source.SnapShot_Date AND target.Pool = source.Pool
WHEN MATCHED THEN
  UPDATE SET
  target.SNAPSHOT_DATE = source.SNAPSHOT_DATE,
 target.POOL = TRIM(source.POOL),
 target.GCPDESC = TRIM(source.GCPDESC),
 target.TOTAL_BUILD_TIME = source.TOTAL_BUILD_TIME,
 target.BEGINNING_BALANCE = source.BEGINNING_BALANCE,
 target.ACCOUNTS_REMAINING = source.ACCOUNTS_REMAINING,
 target.RECORDS_ADDED = source.RECORDS_ADDED,
 target.BUILD_START_DATE = TRIM(source.BUILD_START_DATE),
 target.BUILD_START_TIME = TRIM(source.BUILD_START_TIME),
 target.BUILD_FINISH_DATE = TRIM(source.BUILD_FINISH_DATE),
 target.BUILD_FINISH_TIME = TRIM(source.BUILD_FINISH_TIME),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SNAPSHOT_DATE, POOL, GCPDESC, TOTAL_BUILD_TIME, BEGINNING_BALANCE, ACCOUNTS_REMAINING, RECORDS_ADDED, BUILD_START_DATE, BUILD_START_TIME, BUILD_FINISH_DATE, BUILD_FINISH_TIME, DWLastUpdateDateTime)
  VALUES (source.SNAPSHOT_DATE, TRIM(source.POOL), TRIM(source.GCPDESC), source.TOTAL_BUILD_TIME, source.BEGINNING_BALANCE, source.ACCOUNTS_REMAINING, source.RECORDS_ADDED, TRIM(source.BUILD_START_DATE), TRIM(source.BUILD_START_TIME), TRIM(source.BUILD_FINISH_DATE), TRIM(source.BUILD_FINISH_TIME), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShot_Date, Pool
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgGCPOOLGENINFO_HISTORY
      GROUP BY SnapShot_Date, Pool
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgGCPOOLGENINFO_HISTORY');
ELSE
  COMMIT TRANSACTION;
END IF;
