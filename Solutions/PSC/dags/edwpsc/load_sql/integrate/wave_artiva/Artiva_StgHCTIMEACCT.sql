
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgHCTIMEACCT AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgHCTIMEACCT AS source
ON target.HCTIMEKEY = source.HCTIMEKEY
WHEN MATCHED THEN
  UPDATE SET
  target.HCTIMEKEY = TRIM(source.HCTIMEKEY),
 target.HCTIMEID = TRIM(source.HCTIMEID),
 target.HCTIMEENCNTRID = TRIM(source.HCTIMEENCNTRID),
 target.HCTIMEUSER = TRIM(source.HCTIMEUSER),
 target.HCTIMEPYR = TRIM(source.HCTIMEPYR),
 target.HCTIMEDATE = TRIM(source.HCTIMEDATE),
 target.HCTIMETIME = source.HCTIMETIME,
 target.HCTIMESTIME = source.HCTIMESTIME,
 target.HCTIMEETIME = source.HCTIMEETIME,
 target.HCTIMETTIME = TRIM(source.HCTIMETTIME),
 target.UserID = TRIM(source.UserID),
 target.DWLastUpdateDatetime = source.DWLastUpdateDatetime
WHEN NOT MATCHED THEN
  INSERT (HCTIMEKEY, HCTIMEID, HCTIMEENCNTRID, HCTIMEUSER, HCTIMEPYR, HCTIMEDATE, HCTIMETIME, HCTIMESTIME, HCTIMEETIME, HCTIMETTIME, UserID, DWLastUpdateDatetime)
  VALUES (TRIM(source.HCTIMEKEY), TRIM(source.HCTIMEID), TRIM(source.HCTIMEENCNTRID), TRIM(source.HCTIMEUSER), TRIM(source.HCTIMEPYR), TRIM(source.HCTIMEDATE), source.HCTIMETIME, source.HCTIMESTIME, source.HCTIMEETIME, TRIM(source.HCTIMETTIME), TRIM(source.UserID), source.DWLastUpdateDatetime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT HCTIMEKEY
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgHCTIMEACCT
      GROUP BY HCTIMEKEY
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgHCTIMEACCT');
ELSE
  COMMIT TRANSACTION;
END IF;
