
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCLINKHIST AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSOCLINKHIST AS source
ON target.PSOCLHKEY = source.PSOCLHKEY
WHEN MATCHED THEN
  UPDATE SET
  target.PSOCLHKEY = source.PSOCLHKEY,
 target.PSOCLHACTION = TRIM(source.PSOCLHACTION),
 target.PSOCLHDATE = source.PSOCLHDATE,
 target.PSOCLHDESC = TRIM(source.PSOCLHDESC),
 target.PSOCLHLEAD = TRIM(source.PSOCLHLEAD),
 target.PSOCLHLINKID = source.PSOCLHLINKID,
 target.PSOCLHMSGID = source.PSOCLHMSGID,
 target.PSOCLHTIME = source.PSOCLHTIME,
 target.PSOCLHUSER = TRIM(source.PSOCLHUSER),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PSOCLHKEY, PSOCLHACTION, PSOCLHDATE, PSOCLHDESC, PSOCLHLEAD, PSOCLHLINKID, PSOCLHMSGID, PSOCLHTIME, PSOCLHUSER, DWLastUpdateDateTime)
  VALUES (source.PSOCLHKEY, TRIM(source.PSOCLHACTION), source.PSOCLHDATE, TRIM(source.PSOCLHDESC), TRIM(source.PSOCLHLEAD), source.PSOCLHLINKID, source.PSOCLHMSGID, source.PSOCLHTIME, TRIM(source.PSOCLHUSER), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PSOCLHKEY
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCLINKHIST
      GROUP BY PSOCLHKEY
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCLINKHIST');
ELSE
  COMMIT TRANSACTION;
END IF;
