
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPPI_PSPEPPINOTES AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPPI_PSPEPPINOTES AS source
ON target.PSPEPPIKEY = source.PSPEPPIKEY AND target.NOTE_CNT = source.NOTE_CNT
WHEN MATCHED THEN
  UPDATE SET
  target.NOTE_CNT = source.NOTE_CNT,
 target.NOTE_DATE = source.NOTE_DATE,
 target.NOTE_TIME = source.NOTE_TIME,
 target.NOTE_TYPE = TRIM(source.NOTE_TYPE),
 target.NOTE_USER = TRIM(source.NOTE_USER),
 target.PSPEPPIKEY = TRIM(source.PSPEPPIKEY),
 target.PSPEPPINOTES = TRIM(source.PSPEPPINOTES),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (NOTE_CNT, NOTE_DATE, NOTE_TIME, NOTE_TYPE, NOTE_USER, PSPEPPIKEY, PSPEPPINOTES, DWLastUpdateDateTime)
  VALUES (source.NOTE_CNT, source.NOTE_DATE, source.NOTE_TIME, TRIM(source.NOTE_TYPE), TRIM(source.NOTE_USER), TRIM(source.PSPEPPIKEY), TRIM(source.PSPEPPINOTES), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PSPEPPIKEY, NOTE_CNT
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPPI_PSPEPPINOTES
      GROUP BY PSPEPPIKEY, NOTE_CNT
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPPI_PSPEPPINOTES');
ELSE
  COMMIT TRANSACTION;
END IF;
