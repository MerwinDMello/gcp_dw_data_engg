
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSHOLDRULESUTILITY_PSHRUNOTES AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSHOLDRULESUTILITY_PSHRUNOTES AS source
ON target.PSHRUKEY = source.PSHRUKEY AND target.NOTE_CNT = source.NOTE_CNT
WHEN MATCHED THEN
  UPDATE SET
  target.NOTE_CNT = source.NOTE_CNT,
 target.NOTE_DATE = source.NOTE_DATE,
 target.NOTE_TIME = source.NOTE_TIME,
 target.NOTE_TYPE = TRIM(source.NOTE_TYPE),
 target.NOTE_USER = TRIM(source.NOTE_USER),
 target.PSHRUKEY = source.PSHRUKEY,
 target.PSHRUNOTES = TRIM(source.PSHRUNOTES),
 target.NoteDateTime = source.NoteDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (NOTE_CNT, NOTE_DATE, NOTE_TIME, NOTE_TYPE, NOTE_USER, PSHRUKEY, PSHRUNOTES, NoteDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.NOTE_CNT, source.NOTE_DATE, source.NOTE_TIME, TRIM(source.NOTE_TYPE), TRIM(source.NOTE_USER), source.PSHRUKEY, TRIM(source.PSHRUNOTES), source.NoteDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PSHRUKEY, NOTE_CNT
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSHOLDRULESUTILITY_PSHRUNOTES
      GROUP BY PSHRUKEY, NOTE_CNT
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSHOLDRULESUTILITY_PSHRUNOTES');
ELSE
  COMMIT TRANSACTION;
END IF;
