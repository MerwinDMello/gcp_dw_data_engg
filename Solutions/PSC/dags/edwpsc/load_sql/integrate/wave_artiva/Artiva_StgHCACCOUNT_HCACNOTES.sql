
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgHCACCOUNT_HCACNOTES AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgHCACCOUNT_HCACNOTES AS source
ON target.HCACID = source.HCACID AND target.NOTE_CNT = source.NOTE_CNT
WHEN MATCHED THEN
  UPDATE SET
  target.HCACID = source.HCACID,
 target.HCACNOTES = TRIM(source.HCACNOTES),
 target.NOTE_CNT = source.NOTE_CNT,
 target.NOTE_DATE = source.NOTE_DATE,
 target.NOTE_TIME = source.NOTE_TIME,
 target.NoteDateTime = source.NoteDateTime,
 target.NOTE_TYPE = TRIM(source.NOTE_TYPE),
 target.NOTE_USER = TRIM(source.NOTE_USER),
 target.HCPatientAccountingNumber = TRIM(source.HCPatientAccountingNumber),
 target.ECWClaimKey = source.ECWClaimKey,
 target.ECWClaimNumber = source.ECWClaimNumber,
 target.ECWRegionKey = source.ECWRegionKey,
 target.PVClaimKey = source.PVClaimKey,
 target.PVClaimNumber = source.PVClaimNumber,
 target.PVRegionKey = source.PVRegionKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (HCACID, HCACNOTES, NOTE_CNT, NOTE_DATE, NOTE_TIME, NoteDateTime, NOTE_TYPE, NOTE_USER, HCPatientAccountingNumber, ECWClaimKey, ECWClaimNumber, ECWRegionKey, PVClaimKey, PVClaimNumber, PVRegionKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.HCACID, TRIM(source.HCACNOTES), source.NOTE_CNT, source.NOTE_DATE, source.NOTE_TIME, source.NoteDateTime, TRIM(source.NOTE_TYPE), TRIM(source.NOTE_USER), TRIM(source.HCPatientAccountingNumber), source.ECWClaimKey, source.ECWClaimNumber, source.ECWRegionKey, source.PVClaimKey, source.PVClaimNumber, source.PVRegionKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT HCACID, NOTE_CNT
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgHCACCOUNT_HCACNOTES
      GROUP BY HCACID, NOTE_CNT
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgHCACCOUNT_HCACNOTES');
ELSE
  COMMIT TRANSACTION;
END IF;
