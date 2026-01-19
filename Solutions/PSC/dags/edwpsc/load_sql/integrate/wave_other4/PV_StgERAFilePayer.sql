
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFilePayer AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFilePayer AS source
ON target.Payer_id = source.Payer_id
WHEN MATCHED THEN
  UPDATE SET
  target.Payer_id = source.Payer_id,
 target.fileid = source.fileid,
 target.bpr_id = source.bpr_id,
 target.TRN_id = source.TRN_id,
 target.Payer01 = TRIM(source.Payer01),
 target.Payer02 = TRIM(source.Payer02),
 target.Payer03 = TRIM(source.Payer03),
 target.Payer04 = TRIM(source.Payer04),
 target.Payer05 = TRIM(source.Payer05),
 target.Payer06 = TRIM(source.Payer06),
 target.Payer07 = TRIM(source.Payer07),
 target.Payer08 = TRIM(source.Payer08),
 target.Payer09 = TRIM(source.Payer09),
 target.Payer10 = TRIM(source.Payer10),
 target.PayerSegment = TRIM(source.PayerSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (Payer_id, fileid, bpr_id, TRN_id, Payer01, Payer02, Payer03, Payer04, Payer05, Payer06, Payer07, Payer08, Payer09, Payer10, PayerSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.Payer_id, source.fileid, source.bpr_id, source.TRN_id, TRIM(source.Payer01), TRIM(source.Payer02), TRIM(source.Payer03), TRIM(source.Payer04), TRIM(source.Payer05), TRIM(source.Payer06), TRIM(source.Payer07), TRIM(source.Payer08), TRIM(source.Payer09), TRIM(source.Payer10), TRIM(source.PayerSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT Payer_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFilePayer
      GROUP BY Payer_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFilePayer');
ELSE
  COMMIT TRANSACTION;
END IF;
