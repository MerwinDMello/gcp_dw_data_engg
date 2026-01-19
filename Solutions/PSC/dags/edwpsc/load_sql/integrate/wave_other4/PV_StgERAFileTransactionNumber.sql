
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileTransactionNumber AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileTransactionNumber AS source
ON target.TRN_id = source.TRN_id
WHEN MATCHED THEN
  UPDATE SET
  target.TRN_id = source.TRN_id,
 target.bpr_id = source.bpr_id,
 target.fileid = source.fileid,
 target.TRN01 = TRIM(source.TRN01),
 target.TRN02 = TRIM(source.TRN02),
 target.TRN03 = TRIM(source.TRN03),
 target.TRN04 = TRIM(source.TRN04),
 target.TRN05 = TRIM(source.TRN05),
 target.TRN06 = TRIM(source.TRN06),
 target.TRN07 = TRIM(source.TRN07),
 target.TRN08 = TRIM(source.TRN08),
 target.TRN09 = TRIM(source.TRN09),
 target.TRN10 = TRIM(source.TRN10),
 target.TRNSegment = TRIM(source.TRNSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (TRN_id, bpr_id, fileid, TRN01, TRN02, TRN03, TRN04, TRN05, TRN06, TRN07, TRN08, TRN09, TRN10, TRNSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.TRN_id, source.bpr_id, source.fileid, TRIM(source.TRN01), TRIM(source.TRN02), TRIM(source.TRN03), TRIM(source.TRN04), TRIM(source.TRN05), TRIM(source.TRN06), TRIM(source.TRN07), TRIM(source.TRN08), TRIM(source.TRN09), TRIM(source.TRN10), TRIM(source.TRNSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TRN_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileTransactionNumber
      GROUP BY TRN_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileTransactionNumber');
ELSE
  COMMIT TRANSACTION;
END IF;
