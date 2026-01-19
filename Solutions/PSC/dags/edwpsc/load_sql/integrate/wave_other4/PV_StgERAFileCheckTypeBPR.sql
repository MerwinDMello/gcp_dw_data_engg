
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileCheckTypeBPR AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileCheckTypeBPR AS source
ON target.bpr_id = source.bpr_id
WHEN MATCHED THEN
  UPDATE SET
  target.bpr_id = source.bpr_id,
 target.fileid = source.fileid,
 target.BPR01 = TRIM(source.BPR01),
 target.BPR02 = TRIM(source.BPR02),
 target.BPR03 = TRIM(source.BPR03),
 target.BPR04 = TRIM(source.BPR04),
 target.BPR05 = TRIM(source.BPR05),
 target.BPR06 = TRIM(source.BPR06),
 target.BPR07 = TRIM(source.BPR07),
 target.BPR08 = TRIM(source.BPR08),
 target.BPR09 = TRIM(source.BPR09),
 target.BPR10 = TRIM(source.BPR10),
 target.BPR11 = TRIM(source.BPR11),
 target.BPR12 = TRIM(source.BPR12),
 target.BPR13 = TRIM(source.BPR13),
 target.BPR14 = TRIM(source.BPR14),
 target.BPR15 = TRIM(source.BPR15),
 target.BPR16 = TRIM(source.BPR16),
 target.BPRSegment = TRIM(source.BPRSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (bpr_id, fileid, BPR01, BPR02, BPR03, BPR04, BPR05, BPR06, BPR07, BPR08, BPR09, BPR10, BPR11, BPR12, BPR13, BPR14, BPR15, BPR16, BPRSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.bpr_id, source.fileid, TRIM(source.BPR01), TRIM(source.BPR02), TRIM(source.BPR03), TRIM(source.BPR04), TRIM(source.BPR05), TRIM(source.BPR06), TRIM(source.BPR07), TRIM(source.BPR08), TRIM(source.BPR09), TRIM(source.BPR10), TRIM(source.BPR11), TRIM(source.BPR12), TRIM(source.BPR13), TRIM(source.BPR14), TRIM(source.BPR15), TRIM(source.BPR16), TRIM(source.BPRSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT bpr_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileCheckTypeBPR
      GROUP BY bpr_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileCheckTypeBPR');
ELSE
  COMMIT TRANSACTION;
END IF;
