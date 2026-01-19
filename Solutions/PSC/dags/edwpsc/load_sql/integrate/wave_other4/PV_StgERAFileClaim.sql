
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaim AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileClaim AS source
ON target.claim_id = source.claim_id
WHEN MATCHED THEN
  UPDATE SET
  target.claim_id = source.claim_id,
 target.Payer_id = source.Payer_id,
 target.fileid = source.fileid,
 target.bpr_id = source.bpr_id,
 target.TRN_id = source.TRN_id,
 target.Claim01 = TRIM(source.Claim01),
 target.Claim02 = TRIM(source.Claim02),
 target.Claim03 = TRIM(source.Claim03),
 target.Claim04 = TRIM(source.Claim04),
 target.Claim05 = TRIM(source.Claim05),
 target.Claim06 = TRIM(source.Claim06),
 target.Claim07 = TRIM(source.Claim07),
 target.Claim08 = TRIM(source.Claim08),
 target.Claim09 = TRIM(source.Claim09),
 target.Claim10 = TRIM(source.Claim10),
 target.Claim11 = TRIM(source.Claim11),
 target.Claim12 = TRIM(source.Claim12),
 target.Claim13 = TRIM(source.Claim13),
 target.Claim14 = TRIM(source.Claim14),
 target.ClaimSegment = TRIM(source.ClaimSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (claim_id, Payer_id, fileid, bpr_id, TRN_id, Claim01, Claim02, Claim03, Claim04, Claim05, Claim06, Claim07, Claim08, Claim09, Claim10, Claim11, Claim12, Claim13, Claim14, ClaimSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.claim_id, source.Payer_id, source.fileid, source.bpr_id, source.TRN_id, TRIM(source.Claim01), TRIM(source.Claim02), TRIM(source.Claim03), TRIM(source.Claim04), TRIM(source.Claim05), TRIM(source.Claim06), TRIM(source.Claim07), TRIM(source.Claim08), TRIM(source.Claim09), TRIM(source.Claim10), TRIM(source.Claim11), TRIM(source.Claim12), TRIM(source.Claim13), TRIM(source.Claim14), TRIM(source.ClaimSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT claim_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaim
      GROUP BY claim_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaim');
ELSE
  COMMIT TRANSACTION;
END IF;
