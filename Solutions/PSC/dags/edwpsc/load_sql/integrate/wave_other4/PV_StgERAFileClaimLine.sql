
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimLine AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileClaimLine AS source
ON target.claimline_id = source.claimline_id
WHEN MATCHED THEN
  UPDATE SET
  target.claimline_id = source.claimline_id,
 target.claim_id = source.claim_id,
 target.Payer_id = source.Payer_id,
 target.fileid = source.fileid,
 target.bpr_id = source.bpr_id,
 target.TRN_id = source.TRN_id,
 target.ClaimLineCPT = TRIM(source.ClaimLineCPT),
 target.ClaimLineMod1 = TRIM(source.ClaimLineMod1),
 target.ClaimLineMod2 = TRIM(source.ClaimLineMod2),
 target.ClaimLineMod3 = TRIM(source.ClaimLineMod3),
 target.ClaimLineMod4 = TRIM(source.ClaimLineMod4),
 target.ClaimLine02 = TRIM(source.ClaimLine02),
 target.ClaimLine03 = TRIM(source.ClaimLine03),
 target.ClaimLine04 = TRIM(source.ClaimLine04),
 target.ClaimLine05 = TRIM(source.ClaimLine05),
 target.ClaimLine06 = TRIM(source.ClaimLine06),
 target.ClaimLine07 = TRIM(source.ClaimLine07),
 target.ClaimLineSegment = TRIM(source.ClaimLineSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (claimline_id, claim_id, Payer_id, fileid, bpr_id, TRN_id, ClaimLineCPT, ClaimLineMod1, ClaimLineMod2, ClaimLineMod3, ClaimLineMod4, ClaimLine02, ClaimLine03, ClaimLine04, ClaimLine05, ClaimLine06, ClaimLine07, ClaimLineSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.claimline_id, source.claim_id, source.Payer_id, source.fileid, source.bpr_id, source.TRN_id, TRIM(source.ClaimLineCPT), TRIM(source.ClaimLineMod1), TRIM(source.ClaimLineMod2), TRIM(source.ClaimLineMod3), TRIM(source.ClaimLineMod4), TRIM(source.ClaimLine02), TRIM(source.ClaimLine03), TRIM(source.ClaimLine04), TRIM(source.ClaimLine05), TRIM(source.ClaimLine06), TRIM(source.ClaimLine07), TRIM(source.ClaimLineSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT claimline_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimLine
      GROUP BY claimline_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimLine');
ELSE
  COMMIT TRANSACTION;
END IF;
