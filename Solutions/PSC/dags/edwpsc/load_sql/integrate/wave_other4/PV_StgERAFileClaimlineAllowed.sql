
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineAllowed AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileClaimlineAllowed AS source
ON target.Allowed_id = source.Allowed_id
WHEN MATCHED THEN
  UPDATE SET
  target.Allowed_id = source.Allowed_id,
 target.claimline_id = source.claimline_id,
 target.claim_id = source.claim_id,
 target.Payer_id = source.Payer_id,
 target.fileid = source.fileid,
 target.bpr_id = source.bpr_id,
 target.TRN_id = source.TRN_id,
 target.Allowed01 = TRIM(source.Allowed01),
 target.AllowedSegment = TRIM(source.AllowedSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (Allowed_id, claimline_id, claim_id, Payer_id, fileid, bpr_id, TRN_id, Allowed01, AllowedSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.Allowed_id, source.claimline_id, source.claim_id, source.Payer_id, source.fileid, source.bpr_id, source.TRN_id, TRIM(source.Allowed01), TRIM(source.AllowedSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT Allowed_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineAllowed
      GROUP BY Allowed_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineAllowed');
ELSE
  COMMIT TRANSACTION;
END IF;
