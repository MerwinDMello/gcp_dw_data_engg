
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineRARC AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileClaimlineRARC AS source
ON target.RARC_id = source.RARC_id
WHEN MATCHED THEN
  UPDATE SET
  target.RARC_id = source.RARC_id,
 target.claimline_id = source.claimline_id,
 target.claim_id = source.claim_id,
 target.Payer_id = source.Payer_id,
 target.fileid = source.fileid,
 target.bpr_id = source.bpr_id,
 target.TRN_id = source.TRN_id,
 target.RARC01 = TRIM(source.RARC01),
 target.RARC02_Code = TRIM(source.RARC02_Code),
 target.RARCSegment = TRIM(source.RARCSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (RARC_id, claimline_id, claim_id, Payer_id, fileid, bpr_id, TRN_id, RARC01, RARC02_Code, RARCSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.RARC_id, source.claimline_id, source.claim_id, source.Payer_id, source.fileid, source.bpr_id, source.TRN_id, TRIM(source.RARC01), TRIM(source.RARC02_Code), TRIM(source.RARCSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT RARC_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineRARC
      GROUP BY RARC_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineRARC');
ELSE
  COMMIT TRANSACTION;
END IF;
