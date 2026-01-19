
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineCARC AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileClaimlineCARC AS source
ON target.CARC_id = source.CARC_id
WHEN MATCHED THEN
  UPDATE SET
  target.CARC_id = source.CARC_id,
 target.claimline_id = source.claimline_id,
 target.claim_id = source.claim_id,
 target.Payer_id = source.Payer_id,
 target.fileid = source.fileid,
 target.bpr_id = source.bpr_id,
 target.TRN_id = source.TRN_id,
 target.CARC01_Category = TRIM(source.CARC01_Category),
 target.CARC02_Code = TRIM(source.CARC02_Code),
 target.CARC03_Amount = TRIM(source.CARC03_Amount),
 target.CARC04 = TRIM(source.CARC04),
 target.CARC05 = TRIM(source.CARC05),
 target.CARC06 = TRIM(source.CARC06),
 target.CARC07 = TRIM(source.CARC07),
 target.CARC08 = TRIM(source.CARC08),
 target.CARC09 = TRIM(source.CARC09),
 target.CARC10 = TRIM(source.CARC10),
 target.CARC11 = TRIM(source.CARC11),
 target.CARC12 = TRIM(source.CARC12),
 target.CARC13 = TRIM(source.CARC13),
 target.CARC14 = TRIM(source.CARC14),
 target.CARC15 = TRIM(source.CARC15),
 target.CARC16 = TRIM(source.CARC16),
 target.CARC17 = TRIM(source.CARC17),
 target.CARC18 = TRIM(source.CARC18),
 target.CARC19 = TRIM(source.CARC19),
 target.CARCSegment = TRIM(source.CARCSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (CARC_id, claimline_id, claim_id, Payer_id, fileid, bpr_id, TRN_id, CARC01_Category, CARC02_Code, CARC03_Amount, CARC04, CARC05, CARC06, CARC07, CARC08, CARC09, CARC10, CARC11, CARC12, CARC13, CARC14, CARC15, CARC16, CARC17, CARC18, CARC19, CARCSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.CARC_id, source.claimline_id, source.claim_id, source.Payer_id, source.fileid, source.bpr_id, source.TRN_id, TRIM(source.CARC01_Category), TRIM(source.CARC02_Code), TRIM(source.CARC03_Amount), TRIM(source.CARC04), TRIM(source.CARC05), TRIM(source.CARC06), TRIM(source.CARC07), TRIM(source.CARC08), TRIM(source.CARC09), TRIM(source.CARC10), TRIM(source.CARC11), TRIM(source.CARC12), TRIM(source.CARC13), TRIM(source.CARC14), TRIM(source.CARC15), TRIM(source.CARC16), TRIM(source.CARC17), TRIM(source.CARC18), TRIM(source.CARC19), TRIM(source.CARCSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CARC_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineCARC
      GROUP BY CARC_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimlineCARC');
ELSE
  COMMIT TRANSACTION;
END IF;
