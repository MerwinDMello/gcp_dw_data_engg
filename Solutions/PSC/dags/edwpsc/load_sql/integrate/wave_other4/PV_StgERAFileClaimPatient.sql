
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimPatient AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileClaimPatient AS source
ON target.claim_patient_id = source.claim_patient_id
WHEN MATCHED THEN
  UPDATE SET
  target.claim_patient_id = source.claim_patient_id,
 target.claim_id = source.claim_id,
 target.Payer_id = source.Payer_id,
 target.fileid = source.fileid,
 target.bpr_id = source.bpr_id,
 target.TRN_id = source.TRN_id,
 target.ClaimPatient01 = TRIM(source.ClaimPatient01),
 target.ClaimPatient02 = TRIM(source.ClaimPatient02),
 target.ClaimPatient03 = TRIM(source.ClaimPatient03),
 target.ClaimPatient04 = TRIM(source.ClaimPatient04),
 target.ClaimPatient05 = TRIM(source.ClaimPatient05),
 target.ClaimPatient06 = TRIM(source.ClaimPatient06),
 target.ClaimPatient07 = TRIM(source.ClaimPatient07),
 target.ClaimPatient08 = TRIM(source.ClaimPatient08),
 target.ClaimPatient09 = TRIM(source.ClaimPatient09),
 target.ClaimPatientSegment = TRIM(source.ClaimPatientSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.GS_id = source.GS_id,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (claim_patient_id, claim_id, Payer_id, fileid, bpr_id, TRN_id, ClaimPatient01, ClaimPatient02, ClaimPatient03, ClaimPatient04, ClaimPatient05, ClaimPatient06, ClaimPatient07, ClaimPatient08, ClaimPatient09, ClaimPatientSegment, InsertedDTM, GS_id, DWLastUpdateDateTime)
  VALUES (source.claim_patient_id, source.claim_id, source.Payer_id, source.fileid, source.bpr_id, source.TRN_id, TRIM(source.ClaimPatient01), TRIM(source.ClaimPatient02), TRIM(source.ClaimPatient03), TRIM(source.ClaimPatient04), TRIM(source.ClaimPatient05), TRIM(source.ClaimPatient06), TRIM(source.ClaimPatient07), TRIM(source.ClaimPatient08), TRIM(source.ClaimPatient09), TRIM(source.ClaimPatientSegment), source.InsertedDTM, source.GS_id, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT claim_patient_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimPatient
      GROUP BY claim_patient_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileClaimPatient');
ELSE
  COMMIT TRANSACTION;
END IF;
