
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_JuncPatientCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_JuncPatientCoid AS source
ON target.JuncPatientCoidKey = source.JuncPatientCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncPatientCoidKey = source.JuncPatientCoidKey,
 target.PatientKey = source.PatientKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncPatientCoidKey, PatientKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncPatientCoidKey, source.PatientKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncPatientCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_JuncPatientCoid
      GROUP BY JuncPatientCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_JuncPatientCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
