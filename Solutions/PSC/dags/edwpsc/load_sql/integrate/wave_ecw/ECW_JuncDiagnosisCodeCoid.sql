
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncDiagnosisCodeCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncDiagnosisCodeCoid AS source
ON target.JuncDiagnosisCodeCoidKey = source.JuncDiagnosisCodeCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncDiagnosisCodeCoidKey = source.JuncDiagnosisCodeCoidKey,
 target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncDiagnosisCodeCoidKey, DiagnosisCodeKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncDiagnosisCodeCoidKey, source.DiagnosisCodeKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncDiagnosisCodeCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncDiagnosisCodeCoid
      GROUP BY JuncDiagnosisCodeCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncDiagnosisCodeCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
