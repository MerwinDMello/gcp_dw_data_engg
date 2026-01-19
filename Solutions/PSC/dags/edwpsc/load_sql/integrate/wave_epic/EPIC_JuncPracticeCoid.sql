
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_JuncPracticeCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_JuncPracticeCoid AS source
ON target.JuncPracticeCoid = source.JuncPracticeCoid
WHEN MATCHED THEN
  UPDATE SET
  target.JuncPracticeCoid = source.JuncPracticeCoid,
 target.PracticeKey = source.PracticeKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncPracticeCoid, PracticeKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncPracticeCoid, source.PracticeKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncPracticeCoid
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_JuncPracticeCoid
      GROUP BY JuncPracticeCoid
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_JuncPracticeCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
