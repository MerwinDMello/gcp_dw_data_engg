
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_JuncGroupCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_JuncGroupCoid AS source
ON target.JuncGroupCoidKey = source.JuncGroupCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncGroupCoidKey = source.JuncGroupCoidKey,
 target.GroupKey = source.GroupKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncGroupCoidKey, GroupKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncGroupCoidKey, source.GroupKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncGroupCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_JuncGroupCoid
      GROUP BY JuncGroupCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_JuncGroupCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
