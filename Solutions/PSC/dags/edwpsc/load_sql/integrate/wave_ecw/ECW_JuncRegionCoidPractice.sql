
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidPractice AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncRegionCoidPractice AS source
ON target.JuncRegionCoidPracticeKey = source.JuncRegionCoidPracticeKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncRegionCoidPracticeKey = source.JuncRegionCoidPracticeKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.PracticeKey = source.PracticeKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncRegionCoidPracticeKey, RegionKey, Coid, PracticeKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncRegionCoidPracticeKey, source.RegionKey, TRIM(source.Coid), source.PracticeKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncRegionCoidPracticeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidPractice
      GROUP BY JuncRegionCoidPracticeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidPractice');
ELSE
  COMMIT TRANSACTION;
END IF;
