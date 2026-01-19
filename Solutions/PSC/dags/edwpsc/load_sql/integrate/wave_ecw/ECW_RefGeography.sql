
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefGeography AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefGeography AS source
ON target.GeographyKey = source.GeographyKey
WHEN MATCHED THEN
  UPDATE SET
  target.GeographyKey = source.GeographyKey,
 target.GeographyCity = TRIM(source.GeographyCity),
 target.StateKey = TRIM(source.StateKey),
 target.GeographyZipcode = TRIM(source.GeographyZipcode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (GeographyKey, GeographyCity, StateKey, GeographyZipcode, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.GeographyKey, TRIM(source.GeographyCity), TRIM(source.StateKey), TRIM(source.GeographyZipcode), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GeographyKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefGeography
      GROUP BY GeographyKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefGeography');
ELSE
  COMMIT TRANSACTION;
END IF;
