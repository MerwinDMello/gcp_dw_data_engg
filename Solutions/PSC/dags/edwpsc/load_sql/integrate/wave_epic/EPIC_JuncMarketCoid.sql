
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_JuncMarketCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_JuncMarketCoid AS source
ON target.JuncMarketCoidKey = source.JuncMarketCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncMarketCoidKey = source.JuncMarketCoidKey,
 target.MarketKey = source.MarketKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncMarketCoidKey, MarketKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncMarketCoidKey, source.MarketKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncMarketCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_JuncMarketCoid
      GROUP BY JuncMarketCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_JuncMarketCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
