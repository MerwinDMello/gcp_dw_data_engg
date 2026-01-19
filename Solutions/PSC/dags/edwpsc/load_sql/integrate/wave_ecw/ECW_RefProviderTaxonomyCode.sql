
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefProviderTaxonomyCode AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefProviderTaxonomyCode AS source
ON target.TaxonomyCodeKey = source.TaxonomyCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.TaxonomyCodeKey = source.TaxonomyCodeKey,
 target.TaxonomyCode = TRIM(source.TaxonomyCode),
 target.SpecialtyDesc = TRIM(source.SpecialtyDesc),
 target.SubSpecialtyDec = TRIM(source.SubSpecialtyDec),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.TaxonomyCodeDesc = TRIM(source.TaxonomyCodeDesc)
WHEN NOT MATCHED THEN
  INSERT (TaxonomyCodeKey, TaxonomyCode, SpecialtyDesc, SubSpecialtyDec, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, TaxonomyCodeDesc)
  VALUES (source.TaxonomyCodeKey, TRIM(source.TaxonomyCode), TRIM(source.SpecialtyDesc), TRIM(source.SubSpecialtyDec), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, TRIM(source.TaxonomyCodeDesc));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TaxonomyCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefProviderTaxonomyCode
      GROUP BY TaxonomyCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefProviderTaxonomyCode');
ELSE
  COMMIT TRANSACTION;
END IF;
