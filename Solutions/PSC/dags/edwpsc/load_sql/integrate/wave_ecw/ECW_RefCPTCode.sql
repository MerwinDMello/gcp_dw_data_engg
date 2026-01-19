
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCPTCode AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefCPTCode AS source
ON target.CPTCodeKey = source.CPTCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTName = TRIM(source.CPTName),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.Category1Code = TRIM(source.Category1Code),
 target.Category2Code = TRIM(source.Category2Code),
 target.CptTier1Description = TRIM(source.CptTier1Description),
 target.CptTier2Description = TRIM(source.CptTier2Description),
 target.CptTier3Description = TRIM(source.CptTier3Description),
 target.CptTier4Description = TRIM(source.CptTier4Description),
 target.CptTier5Description = TRIM(source.CptTier5Description)
WHEN NOT MATCHED THEN
  INSERT (CPTCodeKey, CPTCode, CPTName, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, Category1Code, Category2Code, CptTier1Description, CptTier2Description, CptTier3Description, CptTier4Description, CptTier5Description)
  VALUES (source.CPTCodeKey, TRIM(source.CPTCode), TRIM(source.CPTName), source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, TRIM(source.Category1Code), TRIM(source.Category2Code), TRIM(source.CptTier1Description), TRIM(source.CptTier2Description), TRIM(source.CptTier3Description), TRIM(source.CptTier4Description), TRIM(source.CptTier5Description));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CPTCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefCPTCode
      GROUP BY CPTCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefCPTCode');
ELSE
  COMMIT TRANSACTION;
END IF;
