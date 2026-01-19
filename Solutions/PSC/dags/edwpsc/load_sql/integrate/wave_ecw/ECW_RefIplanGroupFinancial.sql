
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefIplanGroupFinancial AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefIplanGroupFinancial AS source
ON target.IplanGroupFinancialKey = source.IplanGroupFinancialKey
WHEN MATCHED THEN
  UPDATE SET
  target.IplanGroupFinancialKey = source.IplanGroupFinancialKey,
 target.IplanGroupFinancialName = TRIM(source.IplanGroupFinancialName),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (IplanGroupFinancialKey, IplanGroupFinancialName, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, SysStartTime, SysEndTime)
  VALUES (source.IplanGroupFinancialKey, TRIM(source.IplanGroupFinancialName), source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IplanGroupFinancialKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefIplanGroupFinancial
      GROUP BY IplanGroupFinancialKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefIplanGroupFinancial');
ELSE
  COMMIT TRANSACTION;
END IF;
