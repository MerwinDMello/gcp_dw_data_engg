
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefMarket AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefMarket AS source
ON target.MarketKey = source.MarketKey
WHEN MATCHED THEN
  UPDATE SET
  target.MarketKey = source.MarketKey,
 target.MarketName = TRIM(source.MarketName),
 target.DivisionKey = source.DivisionKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.MarketCode = TRIM(source.MarketCode),
 target.DeleteFlag = source.DeleteFlag,
 target.coidstatflag = source.coidstatflag,
 target.PPMSFlag = source.PPMSFlag,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (MarketKey, MarketName, DivisionKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, MarketCode, DeleteFlag, coidstatflag, PPMSFlag, SysStartTime, SysEndTime)
  VALUES (source.MarketKey, TRIM(source.MarketName), source.DivisionKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.MarketCode), source.DeleteFlag, source.coidstatflag, source.PPMSFlag, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MarketKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefMarket
      GROUP BY MarketKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefMarket');
ELSE
  COMMIT TRANSACTION;
END IF;
