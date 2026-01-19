
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefFinancialClassEPIC AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefFinancialClassEPIC AS source
ON target.FinancialClassKey = source.FinancialClassKey
WHEN MATCHED THEN
  UPDATE SET
  target.FinancialClassKey = source.FinancialClassKey,
 target.FinancialClassCode = TRIM(source.FinancialClassCode),
 target.FinancialClassName = TRIM(source.FinancialClassName),
 target.FinancialClassAbbr = TRIM(source.FinancialClassAbbr),
 target.FinClassC = TRIM(source.FinClassC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.FinancialClassIsPatientBalance = source.FinancialClassIsPatientBalance
WHEN NOT MATCHED THEN
  INSERT (FinancialClassKey, FinancialClassCode, FinancialClassName, FinancialClassAbbr, FinClassC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FinancialClassIsPatientBalance)
  VALUES (source.FinancialClassKey, TRIM(source.FinancialClassCode), TRIM(source.FinancialClassName), TRIM(source.FinancialClassAbbr), TRIM(source.FinClassC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.FinancialClassIsPatientBalance);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT FinancialClassKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefFinancialClassEPIC
      GROUP BY FinancialClassKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefFinancialClassEPIC');
ELSE
  COMMIT TRANSACTION;
END IF;
