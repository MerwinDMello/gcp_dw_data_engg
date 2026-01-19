
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefFinancialClass AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefFinancialClass AS source
ON target.FinancialClassKey = source.FinancialClassKey
WHEN MATCHED THEN
  UPDATE SET
  target.FinancialClassKey = source.FinancialClassKey,
 target.FinancialClassName = TRIM(source.FinancialClassName),
 target.FinancialClassIsPatientBalance = source.FinancialClassIsPatientBalance,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (FinancialClassKey, FinancialClassName, FinancialClassIsPatientBalance, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.FinancialClassKey, TRIM(source.FinancialClassName), source.FinancialClassIsPatientBalance, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT FinancialClassKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefFinancialClass
      GROUP BY FinancialClassKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefFinancialClass');
ELSE
  COMMIT TRANSACTION;
END IF;
