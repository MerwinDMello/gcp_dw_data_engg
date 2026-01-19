
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactMIPS AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactMIPS AS source
ON target.PKMipsKey = source.PKMipsKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKMipsKey = source.PKMipsKey,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.Measure = TRIM(source.Measure),
 target.MeasureNumber = source.MeasureNumber,
 target.MeasureDescription = TRIM(source.MeasureDescription),
 target.MeasureTitle = TRIM(source.MeasureTitle),
 target.ResponseSubmittedFlag = source.ResponseSubmittedFlag,
 target.ResponseDeletedFlag = source.ResponseDeletedFlag,
 target.ResponseIsBillingProviderFlag = source.ResponseIsBillingProviderFlag,
 target.CreatedDateTime = source.CreatedDateTime,
 target.CreatedByUserName = TRIM(source.CreatedByUserName),
 target.CreatedByUserLastName = TRIM(source.CreatedByUserLastName),
 target.CreatedByUserFirstName = TRIM(source.CreatedByUserFirstName),
 target.ModifiedDateTime = source.ModifiedDateTime,
 target.ModifiedByUserName = TRIM(source.ModifiedByUserName),
 target.ModifiedByUserLastName = TRIM(source.ModifiedByUserLastName),
 target.ModifiedByUserFirstName = TRIM(source.ModifiedByUserFirstName),
 target.ChargeTransactionId = source.ChargeTransactionId,
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.RoleName = TRIM(source.RoleName),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKMipsKey, PKRegionName, Measure, MeasureNumber, MeasureDescription, MeasureTitle, ResponseSubmittedFlag, ResponseDeletedFlag, ResponseIsBillingProviderFlag, CreatedDateTime, CreatedByUserName, CreatedByUserLastName, CreatedByUserFirstName, ModifiedDateTime, ModifiedByUserName, ModifiedByUserLastName, ModifiedByUserFirstName, ChargeTransactionId, PKFinancialNumber, PatientLastName, PatientFirstName, RoleName, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PKMipsKey, TRIM(source.PKRegionName), TRIM(source.Measure), source.MeasureNumber, TRIM(source.MeasureDescription), TRIM(source.MeasureTitle), source.ResponseSubmittedFlag, source.ResponseDeletedFlag, source.ResponseIsBillingProviderFlag, source.CreatedDateTime, TRIM(source.CreatedByUserName), TRIM(source.CreatedByUserLastName), TRIM(source.CreatedByUserFirstName), source.ModifiedDateTime, TRIM(source.ModifiedByUserName), TRIM(source.ModifiedByUserLastName), TRIM(source.ModifiedByUserFirstName), source.ChargeTransactionId, TRIM(source.PKFinancialNumber), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.RoleName), source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKMipsKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactMIPS
      GROUP BY PKMipsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactMIPS');
ELSE
  COMMIT TRANSACTION;
END IF;
