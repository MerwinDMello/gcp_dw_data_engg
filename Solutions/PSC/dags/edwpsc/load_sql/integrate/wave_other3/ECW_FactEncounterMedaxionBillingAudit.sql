
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionBillingAudit AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterMedaxionBillingAudit AS source
ON target.MedaxionBillingAuditKey = source.MedaxionBillingAuditKey
WHEN MATCHED THEN
  UPDATE SET
  target.MedaxionBillingAuditKey = source.MedaxionBillingAuditKey,
 target.MedaxionRegionName = TRIM(source.MedaxionRegionName),
 target.MedaxionLocation = TRIM(source.MedaxionLocation),
 target.ServiceDateKey = source.ServiceDateKey,
 target.ServiceDateTime = source.ServiceDateTime,
 target.CaseNumberFIN = TRIM(source.CaseNumberFIN),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.PatientName = TRIM(source.PatientName),
 target.ProviderName = TRIM(source.ProviderName),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ReportType = TRIM(source.ReportType),
 target.EventReason = TRIM(source.EventReason),
 target.FirstFileDate = source.FirstFileDate,
 target.LastFileDate = source.LastFileDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (MedaxionBillingAuditKey, MedaxionRegionName, MedaxionLocation, ServiceDateKey, ServiceDateTime, CaseNumberFIN, PatientMRN, PatientName, ProviderName, ProviderNPI, ReportType, EventReason, FirstFileDate, LastFileDate, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.MedaxionBillingAuditKey, TRIM(source.MedaxionRegionName), TRIM(source.MedaxionLocation), source.ServiceDateKey, source.ServiceDateTime, TRIM(source.CaseNumberFIN), TRIM(source.PatientMRN), TRIM(source.PatientName), TRIM(source.ProviderName), TRIM(source.ProviderNPI), TRIM(source.ReportType), TRIM(source.EventReason), source.FirstFileDate, source.LastFileDate, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MedaxionBillingAuditKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionBillingAudit
      GROUP BY MedaxionBillingAuditKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionBillingAudit');
ELSE
  COMMIT TRANSACTION;
END IF;
