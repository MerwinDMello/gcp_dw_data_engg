
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterCPT AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactEncounterCPT AS source
ON target.EncounterCPTKey = source.EncounterCPTKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterCPTKey = source.EncounterCPTKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.ClaimKey = source.ClaimKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.PatientInternalId = source.PatientInternalId,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTUnits = source.CPTUnits,
 target.CPTCharges = source.CPTCharges,
 target.VisitDate = source.VisitDate,
 target.PrimaryFlag = source.PrimaryFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.DeletedByUserKey = source.DeletedByUserKey,
 target.DeletedOnDateKey = source.DeletedOnDateKey,
 target.EncounterStatus = TRIM(source.EncounterStatus),
 target.AccountId = source.AccountId,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderId = TRIM(source.ServicingProviderId),
 target.BillingProviderKey = source.BillingProviderKey,
 target.BillingProviderId = source.BillingProviderId,
 target.PatientKey = source.PatientKey,
 target.PatientId = source.PatientId,
 target.PracticeKey = source.PracticeKey,
 target.PracticeId = source.PracticeId,
 target.TransactionId = source.TransactionId,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceCPrimaryKeyValue = TRIM(source.SourceCPrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterCPTKey, RegionKey, Coid, ClaimKey, EncounterKey, EncounterID, PatientInternalId, CPTCodeKey, CPTCode, CPTUnits, CPTCharges, VisitDate, PrimaryFlag, DeleteFlag, CreatedByUserKey, DeletedByUserKey, DeletedOnDateKey, EncounterStatus, AccountId, ServicingProviderKey, ServicingProviderId, BillingProviderKey, BillingProviderId, PatientKey, PatientId, PracticeKey, PracticeId, TransactionId, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SourceCPrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterCPTKey, source.RegionKey, TRIM(source.Coid), source.ClaimKey, source.EncounterKey, source.EncounterID, source.PatientInternalId, source.CPTCodeKey, TRIM(source.CPTCode), source.CPTUnits, source.CPTCharges, source.VisitDate, source.PrimaryFlag, source.DeleteFlag, source.CreatedByUserKey, source.DeletedByUserKey, source.DeletedOnDateKey, TRIM(source.EncounterStatus), source.AccountId, source.ServicingProviderKey, TRIM(source.ServicingProviderId), source.BillingProviderKey, source.BillingProviderId, source.PatientKey, source.PatientId, source.PracticeKey, source.PracticeId, source.TransactionId, source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.SourceCPrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterCPTKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterCPT
      GROUP BY EncounterCPTKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterCPT');
ELSE
  COMMIT TRANSACTION;
END IF;
