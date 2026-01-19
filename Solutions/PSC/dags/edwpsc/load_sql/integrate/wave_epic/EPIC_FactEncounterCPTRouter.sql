
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterCPTRouter AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactEncounterCPTRouter AS source
ON target.EncounterCPTRouterKey = source.EncounterCPTRouterKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterCPTRouterKey = source.EncounterCPTRouterKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.VisitNumber = source.VisitNumber,
 target.EncounterKey = source.EncounterKey,
 target.EncounterId = source.EncounterId,
 target.PatientInternalId = source.PatientInternalId,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTUnits = source.CPTUnits,
 target.ServiceDateKey = source.ServiceDateKey,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.CreatedDateTime = source.CreatedDateTime,
 target.CreatedByUserComments = TRIM(source.CreatedByUserComments),
 target.ChargeFiledDateTime = source.ChargeFiledDateTime,
 target.AccountId = source.AccountId,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderId = TRIM(source.ServicingProviderId),
 target.BillingProviderKey = source.BillingProviderKey,
 target.BillingProviderId = TRIM(source.BillingProviderId),
 target.PatientKey = source.PatientKey,
 target.PatientId = TRIM(source.PatientId),
 target.PracticeKey = source.PracticeKey,
 target.PracticeId = TRIM(source.PracticeId),
 target.ServiceAreaKey = source.ServiceAreaKey,
 target.ServiceAreaId = source.ServiceAreaId,
 target.TriggeredDateKey = source.TriggeredDateKey,
 target.HospitalAccountId = source.HospitalAccountId,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterCPTRouterKey, RegionKey, Coid, VisitNumber, EncounterKey, EncounterId, PatientInternalId, CPTCodeKey, CPTCode, CPTUnits, ServiceDateKey, CreatedByUserKey, CreatedDateTime, CreatedByUserComments, ChargeFiledDateTime, AccountId, ServicingProviderKey, ServicingProviderId, BillingProviderKey, BillingProviderId, PatientKey, PatientId, PracticeKey, PracticeId, ServiceAreaKey, ServiceAreaId, TriggeredDateKey, HospitalAccountId, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterCPTRouterKey, source.RegionKey, TRIM(source.Coid), source.VisitNumber, source.EncounterKey, source.EncounterId, source.PatientInternalId, source.CPTCodeKey, TRIM(source.CPTCode), source.CPTUnits, source.ServiceDateKey, source.CreatedByUserKey, source.CreatedDateTime, TRIM(source.CreatedByUserComments), source.ChargeFiledDateTime, source.AccountId, source.ServicingProviderKey, TRIM(source.ServicingProviderId), source.BillingProviderKey, TRIM(source.BillingProviderId), source.PatientKey, TRIM(source.PatientId), source.PracticeKey, TRIM(source.PracticeId), source.ServiceAreaKey, source.ServiceAreaId, source.TriggeredDateKey, source.HospitalAccountId, TRIM(source.SourceAPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterCPTRouterKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterCPTRouter
      GROUP BY EncounterCPTRouterKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterCPTRouter');
ELSE
  COMMIT TRANSACTION;
END IF;
