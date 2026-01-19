
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_RprtErrorDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_RprtErrorDetail AS source
ON target.OpenConnectErrorDetailKey = source.OpenConnectErrorDetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.OpenConnectErrorDetailKey = source.OpenConnectErrorDetailKey,
 target.SnapShotDateKey = source.SnapShotDateKey,
 target.MessageId = TRIM(source.MessageId),
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CreatedDate = source.CreatedDate,
 target.DateMessageReceivedOriginal = source.DateMessageReceivedOriginal,
 target.DateMessageReceived = source.DateMessageReceived,
 target.SendingApplication = TRIM(source.SendingApplication),
 target.CrosswalkErrorRollup = TRIM(source.CrosswalkErrorRollup),
 target.CrosswalkError = TRIM(source.CrosswalkError),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.MessageControlId = TRIM(source.MessageControlId),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientDateOfBirth = source.PatientDateOfBirth,
 target.MissionPatientId = TRIM(source.MissionPatientId),
 target.PatientClass = TRIM(source.PatientClass),
 target.PatientType = TRIM(source.PatientType),
 target.FinancialClass = TRIM(source.FinancialClass),
 target.PrimaryIplanKey = source.PrimaryIplanKey,
 target.PrimaryInsuranceId = TRIM(source.PrimaryInsuranceId),
 target.PrimaryEcwInsuranceId = TRIM(source.PrimaryEcwInsuranceId),
 target.PrimaryInsuranceName = TRIM(source.PrimaryInsuranceName),
 target.SecondaryIplanKey = source.SecondaryIplanKey,
 target.SecondaryInsuranceId = TRIM(source.SecondaryInsuranceId),
 target.SecondaryEcwInsuranceId = TRIM(source.SecondaryEcwInsuranceId),
 target.SecondaryInsuranceName = TRIM(source.SecondaryInsuranceName),
 target.TertiaryIplanKey = source.TertiaryIplanKey,
 target.TertiaryInsuranceId = TRIM(source.TertiaryInsuranceId),
 target.TertiaryEcwInsuranceId = TRIM(source.TertiaryEcwInsuranceId),
 target.TertiaryInsuranceName = TRIM(source.TertiaryInsuranceName),
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.CPTModifier = TRIM(source.CPTModifier),
 target.ProcedureDate = source.ProcedureDate,
 target.ProcedurePOS = TRIM(source.ProcedurePOS),
 target.AdmittingProviderKey = source.AdmittingProviderKey,
 target.AdmittingProviderId = TRIM(source.AdmittingProviderId),
 target.BillingProviderKey = source.BillingProviderKey,
 target.BillingProviderId = TRIM(source.BillingProviderId),
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderId = TRIM(source.ServicingProviderId),
 target.PerformedByProviderKey = source.PerformedByProviderKey,
 target.PerformedByProviderId = TRIM(source.PerformedByProviderId),
 target.ReferringProviderKey = source.ReferringProviderKey,
 target.ReferringProviderId = TRIM(source.ReferringProviderId),
 target.ConsultingProviderKey = source.ConsultingProviderKey,
 target.ConsultingProviderId = TRIM(source.ConsultingProviderId),
 target.ProviderKey = source.ProviderKey,
 target.ProviderId = TRIM(source.ProviderId),
 target.OrderedByProviderKey = source.OrderedByProviderKey,
 target.OrderedByProviderId = TRIM(source.OrderedByProviderId),
 target.VisitNumberSplitCheckProviderKey = source.VisitNumberSplitCheckProviderKey,
 target.VisitNumberSplitCheckProviderId = TRIM(source.VisitNumberSplitCheckProviderId),
 target.FacilityKey = source.FacilityKey,
 target.FacilityId = TRIM(source.FacilityId),
 target.eCWFacilityId = TRIM(source.eCWFacilityId),
 target.FacilityActiveFlag = source.FacilityActiveFlag,
 target.Practice = TRIM(source.Practice),
 target.EncounterId = TRIM(source.EncounterId),
 target.EncounterKey = source.EncounterKey,
 target.HospitalService = TRIM(source.HospitalService),
 target.TriggeringEvent = TRIM(source.TriggeringEvent),
 target.SuppressFlag = source.SuppressFlag,
 target.Category = TRIM(source.Category),
 target.ADTMessageControlId = TRIM(source.ADTMessageControlId),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (OpenConnectErrorDetailKey, SnapShotDateKey, MessageId, RegionKey, Coid, CreatedDate, DateMessageReceivedOriginal, DateMessageReceived, SendingApplication, CrosswalkErrorRollup, CrosswalkError, VisitNumber, MessageControlId, PatientLastName, PatientFirstName, PatientDateOfBirth, MissionPatientId, PatientClass, PatientType, FinancialClass, PrimaryIplanKey, PrimaryInsuranceId, PrimaryEcwInsuranceId, PrimaryInsuranceName, SecondaryIplanKey, SecondaryInsuranceId, SecondaryEcwInsuranceId, SecondaryInsuranceName, TertiaryIplanKey, TertiaryInsuranceId, TertiaryEcwInsuranceId, TertiaryInsuranceName, ProcedureCode, CPTModifier, ProcedureDate, ProcedurePOS, AdmittingProviderKey, AdmittingProviderId, BillingProviderKey, BillingProviderId, ServicingProviderKey, ServicingProviderId, PerformedByProviderKey, PerformedByProviderId, ReferringProviderKey, ReferringProviderId, ConsultingProviderKey, ConsultingProviderId, ProviderKey, ProviderId, OrderedByProviderKey, OrderedByProviderId, VisitNumberSplitCheckProviderKey, VisitNumberSplitCheckProviderId, FacilityKey, FacilityId, eCWFacilityId, FacilityActiveFlag, Practice, EncounterId, EncounterKey, HospitalService, TriggeringEvent, SuppressFlag, Category, ADTMessageControlId, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.OpenConnectErrorDetailKey, source.SnapShotDateKey, TRIM(source.MessageId), source.RegionKey, TRIM(source.Coid), source.CreatedDate, source.DateMessageReceivedOriginal, source.DateMessageReceived, TRIM(source.SendingApplication), TRIM(source.CrosswalkErrorRollup), TRIM(source.CrosswalkError), TRIM(source.VisitNumber), TRIM(source.MessageControlId), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), source.PatientDateOfBirth, TRIM(source.MissionPatientId), TRIM(source.PatientClass), TRIM(source.PatientType), TRIM(source.FinancialClass), source.PrimaryIplanKey, TRIM(source.PrimaryInsuranceId), TRIM(source.PrimaryEcwInsuranceId), TRIM(source.PrimaryInsuranceName), source.SecondaryIplanKey, TRIM(source.SecondaryInsuranceId), TRIM(source.SecondaryEcwInsuranceId), TRIM(source.SecondaryInsuranceName), source.TertiaryIplanKey, TRIM(source.TertiaryInsuranceId), TRIM(source.TertiaryEcwInsuranceId), TRIM(source.TertiaryInsuranceName), TRIM(source.ProcedureCode), TRIM(source.CPTModifier), source.ProcedureDate, TRIM(source.ProcedurePOS), source.AdmittingProviderKey, TRIM(source.AdmittingProviderId), source.BillingProviderKey, TRIM(source.BillingProviderId), source.ServicingProviderKey, TRIM(source.ServicingProviderId), source.PerformedByProviderKey, TRIM(source.PerformedByProviderId), source.ReferringProviderKey, TRIM(source.ReferringProviderId), source.ConsultingProviderKey, TRIM(source.ConsultingProviderId), source.ProviderKey, TRIM(source.ProviderId), source.OrderedByProviderKey, TRIM(source.OrderedByProviderId), source.VisitNumberSplitCheckProviderKey, TRIM(source.VisitNumberSplitCheckProviderId), source.FacilityKey, TRIM(source.FacilityId), TRIM(source.eCWFacilityId), source.FacilityActiveFlag, TRIM(source.Practice), TRIM(source.EncounterId), source.EncounterKey, TRIM(source.HospitalService), TRIM(source.TriggeringEvent), source.SuppressFlag, TRIM(source.Category), TRIM(source.ADTMessageControlId), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OpenConnectErrorDetailKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_RprtErrorDetail
      GROUP BY OpenConnectErrorDetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_RprtErrorDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
