
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtInterfaceMessagesPK AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtInterfaceMessagesPK AS source
ON target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue AND target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue AND target.SendingApplication = source.SendingApplication AND target.ProcedureCode = source.ProcedureCode
WHEN MATCHED THEN
  UPDATE SET
  target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CreatedDate = source.CreatedDate,
 target.SendingApplication = TRIM(source.SendingApplication),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.MessageControlId = TRIM(source.MessageControlId),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientId = TRIM(source.PatientId),
 target.PatientClass = TRIM(source.PatientClass),
 target.PatientType = TRIM(source.PatientType),
 target.FinancialClass = TRIM(source.FinancialClass),
 target.PrimaryIplanKey = source.PrimaryIplanKey,
 target.PrimaryInsuranceId = TRIM(source.PrimaryInsuranceId),
 target.PrimaryEcwInsuranceId = source.PrimaryEcwInsuranceId,
 target.PrimaryInsuranceName = TRIM(source.PrimaryInsuranceName),
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.CPTModifier = TRIM(source.CPTModifier),
 target.ProcedureDate = source.ProcedureDate,
 target.ProcedurePOS = TRIM(source.ProcedurePOS),
 target.RevenueCode = TRIM(source.RevenueCode),
 target.FacilityKey = source.FacilityKey,
 target.FacilityId = TRIM(source.FacilityId),
 target.eCWFacilityId = TRIM(source.eCWFacilityId),
 target.Practice = TRIM(source.Practice),
 target.EncounterId = source.EncounterId,
 target.EncounterKey = source.EncounterKey,
 target.ClaimId = source.ClaimId,
 target.ClaimKey = source.ClaimKey,
 target.ReconCategory = TRIM(source.ReconCategory),
 target.SourceMessage = TRIM(source.SourceMessage),
 target.BatchDate = source.BatchDate,
 target.CPTUnits = source.CPTUnits,
 target.CDMCode = TRIM(source.CDMCode),
 target.ConsolidatedFlag = source.ConsolidatedFlag,
 target.ConsolidationTargetFlag = source.ConsolidationTargetFlag,
 target.FirstBatchDate = source.FirstBatchDate,
 target.MessageCreatedDate = source.MessageCreatedDate,
 target.DateMessageReceivedOriginal = source.DateMessageReceivedOriginal,
 target.DateMessageReceived = source.DateMessageReceived,
 target.ClaimCreatedByUserId = TRIM(source.ClaimCreatedByUserId),
 target.VoidFlag = TRIM(source.VoidFlag),
 target.ReceivedFlag = source.ReceivedFlag,
 target.TransmittedFlag = source.TransmittedFlag,
 target.DateMessageTransmitted = source.DateMessageTransmitted,
 target.PendingFlag = source.PendingFlag,
 target.eCWSuppressFlag = source.eCWSuppressFlag,
 target.BatchId = TRIM(source.BatchId),
 target.AuthorizationNumber = TRIM(source.AuthorizationNumber),
 target.HospitalService = TRIM(source.HospitalService),
 target.IsError = source.IsError,
 target.IsErrorOpenConnect = source.IsErrorOpenConnect,
 target.IsErrorEcw = source.IsErrorEcw,
 target.FacilityActiveFlag = source.FacilityActiveFlag,
 target.SystemSuppressFlag = source.SystemSuppressFlag,
 target.SuppressFlag = source.SuppressFlag,
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
 target.OrderedById = TRIM(source.OrderedById),
 target.TriggeringEvent = TRIM(source.TriggeringEvent),
 target.LastMessageId = TRIM(source.LastMessageId),
 target.LastMessageControlId = TRIM(source.LastMessageControlId),
 target.FirstStatusReason = TRIM(source.FirstStatusReason),
 target.FirstStatusMessage = TRIM(source.FirstStatusMessage),
 target.FirstStatusDate = source.FirstStatusDate,
 target.FirstCategory = TRIM(source.FirstCategory),
 target.FirstCategoryDate = source.FirstCategoryDate,
 target.LastStatusReason = TRIM(source.LastStatusReason),
 target.LastStatusMessage = TRIM(source.LastStatusMessage),
 target.LastStatusDate = source.LastStatusDate,
 target.LastCategory = TRIM(source.LastCategory),
 target.LastCategoryDate = source.LastCategoryDate,
 target.FirstIsError = source.FirstIsError,
 target.FirstIsErrorDate = source.FirstIsErrorDate,
 target.FirstIsErrorEcw = source.FirstIsErrorEcw,
 target.FirstIsErrorEcwDate = source.FirstIsErrorEcwDate,
 target.FirstSuppressFlag = source.FirstSuppressFlag,
 target.FirstSuppressFlagDate = source.FirstSuppressFlagDate,
 target.FirstDFTDateReceived = source.FirstDFTDateReceived,
 target.LastDFTDateReceived = source.LastDFTDateReceived,
 target.FirstPendingFlag = source.FirstPendingFlag,
 target.FirsteCWSuppressFlag = source.FirsteCWSuppressFlag,
 target.FirsteCWSuppressFlagDate = source.FirsteCWSuppressFlagDate,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdatedDate = source.DWLastUpdatedDate,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (RegionKey, Coid, CreatedDate, SendingApplication, VisitNumber, MessageControlId, PatientLastName, PatientFirstName, PatientId, PatientClass, PatientType, FinancialClass, PrimaryIplanKey, PrimaryInsuranceId, PrimaryEcwInsuranceId, PrimaryInsuranceName, ProcedureCode, CPTModifier, ProcedureDate, ProcedurePOS, RevenueCode, FacilityKey, FacilityId, eCWFacilityId, Practice, EncounterId, EncounterKey, ClaimId, ClaimKey, ReconCategory, SourceMessage, BatchDate, CPTUnits, CDMCode, ConsolidatedFlag, ConsolidationTargetFlag, FirstBatchDate, MessageCreatedDate, DateMessageReceivedOriginal, DateMessageReceived, ClaimCreatedByUserId, VoidFlag, ReceivedFlag, TransmittedFlag, DateMessageTransmitted, PendingFlag, eCWSuppressFlag, BatchId, AuthorizationNumber, HospitalService, IsError, IsErrorOpenConnect, IsErrorEcw, FacilityActiveFlag, SystemSuppressFlag, SuppressFlag, AdmittingProviderKey, AdmittingProviderId, BillingProviderKey, BillingProviderId, ServicingProviderKey, ServicingProviderId, PerformedByProviderKey, PerformedByProviderId, ReferringProviderKey, ReferringProviderId, ConsultingProviderKey, ConsultingProviderId, ProviderKey, ProviderId, OrderedByProviderKey, OrderedById, TriggeringEvent, LastMessageId, LastMessageControlId, FirstStatusReason, FirstStatusMessage, FirstStatusDate, FirstCategory, FirstCategoryDate, LastStatusReason, LastStatusMessage, LastStatusDate, LastCategory, LastCategoryDate, FirstIsError, FirstIsErrorDate, FirstIsErrorEcw, FirstIsErrorEcwDate, FirstSuppressFlag, FirstSuppressFlagDate, FirstDFTDateReceived, LastDFTDateReceived, FirstPendingFlag, FirsteCWSuppressFlag, FirsteCWSuppressFlagDate, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SourceSystemCode, DWLastUpdatedDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.RegionKey, TRIM(source.Coid), source.CreatedDate, TRIM(source.SendingApplication), TRIM(source.VisitNumber), TRIM(source.MessageControlId), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.PatientId), TRIM(source.PatientClass), TRIM(source.PatientType), TRIM(source.FinancialClass), source.PrimaryIplanKey, TRIM(source.PrimaryInsuranceId), source.PrimaryEcwInsuranceId, TRIM(source.PrimaryInsuranceName), TRIM(source.ProcedureCode), TRIM(source.CPTModifier), source.ProcedureDate, TRIM(source.ProcedurePOS), TRIM(source.RevenueCode), source.FacilityKey, TRIM(source.FacilityId), TRIM(source.eCWFacilityId), TRIM(source.Practice), source.EncounterId, source.EncounterKey, source.ClaimId, source.ClaimKey, TRIM(source.ReconCategory), TRIM(source.SourceMessage), source.BatchDate, source.CPTUnits, TRIM(source.CDMCode), source.ConsolidatedFlag, source.ConsolidationTargetFlag, source.FirstBatchDate, source.MessageCreatedDate, source.DateMessageReceivedOriginal, source.DateMessageReceived, TRIM(source.ClaimCreatedByUserId), TRIM(source.VoidFlag), source.ReceivedFlag, source.TransmittedFlag, source.DateMessageTransmitted, source.PendingFlag, source.eCWSuppressFlag, TRIM(source.BatchId), TRIM(source.AuthorizationNumber), TRIM(source.HospitalService), source.IsError, source.IsErrorOpenConnect, source.IsErrorEcw, source.FacilityActiveFlag, source.SystemSuppressFlag, source.SuppressFlag, source.AdmittingProviderKey, TRIM(source.AdmittingProviderId), source.BillingProviderKey, TRIM(source.BillingProviderId), source.ServicingProviderKey, TRIM(source.ServicingProviderId), source.PerformedByProviderKey, TRIM(source.PerformedByProviderId), source.ReferringProviderKey, TRIM(source.ReferringProviderId), source.ConsultingProviderKey, TRIM(source.ConsultingProviderId), source.ProviderKey, TRIM(source.ProviderId), source.OrderedByProviderKey, TRIM(source.OrderedById), TRIM(source.TriggeringEvent), TRIM(source.LastMessageId), TRIM(source.LastMessageControlId), TRIM(source.FirstStatusReason), TRIM(source.FirstStatusMessage), source.FirstStatusDate, TRIM(source.FirstCategory), source.FirstCategoryDate, TRIM(source.LastStatusReason), TRIM(source.LastStatusMessage), source.LastStatusDate, TRIM(source.LastCategory), source.LastCategoryDate, source.FirstIsError, source.FirstIsErrorDate, source.FirstIsErrorEcw, source.FirstIsErrorEcwDate, source.FirstSuppressFlag, source.FirstSuppressFlagDate, source.FirstDFTDateReceived, source.LastDFTDateReceived, source.FirstPendingFlag, source.FirsteCWSuppressFlag, source.FirsteCWSuppressFlagDate, TRIM(source.SourceAPrimaryKeyValue), TRIM(source.SourceBPrimaryKeyValue), TRIM(source.SourceSystemCode), source.DWLastUpdatedDate, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SendingApplication, ProcedureCode
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtInterfaceMessagesPK
      GROUP BY SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SendingApplication, ProcedureCode
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtInterfaceMessagesPK');
ELSE
  COMMIT TRANSACTION;
END IF;
