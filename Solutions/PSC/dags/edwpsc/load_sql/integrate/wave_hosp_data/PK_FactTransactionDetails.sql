
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactTransactionDetails AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactTransactionDetails AS source
ON target.PKTransactionDetailsKey = source.PKTransactionDetailsKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKTransactionDetailsKey = source.PKTransactionDetailsKey,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.Coid = TRIM(source.Coid),
 target.AccountId = TRIM(source.AccountId),
 target.AuthorizingUserId = TRIM(source.AuthorizingUserId),
 target.Comments = TRIM(source.Comments),
 target.ReviewedDateTime = source.ReviewedDateTime,
 target.ReviewedDateKey = source.ReviewedDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.CurrentStatus = TRIM(source.CurrentStatus),
 target.ChargeTransactionId = TRIM(source.ChargeTransactionId),
 target.VisitId = TRIM(source.VisitId),
 target.HeldForReview = source.HeldForReview,
 target.ChargeSyncId = source.ChargeSyncId,
 target.ChargeSyncRepId = source.ChargeSyncRepId,
 target.ChargeQuantity = source.ChargeQuantity,
 target.CptCode = TRIM(source.CptCode),
 target.CptCodeDescription = TRIM(source.CptCodeDescription),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.ReasonForVisit = TRIM(source.ReasonForVisit),
 target.FinancialClass = TRIM(source.FinancialClass),
 target.VisitTypeIndicator = TRIM(source.VisitTypeIndicator),
 target.BillingAreaAbbrev = TRIM(source.BillingAreaAbbrev),
 target.BillingAreaName = TRIM(source.BillingAreaName),
 target.BillingAreaNumber = TRIM(source.BillingAreaNumber),
 target.BillingDepartmentId = source.BillingDepartmentId,
 target.BillingDepartmentLabel = TRIM(source.BillingDepartmentLabel),
 target.BillingProviderFirstName = TRIM(source.BillingProviderFirstName),
 target.BillingProviderLastName = TRIM(source.BillingProviderLastName),
 target.BillingProviderUserName = TRIM(source.BillingProviderUserName),
 target.BillingProviderUserNumber = TRIM(source.BillingProviderUserNumber),
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.ModifierDescription1 = TRIM(source.ModifierDescription1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.ModifierDescription2 = TRIM(source.ModifierDescription2),
 target.CPTModifier3 = TRIM(source.CPTModifier3),
 target.ModifierDescription3 = TRIM(source.ModifierDescription3),
 target.CPTModifier4 = TRIM(source.CPTModifier4),
 target.ModifierDescription4 = TRIM(source.ModifierDescription4),
 target.CPTModifier5 = TRIM(source.CPTModifier5),
 target.ModifierDescription5 = TRIM(source.ModifierDescription5),
 target.VisitLocationFacility = TRIM(source.VisitLocationFacility),
 target.VisitLocationUnit = TRIM(source.VisitLocationUnit),
 target.VisitLocation = TRIM(source.VisitLocation),
 target.VisitLocationBed = TRIM(source.VisitLocationBed),
 target.CreatedTime = source.CreatedTime,
 target.ModifiedTime = source.ModifiedTime,
 target.ErrorsFlag = TRIM(source.ErrorsFlag),
 target.DeleteFlag = source.DeleteFlag,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.BillingProviderDeleteFlag = source.BillingProviderDeleteFlag,
 target.BillingProviderNPI = TRIM(source.BillingProviderNPI),
 target.LastModifiedTime = source.LastModifiedTime,
 target.AuthorizedTime = source.AuthorizedTime,
 target.SupervisingProvider = TRIM(source.SupervisingProvider),
 target.ServicingProvider = TRIM(source.ServicingProvider),
 target.ServiceSite = TRIM(source.ServiceSite),
 target.TransactionSyncId = TRIM(source.TransactionSyncId),
 target.SupervisingProviderNPI = TRIM(source.SupervisingProviderNPI),
 target.ServicingProviderNPI = TRIM(source.ServicingProviderNPI),
 target.SOURCE = TRIM(source.SOURCE),
 target.DischargeDate = source.DischargeDate,
 target.BatchId = source.BatchId,
 target.AdmitDate = source.AdmitDate,
 target.PracticeId = TRIM(source.PracticeId),
 target.ChargeId = TRIM(source.ChargeId),
 target.MessageControlId = TRIM(source.MessageControlId),
 target.SavedDate = source.SavedDate,
 target.SavedBy = TRIM(source.SavedBy),
 target.SavedByFirstName = TRIM(source.SavedByFirstName),
 target.SavedByLastName = TRIM(source.SavedByLastName),
 target.NonBillableReason = TRIM(source.NonBillableReason),
 target.PosType = TRIM(source.PosType),
 target.EncounterStatus = TRIM(source.EncounterStatus),
 target.CptChangeRequest = TRIM(source.CptChangeRequest),
 target.CptChangeRequestDate = source.CptChangeRequestDate,
 target.CptChangeStatus = TRIM(source.CptChangeStatus),
 target.CptChangeReason = TRIM(source.CptChangeReason),
 target.PrimaryInsuranceName = TRIM(source.PrimaryInsuranceName),
 target.SecondaryInsuranceName = TRIM(source.SecondaryInsuranceName),
 target.ReasonForHold = TRIM(source.ReasonForHold),
 target.BillingComments = TRIM(source.BillingComments),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKTransactionDetailsKey, PKRegionName, Coid, AccountId, AuthorizingUserId, Comments, ReviewedDateTime, ReviewedDateKey, ServiceDateKey, CurrentStatus, ChargeTransactionId, VisitId, HeldForReview, ChargeSyncId, ChargeSyncRepId, ChargeQuantity, CptCode, CptCodeDescription, PatientFirstName, PatientLastName, PatientMiddleName, PatientMRN, PKFinancialNumber, ReasonForVisit, FinancialClass, VisitTypeIndicator, BillingAreaAbbrev, BillingAreaName, BillingAreaNumber, BillingDepartmentId, BillingDepartmentLabel, BillingProviderFirstName, BillingProviderLastName, BillingProviderUserName, BillingProviderUserNumber, CPTModifier1, ModifierDescription1, CPTModifier2, ModifierDescription2, CPTModifier3, ModifierDescription3, CPTModifier4, ModifierDescription4, CPTModifier5, ModifierDescription5, VisitLocationFacility, VisitLocationUnit, VisitLocation, VisitLocationBed, CreatedTime, ModifiedTime, ErrorsFlag, DeleteFlag, SourceSystemCode, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, BillingProviderDeleteFlag, BillingProviderNPI, LastModifiedTime, AuthorizedTime, SupervisingProvider, ServicingProvider, ServiceSite, TransactionSyncId, SupervisingProviderNPI, ServicingProviderNPI, SOURCE, DischargeDate, BatchId, AdmitDate, PracticeId, ChargeId, MessageControlId, SavedDate, SavedBy, SavedByFirstName, SavedByLastName, NonBillableReason, PosType, EncounterStatus, CptChangeRequest, CptChangeRequestDate, CptChangeStatus, CptChangeReason, PrimaryInsuranceName, SecondaryInsuranceName, ReasonForHold, BillingComments, DWLastUpdateDateTime)
  VALUES (source.PKTransactionDetailsKey, TRIM(source.PKRegionName), TRIM(source.Coid), TRIM(source.AccountId), TRIM(source.AuthorizingUserId), TRIM(source.Comments), source.ReviewedDateTime, source.ReviewedDateKey, source.ServiceDateKey, TRIM(source.CurrentStatus), TRIM(source.ChargeTransactionId), TRIM(source.VisitId), source.HeldForReview, source.ChargeSyncId, source.ChargeSyncRepId, source.ChargeQuantity, TRIM(source.CptCode), TRIM(source.CptCodeDescription), TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PatientMiddleName), TRIM(source.PatientMRN), TRIM(source.PKFinancialNumber), TRIM(source.ReasonForVisit), TRIM(source.FinancialClass), TRIM(source.VisitTypeIndicator), TRIM(source.BillingAreaAbbrev), TRIM(source.BillingAreaName), TRIM(source.BillingAreaNumber), source.BillingDepartmentId, TRIM(source.BillingDepartmentLabel), TRIM(source.BillingProviderFirstName), TRIM(source.BillingProviderLastName), TRIM(source.BillingProviderUserName), TRIM(source.BillingProviderUserNumber), TRIM(source.CPTModifier1), TRIM(source.ModifierDescription1), TRIM(source.CPTModifier2), TRIM(source.ModifierDescription2), TRIM(source.CPTModifier3), TRIM(source.ModifierDescription3), TRIM(source.CPTModifier4), TRIM(source.ModifierDescription4), TRIM(source.CPTModifier5), TRIM(source.ModifierDescription5), TRIM(source.VisitLocationFacility), TRIM(source.VisitLocationUnit), TRIM(source.VisitLocation), TRIM(source.VisitLocationBed), source.CreatedTime, source.ModifiedTime, TRIM(source.ErrorsFlag), source.DeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.SourceAPrimaryKeyValue), source.SourceBPrimaryKeyValue, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.BillingProviderDeleteFlag, TRIM(source.BillingProviderNPI), source.LastModifiedTime, source.AuthorizedTime, TRIM(source.SupervisingProvider), TRIM(source.ServicingProvider), TRIM(source.ServiceSite), TRIM(source.TransactionSyncId), TRIM(source.SupervisingProviderNPI), TRIM(source.ServicingProviderNPI), TRIM(source.SOURCE), source.DischargeDate, source.BatchId, source.AdmitDate, TRIM(source.PracticeId), TRIM(source.ChargeId), TRIM(source.MessageControlId), source.SavedDate, TRIM(source.SavedBy), TRIM(source.SavedByFirstName), TRIM(source.SavedByLastName), TRIM(source.NonBillableReason), TRIM(source.PosType), TRIM(source.EncounterStatus), TRIM(source.CptChangeRequest), source.CptChangeRequestDate, TRIM(source.CptChangeStatus), TRIM(source.CptChangeReason), TRIM(source.PrimaryInsuranceName), TRIM(source.SecondaryInsuranceName), TRIM(source.ReasonForHold), TRIM(source.BillingComments), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKTransactionDetailsKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactTransactionDetails
      GROUP BY PKTransactionDetailsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactTransactionDetails');
ELSE
  COMMIT TRANSACTION;
END IF;
