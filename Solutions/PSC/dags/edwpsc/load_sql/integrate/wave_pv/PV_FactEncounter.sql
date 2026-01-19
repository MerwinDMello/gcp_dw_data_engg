
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactEncounter AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactEncounter AS source
ON target.EncounterKey = source.EncounterKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterKey = source.EncounterKey,
 target.ClaimKey = source.ClaimKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.FacilityKey = source.FacilityKey,
 target.PatientKey = source.PatientKey,
 target.VisitReason = TRIM(source.VisitReason),
 target.VisitDate = source.VisitDate,
 target.VisitStartTime = source.VisitStartTime,
 target.VisitEndTime = source.VisitEndTime,
 target.VisitTypeKey = source.VisitTypeKey,
 target.VisitCopay = source.VisitCopay,
 target.POA = source.POA,
 target.OtherPaymentAmt = source.OtherPaymentAmt,
 target.VisitStatusKey = source.VisitStatusKey,
 target.EncounterType = TRIM(source.EncounterType),
 target.DeleteFlag = source.DeleteFlag,
 target.EncounterLock = TRIM(source.EncounterLock),
 target.VisitArrivedTime = source.VisitArrivedTime,
 target.StatusAfterCheckIn = TRIM(source.StatusAfterCheckIn),
 target.ClaimRequired = source.ClaimRequired,
 target.VisitTypeOverriden = TRIM(source.VisitTypeOverriden),
 target.ClaimNumber = source.ClaimNumber,
 target.ReferringProviderKey = source.ReferringProviderKey,
 target.FacilityResourceKey = source.FacilityResourceKey,
 target.TimeIn = source.TimeIn,
 target.TimeOut = source.TimeOut,
 target.DepartureTime = source.DepartureTime,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.LogDetailPK = TRIM(source.LogDetailPK),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName),
 target.EncounterCategory = TRIM(source.EncounterCategory),
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.QuickVisitFlag = source.QuickVisitFlag,
 target.CopayType = TRIM(source.CopayType),
 target.OtherPaymentType = TRIM(source.OtherPaymentType),
 target.LastUpdatedByUserKey = source.LastUpdatedByUserKey,
 target.LastUpdatedByUserDateKey = source.LastUpdatedByUserDateKey,
 target.QuickVisitType = TRIM(source.QuickVisitType),
 target.EligibilityStatusCode = source.EligibilityStatusCode,
 target.PreviousPayType = TRIM(source.PreviousPayType),
 target.POAPaymentNumber = source.POAPaymentNumber,
 target.IsPatientTelemedicineEligible = source.IsPatientTelemedicineEligible,
 target.TelemedicineType = source.TelemedicineType,
 target.TelemedicineStatus = source.TelemedicineStatus
WHEN NOT MATCHED THEN
  INSERT (EncounterKey, ClaimKey, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, FacilityKey, PatientKey, VisitReason, VisitDate, VisitStartTime, VisitEndTime, VisitTypeKey, VisitCopay, POA, OtherPaymentAmt, VisitStatusKey, EncounterType, DeleteFlag, EncounterLock, VisitArrivedTime, StatusAfterCheckIn, ClaimRequired, VisitTypeOverriden, ClaimNumber, ReferringProviderKey, FacilityResourceKey, TimeIn, TimeOut, DepartureTime, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, LogDetailPK, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PracticeKey, PracticeName, EncounterCategory, CreatedByUserKey, QuickVisitFlag, CopayType, OtherPaymentType, LastUpdatedByUserKey, LastUpdatedByUserDateKey, QuickVisitType, EligibilityStatusCode, PreviousPayType, POAPaymentNumber, IsPatientTelemedicineEligible, TelemedicineType, TelemedicineStatus)
  VALUES (source.EncounterKey, source.ClaimKey, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.FacilityKey, source.PatientKey, TRIM(source.VisitReason), source.VisitDate, source.VisitStartTime, source.VisitEndTime, source.VisitTypeKey, source.VisitCopay, source.POA, source.OtherPaymentAmt, source.VisitStatusKey, TRIM(source.EncounterType), source.DeleteFlag, TRIM(source.EncounterLock), source.VisitArrivedTime, TRIM(source.StatusAfterCheckIn), source.ClaimRequired, TRIM(source.VisitTypeOverriden), source.ClaimNumber, source.ReferringProviderKey, source.FacilityResourceKey, source.TimeIn, source.TimeOut, source.DepartureTime, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.LogDetailPK), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PracticeKey, TRIM(source.PracticeName), TRIM(source.EncounterCategory), source.CreatedByUserKey, source.QuickVisitFlag, TRIM(source.CopayType), TRIM(source.OtherPaymentType), source.LastUpdatedByUserKey, source.LastUpdatedByUserDateKey, TRIM(source.QuickVisitType), source.EligibilityStatusCode, TRIM(source.PreviousPayType), source.POAPaymentNumber, source.IsPatientTelemedicineEligible, source.TelemedicineType, source.TelemedicineStatus);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactEncounter
      GROUP BY EncounterKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactEncounter');
ELSE
  COMMIT TRANSACTION;
END IF;
