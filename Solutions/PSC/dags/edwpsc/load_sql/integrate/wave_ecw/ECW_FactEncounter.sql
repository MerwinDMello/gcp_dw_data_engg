
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounter AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounter AS source
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
 target.VisitStatusKey = source.VisitStatusKey,
 target.EncounterType = source.EncounterType,
 target.DeleteFlag = source.DeleteFlag,
 target.EncounterLock = source.EncounterLock,
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
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.SourcePatientAcctNumber = TRIM(source.SourcePatientAcctNumber),
 target.AppointmentCreatedDate = source.AppointmentCreatedDate,
 target.AppointmentCreatedBy = source.AppointmentCreatedBy,
 target.GFEStatus = TRIM(source.GFEStatus),
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (EncounterKey, ClaimKey, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, FacilityKey, PatientKey, VisitReason, VisitDate, VisitStartTime, VisitEndTime, VisitTypeKey, VisitCopay, VisitStatusKey, EncounterType, DeleteFlag, EncounterLock, VisitArrivedTime, StatusAfterCheckIn, ClaimRequired, VisitTypeOverriden, ClaimNumber, ReferringProviderKey, FacilityResourceKey, TimeIn, TimeOut, DepartureTime, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SourcePatientAcctNumber, AppointmentCreatedDate, AppointmentCreatedBy, GFEStatus, ArchivedRecord)
  VALUES (source.EncounterKey, source.ClaimKey, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.FacilityKey, source.PatientKey, TRIM(source.VisitReason), source.VisitDate, source.VisitStartTime, source.VisitEndTime, source.VisitTypeKey, source.VisitCopay, source.VisitStatusKey, source.EncounterType, source.DeleteFlag, source.EncounterLock, source.VisitArrivedTime, TRIM(source.StatusAfterCheckIn), source.ClaimRequired, TRIM(source.VisitTypeOverriden), source.ClaimNumber, source.ReferringProviderKey, source.FacilityResourceKey, source.TimeIn, source.TimeOut, source.DepartureTime, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.SourcePatientAcctNumber), source.AppointmentCreatedDate, source.AppointmentCreatedBy, TRIM(source.GFEStatus), TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounter
      GROUP BY EncounterKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounter');
ELSE
  COMMIT TRANSACTION;
END IF;
