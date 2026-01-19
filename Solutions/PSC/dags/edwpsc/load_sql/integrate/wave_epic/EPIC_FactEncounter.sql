
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounter AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactEncounter AS source
ON target.EncounterKey = source.EncounterKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterKey = source.EncounterKey,
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
 target.VisitCopayDue = source.VisitCopayDue,
 target.VisitStatusKey = source.VisitStatusKey,
 target.EncounterType = TRIM(source.EncounterType),
 target.DeleteFlag = source.DeleteFlag,
 target.EncounterLock = source.EncounterLock,
 target.VisitArrivedTime = source.VisitArrivedTime,
 target.StatusAfterCheckIn = TRIM(source.StatusAfterCheckIn),
 target.ClaimRequired = source.ClaimRequired,
 target.VisitTypeOverriden = TRIM(source.VisitTypeOverriden),
 target.VisitProviderKey = source.VisitProviderKey,
 target.ReferringProviderKey = source.ReferringProviderKey,
 target.PCPProviderKey = source.PCPProviderKey,
 target.FacilityResourceKey = source.FacilityResourceKey,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.TimeIn = source.TimeIn,
 target.TimeOut = source.TimeOut,
 target.DepartureTime = source.DepartureTime,
 target.PatEncCsnId = source.PatEncCsnId,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PracticeKey = source.PracticeKey,
 target.LastClaimKey = source.LastClaimKey,
 target.EncounterTypeKey = source.EncounterTypeKey,
 target.GFERequested = source.GFERequested,
 target.GFECompleted = source.GFECompleted,
 target.GFEDeclined = source.GFEDeclined
WHEN NOT MATCHED THEN
  INSERT (EncounterKey, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, FacilityKey, PatientKey, VisitReason, VisitDate, VisitStartTime, VisitEndTime, VisitTypeKey, VisitCopay, VisitCopayDue, VisitStatusKey, EncounterType, DeleteFlag, EncounterLock, VisitArrivedTime, StatusAfterCheckIn, ClaimRequired, VisitTypeOverriden, VisitProviderKey, ReferringProviderKey, PCPProviderKey, FacilityResourceKey, CreatedByUserKey, TimeIn, TimeOut, DepartureTime, PatEncCsnId, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PracticeKey, LastClaimKey, EncounterTypeKey, GFERequested, GFECompleted, GFEDeclined)
  VALUES (source.EncounterKey, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.FacilityKey, source.PatientKey, TRIM(source.VisitReason), source.VisitDate, source.VisitStartTime, source.VisitEndTime, source.VisitTypeKey, source.VisitCopay, source.VisitCopayDue, source.VisitStatusKey, TRIM(source.EncounterType), source.DeleteFlag, source.EncounterLock, source.VisitArrivedTime, TRIM(source.StatusAfterCheckIn), source.ClaimRequired, TRIM(source.VisitTypeOverriden), source.VisitProviderKey, source.ReferringProviderKey, source.PCPProviderKey, source.FacilityResourceKey, source.CreatedByUserKey, source.TimeIn, source.TimeOut, source.DepartureTime, source.PatEncCsnId, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PracticeKey, source.LastClaimKey, source.EncounterTypeKey, source.GFERequested, source.GFECompleted, source.GFEDeclined);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounter
      GROUP BY EncounterKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounter');
ELSE
  COMMIT TRANSACTION;
END IF;
