
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterMeditechExpanse AS source
ON target.EncounterMTXKey = source.EncounterMTXKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterMTXKey = source.EncounterMTXKey,
 target.RegionKey = source.RegionKey,
 target.PatientKey = source.PatientKey,
 target.AccountNumber = TRIM(source.AccountNumber),
 target.FacilityKey = source.FacilityKey,
 target.LocationKey = source.LocationKey,
 target.SiteID = TRIM(source.SiteID),
 target.COID = TRIM(source.COID),
 target.AbsentProviderKey = source.AbsentProviderKey,
 target.AdmittingProviderKey = source.AdmittingProviderKey,
 target.EmergencyProviderKey = source.EmergencyProviderKey,
 target.FamilyProviderKey = source.FamilyProviderKey,
 target.PrimaryCareProviderKey = source.PrimaryCareProviderKey,
 target.ReferringProviderKey = source.ReferringProviderKey,
 target.SupervisingProviderKey = source.SupervisingProviderKey,
 target.VisitProviderKey = source.VisitProviderKey,
 target.PrimaryIplanKey = source.PrimaryIplanKey,
 target.EncCreatedBy = source.EncCreatedBy,
 target.EncCreatedDate = source.EncCreatedDate,
 target.ServiceDateKey = source.ServiceDateKey,
 target.RegistrationStatus = TRIM(source.RegistrationStatus),
 target.RegistrationType = TRIM(source.RegistrationType),
 target.DeleteFlag = source.DeleteFlag,
 target.ReasonForVisit = TRIM(source.ReasonForVisit),
 target.AdmitDateTime = source.AdmitDateTime,
 target.ArrivalDateTime = source.ArrivalDateTime,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.EncounterKey = source.EncounterKey,
 target.PatientID = TRIM(source.PatientID),
 target.FacilityID = TRIM(source.FacilityID),
 target.LocationID = TRIM(source.LocationID),
 target.AbsentProviderID = TRIM(source.AbsentProviderID),
 target.AdmittingProviderID = TRIM(source.AdmittingProviderID),
 target.EmergencyProviderID = TRIM(source.EmergencyProviderID),
 target.FamilyProviderID = TRIM(source.FamilyProviderID),
 target.PrimaryCareProviderID = TRIM(source.PrimaryCareProviderID),
 target.ReferringProviderID = TRIM(source.ReferringProviderID),
 target.SupervisingProviderID = TRIM(source.SupervisingProviderID),
 target.VisitProviderID = TRIM(source.VisitProviderID),
 target.PrimaryIplanID = TRIM(source.PrimaryIplanID),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterMTXKey, RegionKey, PatientKey, AccountNumber, FacilityKey, LocationKey, SiteID, COID, AbsentProviderKey, AdmittingProviderKey, EmergencyProviderKey, FamilyProviderKey, PrimaryCareProviderKey, ReferringProviderKey, SupervisingProviderKey, VisitProviderKey, PrimaryIplanKey, EncCreatedBy, EncCreatedDate, ServiceDateKey, RegistrationStatus, RegistrationType, DeleteFlag, ReasonForVisit, AdmitDateTime, ArrivalDateTime, ClaimKey, ClaimNumber, EncounterKey, PatientID, FacilityID, LocationID, AbsentProviderID, AdmittingProviderID, EmergencyProviderID, FamilyProviderID, PrimaryCareProviderID, ReferringProviderID, SupervisingProviderID, VisitProviderID, PrimaryIplanID, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterMTXKey, source.RegionKey, source.PatientKey, TRIM(source.AccountNumber), source.FacilityKey, source.LocationKey, TRIM(source.SiteID), TRIM(source.COID), source.AbsentProviderKey, source.AdmittingProviderKey, source.EmergencyProviderKey, source.FamilyProviderKey, source.PrimaryCareProviderKey, source.ReferringProviderKey, source.SupervisingProviderKey, source.VisitProviderKey, source.PrimaryIplanKey, source.EncCreatedBy, source.EncCreatedDate, source.ServiceDateKey, TRIM(source.RegistrationStatus), TRIM(source.RegistrationType), source.DeleteFlag, TRIM(source.ReasonForVisit), source.AdmitDateTime, source.ArrivalDateTime, source.ClaimKey, source.ClaimNumber, source.EncounterKey, TRIM(source.PatientID), TRIM(source.FacilityID), TRIM(source.LocationID), TRIM(source.AbsentProviderID), TRIM(source.AdmittingProviderID), TRIM(source.EmergencyProviderID), TRIM(source.FamilyProviderID), TRIM(source.PrimaryCareProviderID), TRIM(source.ReferringProviderID), TRIM(source.SupervisingProviderID), TRIM(source.VisitProviderID), TRIM(source.PrimaryIplanID), TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterMTXKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMeditechExpanse
      GROUP BY EncounterMTXKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
