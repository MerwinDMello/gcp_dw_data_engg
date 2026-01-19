
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterToCharge AS source
ON target.EncounterToChargeKey = source.EncounterToChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterToChargeKey = source.EncounterToChargeKey,
 target.EncounterRegionID = source.EncounterRegionID,
 target.EncounterRegionName = TRIM(source.EncounterRegionName),
 target.EncounterSourceSystem = TRIM(source.EncounterSourceSystem),
 target.SendingApplication = TRIM(source.SendingApplication),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.FacilityMnemonic = TRIM(source.FacilityMnemonic),
 target.PlaceOfServiceKey = source.PlaceOfServiceKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.LOCATION = TRIM(source.LOCATION),
 target.HospitalCOID = TRIM(source.HospitalCOID),
 target.Coid = TRIM(source.Coid),
 target.SiteCode = TRIM(source.SiteCode),
 target.AdmitDateKey = source.AdmitDateKey,
 target.AdmitDTM = source.AdmitDTM,
 target.DischargeDateKey = source.DischargeDateKey,
 target.DischargeDTM = source.DischargeDTM,
 target.CensusDateKey = source.CensusDateKey,
 target.PatientName = TRIM(source.PatientName),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.PatientAge = source.PatientAge,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterToChargeKey, EncounterRegionID, EncounterRegionName, EncounterSourceSystem, SendingApplication, VisitNumber, FacilityMnemonic, PlaceOfServiceKey, PracticeID, LOCATION, HospitalCOID, Coid, SiteCode, AdmitDateKey, AdmitDTM, DischargeDateKey, DischargeDTM, CensusDateKey, PatientName, PatientMRN, PatientAge, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterToChargeKey, source.EncounterRegionID, TRIM(source.EncounterRegionName), TRIM(source.EncounterSourceSystem), TRIM(source.SendingApplication), TRIM(source.VisitNumber), TRIM(source.FacilityMnemonic), source.PlaceOfServiceKey, TRIM(source.PracticeID), TRIM(source.LOCATION), TRIM(source.HospitalCOID), TRIM(source.Coid), TRIM(source.SiteCode), source.AdmitDateKey, source.AdmitDTM, source.DischargeDateKey, source.DischargeDTM, source.CensusDateKey, TRIM(source.PatientName), TRIM(source.PatientMRN), source.PatientAge, TRIM(source.SourcePrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterToChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToCharge
      GROUP BY EncounterToChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
