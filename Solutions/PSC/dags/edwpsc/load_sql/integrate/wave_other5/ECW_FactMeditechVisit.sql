
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechVisit AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactMeditechVisit AS source
ON target.MeditechVisitKey = source.MeditechVisitKey
WHEN MATCHED THEN
  UPDATE SET
  target.MeditechVisitKey = source.MeditechVisitKey,
 target.SendingApplication = TRIM(source.SendingApplication),
 target.UniqueChargeIdentifier = TRIM(source.UniqueChargeIdentifier),
 target.FacilityID = TRIM(source.FacilityID),
 target.PracticeID = TRIM(source.PracticeID),
 target.MRN = TRIM(source.MRN),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.AdmitDate = source.AdmitDate,
 target.DischargeDate = source.DischargeDate,
 target.PatientName = TRIM(source.PatientName),
 target.BillingProviderName = TRIM(source.BillingProviderName),
 target.Coid = TRIM(source.Coid),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PatientAge = source.PatientAge,
 target.DEALicenseNumber = TRIM(source.DEALicenseNumber),
 target.ProviderUserName = TRIM(source.ProviderUserName),
 target.NPI = TRIM(source.NPI),
 target.VisitLocationUnit = TRIM(source.VisitLocationUnit),
 target.CensusCOID = TRIM(source.CensusCOID)
WHEN NOT MATCHED THEN
  INSERT (MeditechVisitKey, SendingApplication, UniqueChargeIdentifier, FacilityID, PracticeID, MRN, VisitNumber, AdmitDate, DischargeDate, PatientName, BillingProviderName, Coid, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PatientAge, DEALicenseNumber, ProviderUserName, NPI, VisitLocationUnit, CensusCOID)
  VALUES (source.MeditechVisitKey, TRIM(source.SendingApplication), TRIM(source.UniqueChargeIdentifier), TRIM(source.FacilityID), TRIM(source.PracticeID), TRIM(source.MRN), TRIM(source.VisitNumber), source.AdmitDate, source.DischargeDate, TRIM(source.PatientName), TRIM(source.BillingProviderName), TRIM(source.Coid), TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PatientAge, TRIM(source.DEALicenseNumber), TRIM(source.ProviderUserName), TRIM(source.NPI), TRIM(source.VisitLocationUnit), TRIM(source.CensusCOID));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MeditechVisitKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechVisit
      GROUP BY MeditechVisitKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechVisit');
ELSE
  COMMIT TRANSACTION;
END IF;
