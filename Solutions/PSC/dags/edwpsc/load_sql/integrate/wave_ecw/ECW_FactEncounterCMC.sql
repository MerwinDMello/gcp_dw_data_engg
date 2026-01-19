
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterCMC AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterCMC AS source
ON target.EncounterCMCKey = source.EncounterCMCKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterCMCKey = source.EncounterCMCKey,
 target.PatientMRN = TRIM(source.PatientMRN),
 target.VisitID = TRIM(source.VisitID),
 target.PatientName = TRIM(source.PatientName),
 target.ServiceDateKey = source.ServiceDateKey,
 target.AuthoredDateTime = source.AuthoredDateTime,
 target.ProviderName = TRIM(source.ProviderName),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.AdmitDateTime = source.AdmitDateTime,
 target.DischargeDateTime = source.DischargeDateTime,
 target.LOCATION = TRIM(source.LOCATION),
 target.VisitStatus = TRIM(source.VisitStatus),
 target.POS = TRIM(source.POS),
 target.DerivedFrom = TRIM(source.DerivedFrom),
 target.SourceReport = TRIM(source.SourceReport),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.FileDate = source.FileDate,
 target.FileName = TRIM(source.FileName),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = source.InsertedBy,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = source.ModifiedBy,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DocStatus = TRIM(source.DocStatus),
 target.CodingStatus = TRIM(source.CodingStatus)
WHEN NOT MATCHED THEN
  INSERT (EncounterCMCKey, PatientMRN, VisitID, PatientName, ServiceDateKey, AuthoredDateTime, ProviderName, ProviderNPI, AdmitDateTime, DischargeDateTime, LOCATION, VisitStatus, POS, DerivedFrom, SourceReport, SourceSystemCode, FileDate, FileName, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DocStatus, CodingStatus)
  VALUES (source.EncounterCMCKey, TRIM(source.PatientMRN), TRIM(source.VisitID), TRIM(source.PatientName), source.ServiceDateKey, source.AuthoredDateTime, TRIM(source.ProviderName), TRIM(source.ProviderNPI), source.AdmitDateTime, source.DischargeDateTime, TRIM(source.LOCATION), TRIM(source.VisitStatus), TRIM(source.POS), TRIM(source.DerivedFrom), TRIM(source.SourceReport), TRIM(source.SourceSystemCode), source.FileDate, TRIM(source.FileName), source.DWLastUpdateDateTime, source.InsertedBy, source.InsertedDTM, source.ModifiedBy, source.ModifiedDTM, TRIM(source.DocStatus), TRIM(source.CodingStatus));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterCMCKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterCMC
      GROUP BY EncounterCMCKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterCMC');
ELSE
  COMMIT TRANSACTION;
END IF;
