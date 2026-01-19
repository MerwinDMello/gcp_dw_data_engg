
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechVisitEMPI AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactMeditechVisitEMPI AS source
ON target.MeditechVisitEMPIKey = source.MeditechVisitEMPIKey
WHEN MATCHED THEN
  UPDATE SET
  target.MeditechVisitEMPIKey = source.MeditechVisitEMPIKey,
 target.Coid = TRIM(source.Coid),
 target.MeditechCOCID = TRIM(source.MeditechCOCID),
 target.EMPINumber = source.EMPINumber,
 target.AccountNumber = TRIM(source.AccountNumber),
 target.PatientAccountNumber = source.PatientAccountNumber,
 target.PatientMedicalRecord = TRIM(source.PatientMedicalRecord),
 target.PatientDwId = source.PatientDwId,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceLastUpdateDateTime = source.SourceLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (MeditechVisitEMPIKey, Coid, MeditechCOCID, EMPINumber, AccountNumber, PatientAccountNumber, PatientMedicalRecord, PatientDwId, SourceAPrimaryKeyValue, SourceLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.MeditechVisitEMPIKey, TRIM(source.Coid), TRIM(source.MeditechCOCID), source.EMPINumber, TRIM(source.AccountNumber), source.PatientAccountNumber, TRIM(source.PatientMedicalRecord), source.PatientDwId, TRIM(source.SourceAPrimaryKeyValue), source.SourceLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MeditechVisitEMPIKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechVisitEMPI
      GROUP BY MeditechVisitEMPIKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechVisitEMPI');
ELSE
  COMMIT TRANSACTION;
END IF;
