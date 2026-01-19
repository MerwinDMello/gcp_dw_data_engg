
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechORAuth AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactMeditechORAuth AS source
ON target.MeditechORAuthKey = source.MeditechORAuthKey
WHEN MATCHED THEN
  UPDATE SET
  target.MeditechORAuthKey = source.MeditechORAuthKey,
 target.MeditechCOCID = TRIM(source.MeditechCOCID),
 target.AccountNumber = TRIM(source.AccountNumber),
 target.ProcedureDate = source.ProcedureDate,
 target.ProcedureTime = source.ProcedureTime,
 target.CptCode = TRIM(source.CptCode),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.PatientSex = TRIM(source.PatientSex),
 target.PatientDOB = source.PatientDOB,
 target.PatientTypeCode = TRIM(source.PatientTypeCode),
 target.SurgeonNPI = TRIM(source.SurgeonNPI),
 target.SurgeonName = TRIM(source.SurgeonName),
 target.PrimaryInsurance = TRIM(source.PrimaryInsurance),
 target.PrimaryInsAuthNumber = TRIM(source.PrimaryInsAuthNumber),
 target.SecondarInsAuthNumber = TRIM(source.SecondarInsAuthNumber),
 target.FinancialClass = TRIM(source.FinancialClass),
 target.FinancialClassDesc = TRIM(source.FinancialClassDesc),
 target.ProcedureEstTime = TRIM(source.ProcedureEstTime),
 target.AppointmentORType = TRIM(source.AppointmentORType),
 target.PTLocation = TRIM(source.PTLocation),
 target.AppointmentStatus = TRIM(source.AppointmentStatus),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.AdmitOrderResponseText = TRIM(source.AdmitOrderResponseText),
 target.COID = TRIM(source.COID),
 target.FacilityName = TRIM(source.FacilityName)
WHEN NOT MATCHED THEN
  INSERT (MeditechORAuthKey, MeditechCOCID, AccountNumber, ProcedureDate, ProcedureTime, CptCode, PatientLastName, PatientFirstName, PatientMiddleName, PatientSex, PatientDOB, PatientTypeCode, SurgeonNPI, SurgeonName, PrimaryInsurance, PrimaryInsAuthNumber, SecondarInsAuthNumber, FinancialClass, FinancialClassDesc, ProcedureEstTime, AppointmentORType, PTLocation, AppointmentStatus, DWLastUpdateDateTime, SourceSystemCode, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, AdmitOrderResponseText, COID, FacilityName)
  VALUES (source.MeditechORAuthKey, TRIM(source.MeditechCOCID), TRIM(source.AccountNumber), source.ProcedureDate, source.ProcedureTime, TRIM(source.CptCode), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.PatientMiddleName), TRIM(source.PatientSex), source.PatientDOB, TRIM(source.PatientTypeCode), TRIM(source.SurgeonNPI), TRIM(source.SurgeonName), TRIM(source.PrimaryInsurance), TRIM(source.PrimaryInsAuthNumber), TRIM(source.SecondarInsAuthNumber), TRIM(source.FinancialClass), TRIM(source.FinancialClassDesc), TRIM(source.ProcedureEstTime), TRIM(source.AppointmentORType), TRIM(source.PTLocation), TRIM(source.AppointmentStatus), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.SourceAPrimaryKeyValue), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.AdmitOrderResponseText), TRIM(source.COID), TRIM(source.FacilityName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MeditechORAuthKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechORAuth
      GROUP BY MeditechORAuthKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactMeditechORAuth');
ELSE
  COMMIT TRANSACTION;
END IF;
