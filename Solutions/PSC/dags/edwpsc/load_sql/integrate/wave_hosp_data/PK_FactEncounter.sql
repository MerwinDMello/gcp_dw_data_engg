
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactEncounter AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactEncounter AS source
ON target.PKEncounterKey = source.PKEncounterKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKEncounterKey = source.PKEncounterKey,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.SOURCE = TRIM(source.SOURCE),
 target.VisitLocationUnit = TRIM(source.VisitLocationUnit),
 target.VisitLocationRoom = TRIM(source.VisitLocationRoom),
 target.VisitLocationFacility = TRIM(source.VisitLocationFacility),
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.AppointmentAdmitDate = source.AppointmentAdmitDate,
 target.DischargeDate = source.DischargeDate,
 target.CreatedDate = source.CreatedDate,
 target.DeleteFlag = source.DeleteFlag,
 target.Status = TRIM(source.Status),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.EncounterPOS = TRIM(source.EncounterPOS),
 target.Coid = TRIM(source.Coid),
 target.AttendingProviderLastName = TRIM(source.AttendingProviderLastName),
 target.AttendingProviderFirstName = TRIM(source.AttendingProviderFirstName),
 target.AttendingProviderNPI = TRIM(source.AttendingProviderNPI),
 target.ERProviderLastName = TRIM(source.ERProviderLastName),
 target.ERProviderFirstName = TRIM(source.ERProviderFirstName),
 target.ERProviderNPI = TRIM(source.ERProviderNPI),
 target.AccountBasedEncounterId = source.AccountBasedEncounterId,
 target.PracticeId = TRIM(source.PracticeId),
 target.PosType = TRIM(source.PosType),
 target.PrimaryInsuranceName = TRIM(source.PrimaryInsuranceName),
 target.SecondaryInsuranceName = TRIM(source.SecondaryInsuranceName)
WHEN NOT MATCHED THEN
  INSERT (PKEncounterKey, PKRegionName, PatientLastName, PatientFirstName, PatientMiddleName, PatientMRN, SOURCE, VisitLocationUnit, VisitLocationRoom, VisitLocationFacility, PKFinancialNumber, AppointmentAdmitDate, DischargeDate, CreatedDate, DeleteFlag, Status, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, EncounterPOS, Coid, AttendingProviderLastName, AttendingProviderFirstName, AttendingProviderNPI, ERProviderLastName, ERProviderFirstName, ERProviderNPI, AccountBasedEncounterId, PracticeId, PosType, PrimaryInsuranceName, SecondaryInsuranceName)
  VALUES (source.PKEncounterKey, TRIM(source.PKRegionName), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.PatientMiddleName), TRIM(source.PatientMRN), TRIM(source.SOURCE), TRIM(source.VisitLocationUnit), TRIM(source.VisitLocationRoom), TRIM(source.VisitLocationFacility), TRIM(source.PKFinancialNumber), source.AppointmentAdmitDate, source.DischargeDate, source.CreatedDate, source.DeleteFlag, TRIM(source.Status), source.SourceAPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.EncounterPOS), TRIM(source.Coid), TRIM(source.AttendingProviderLastName), TRIM(source.AttendingProviderFirstName), TRIM(source.AttendingProviderNPI), TRIM(source.ERProviderLastName), TRIM(source.ERProviderFirstName), TRIM(source.ERProviderNPI), source.AccountBasedEncounterId, TRIM(source.PracticeId), TRIM(source.PosType), TRIM(source.PrimaryInsuranceName), TRIM(source.SecondaryInsuranceName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKEncounterKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactEncounter
      GROUP BY PKEncounterKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactEncounter');
ELSE
  COMMIT TRANSACTION;
END IF;
