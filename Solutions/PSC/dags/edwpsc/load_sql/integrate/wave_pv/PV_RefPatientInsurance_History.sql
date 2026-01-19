
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefPatientInsurance_History AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefPatientInsurance_History AS source
ON target.PatientInsuranceKey = source.PatientInsuranceKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.PatientInsuranceKey = source.PatientInsuranceKey,
 target.PatientKey = source.PatientKey,
 target.IplanKey = source.IplanKey,
 target.PatientInsuranceOrder = source.PatientInsuranceOrder,
 target.PatientInsuranceStartDate = TRIM(source.PatientInsuranceStartDate),
 target.PatientInsuranceEndDate = TRIM(source.PatientInsuranceEndDate),
 target.PatientInsuranceGroupNo = TRIM(source.PatientInsuranceGroupNo),
 target.PatientInsuranceGroupName = TRIM(source.PatientInsuranceGroupName),
 target.PatientInsuranceSubscriberNo = TRIM(source.PatientInsuranceSubscriberNo),
 target.PatientInsuranceCopay = TRIM(source.PatientInsuranceCopay),
 target.PatientInsuranceDeleteFlag = source.PatientInsuranceDeleteFlag,
 target.PatientInsuranceGuaranterPatientKey = source.PatientInsuranceGuaranterPatientKey,
 target.PatientInsuranceGuaranterRel = source.PatientInsuranceGuaranterRel,
 target.PatientInsuranceIsGuaranterPatient = source.PatientInsuranceIsGuaranterPatient,
 target.PatientInsuranceSequenceNo = source.PatientInsuranceSequenceNo,
 target.PatientInsuranceEncEligibilityStatus = TRIM(source.PatientInsuranceEncEligibilityStatus),
 target.PatientInsuranceEligibilityMessage = TRIM(source.PatientInsuranceEligibilityMessage),
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey,
 target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName),
 target.SourcePrimaryKey = TRIM(source.SourcePrimaryKey),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (PatientInsuranceKey, PatientKey, IplanKey, PatientInsuranceOrder, PatientInsuranceStartDate, PatientInsuranceEndDate, PatientInsuranceGroupNo, PatientInsuranceGroupName, PatientInsuranceSubscriberNo, PatientInsuranceCopay, PatientInsuranceDeleteFlag, PatientInsuranceGuaranterPatientKey, PatientInsuranceGuaranterRel, PatientInsuranceIsGuaranterPatient, PatientInsuranceSequenceNo, PatientInsuranceEncEligibilityStatus, PatientInsuranceEligibilityMessage, DeleteFlag, RegionKey, PracticeKey, PracticeName, SourcePrimaryKey, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SysStartTime, SysEndTime)
  VALUES (source.PatientInsuranceKey, source.PatientKey, source.IplanKey, source.PatientInsuranceOrder, TRIM(source.PatientInsuranceStartDate), TRIM(source.PatientInsuranceEndDate), TRIM(source.PatientInsuranceGroupNo), TRIM(source.PatientInsuranceGroupName), TRIM(source.PatientInsuranceSubscriberNo), TRIM(source.PatientInsuranceCopay), source.PatientInsuranceDeleteFlag, source.PatientInsuranceGuaranterPatientKey, source.PatientInsuranceGuaranterRel, source.PatientInsuranceIsGuaranterPatient, source.PatientInsuranceSequenceNo, TRIM(source.PatientInsuranceEncEligibilityStatus), TRIM(source.PatientInsuranceEligibilityMessage), source.DeleteFlag, source.RegionKey, source.PracticeKey, TRIM(source.PracticeName), TRIM(source.SourcePrimaryKey), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatientInsuranceKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefPatientInsurance_History
      GROUP BY PatientInsuranceKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefPatientInsurance_History');
ELSE
  COMMIT TRANSACTION;
END IF;
