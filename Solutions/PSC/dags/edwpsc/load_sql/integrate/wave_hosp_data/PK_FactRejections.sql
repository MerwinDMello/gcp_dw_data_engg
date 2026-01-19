
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactRejections AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactRejections AS source
ON target.PKRejectionsKey = source.PKRejectionsKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKRejectionsKey = source.PKRejectionsKey,
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.PKRegionName = TRIM(source.PKRegionName),
 target.MessageID = source.MessageID,
 target.ControlID = TRIM(source.ControlID),
 target.SendingFacility = TRIM(source.SendingFacility),
 target.SiteCode = TRIM(source.SiteCode),
 target.PatientNumber = TRIM(source.PatientNumber),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.ErrorCreateDatetime = source.ErrorCreateDatetime,
 target.ErrorType = TRIM(source.ErrorType),
 target.Error = TRIM(source.Error),
 target.DFT_AttendingProviderNPI = source.DFT_AttendingProviderNPI,
 target.DFT_AttendingProviderLastName = TRIM(source.DFT_AttendingProviderLastName),
 target.DFT_AttendingProviderFirstName = TRIM(source.DFT_AttendingProviderFirstName),
 target.DFT_PerformedByProviderLastName = TRIM(source.DFT_PerformedByProviderLastName),
 target.DFT_PerformedByProviderFirstName = TRIM(source.DFT_PerformedByProviderFirstName),
 target.DFT_AdmitDatetime = source.DFT_AdmitDatetime,
 target.DFT_DischargeDatetime = source.DFT_DischargeDatetime,
 target.DFT_ServiceDate = source.DFT_ServiceDate,
 target.DFT_PatientMRN = TRIM(source.DFT_PatientMRN),
 target.DFT_PatientDOB = source.DFT_PatientDOB,
 target.DFT_FullMessage = TRIM(source.DFT_FullMessage),
 target.DFT_ChargeID = TRIM(source.DFT_ChargeID),
 target.ResolvedFlag = source.ResolvedFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (PKRejectionsKey, PKFinancialNumber, PKRegionName, MessageID, ControlID, SendingFacility, SiteCode, PatientNumber, PatientLastName, PatientFirstName, ErrorCreateDatetime, ErrorType, Error, DFT_AttendingProviderNPI, DFT_AttendingProviderLastName, DFT_AttendingProviderFirstName, DFT_PerformedByProviderLastName, DFT_PerformedByProviderFirstName, DFT_AdmitDatetime, DFT_DischargeDatetime, DFT_ServiceDate, DFT_PatientMRN, DFT_PatientDOB, DFT_FullMessage, DFT_ChargeID, ResolvedFlag, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PKRejectionsKey, TRIM(source.PKFinancialNumber), TRIM(source.PKRegionName), source.MessageID, TRIM(source.ControlID), TRIM(source.SendingFacility), TRIM(source.SiteCode), TRIM(source.PatientNumber), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), source.ErrorCreateDatetime, TRIM(source.ErrorType), TRIM(source.Error), source.DFT_AttendingProviderNPI, TRIM(source.DFT_AttendingProviderLastName), TRIM(source.DFT_AttendingProviderFirstName), TRIM(source.DFT_PerformedByProviderLastName), TRIM(source.DFT_PerformedByProviderFirstName), source.DFT_AdmitDatetime, source.DFT_DischargeDatetime, source.DFT_ServiceDate, TRIM(source.DFT_PatientMRN), source.DFT_PatientDOB, TRIM(source.DFT_FullMessage), TRIM(source.DFT_ChargeID), source.ResolvedFlag, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKRejectionsKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactRejections
      GROUP BY PKRejectionsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactRejections');
ELSE
  COMMIT TRANSACTION;
END IF;
