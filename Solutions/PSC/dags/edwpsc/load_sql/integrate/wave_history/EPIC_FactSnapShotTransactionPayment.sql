
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotTransactionPayment AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotTransactionPayment AS source
ON target.SnapShotDate = source.SnapShotDate AND target.TransactionPaymentKey = source.TransactionPaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionPaymentKey = source.TransactionPaymentKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.RegionKey = source.RegionKey,
 target.PracticeKey = source.PracticeKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.Coid = TRIM(source.Coid),
 target.GLDepartment = TRIM(source.GLDepartment),
 target.Claimkey = source.Claimkey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.PatientKey = source.PatientKey,
 target.PatientID = source.PatientID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = TRIM(source.ServicingProviderID),
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = TRIM(source.RenderingProviderID),
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ClaimDateMonthID = source.ClaimDateMonthID,
 target.ServiceDateKey = source.ServiceDateKey,
 target.ServiceDateMonthID = source.ServiceDateMonthID,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.FinancialClassKey = source.FinancialClassKey,
 target.PaymentTypeKey = source.PaymentTypeKey,
 target.CheckDateKey = source.CheckDateKey,
 target.CheckType = TRIM(source.CheckType),
 target.EncounterID = TRIM(source.EncounterID),
 target.EncounterKey = source.EncounterKey,
 target.PaymentDetailID = source.PaymentDetailID,
 target.PaymentID = TRIM(source.PaymentID),
 target.CheckAmt = source.CheckAmt,
 target.PostedDateKey = source.PostedDateKey,
 target.TransactionType = TRIM(source.TransactionType),
 target.TransactionTypeDesc = TRIM(source.TransactionTypeDesc),
 target.TransactionID = TRIM(source.TransactionID),
 target.TransactionAmt = source.TransactionAmt,
 target.TransactionDateKey = source.TransactionDateKey,
 target.TransactionDateMonthID = source.TransactionDateMonthID,
 target.PaymentFromIplanKey = source.PaymentFromIplanKey,
 target.PaymentFromIplanID = TRIM(source.PaymentFromIplanID),
 target.CheckNumber = TRIM(source.CheckNumber),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.TransactionSameMonthFlag = TRIM(source.TransactionSameMonthFlag)
WHEN NOT MATCHED THEN
  INSERT (TransactionPaymentKey, MonthId, SnapShotDate, RegionKey, PracticeKey, PracticeID, Coid, GLDepartment, Claimkey, ClaimNumber, VisitNumber, TransactionNumber, PatientKey, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ClaimDateMonthID, ServiceDateKey, ServiceDateMonthID, Iplan1IplanKey, Iplan1ID, FinancialClassKey, PaymentTypeKey, CheckDateKey, CheckType, EncounterID, EncounterKey, PaymentDetailID, PaymentID, CheckAmt, PostedDateKey, TransactionType, TransactionTypeDesc, TransactionID, TransactionAmt, TransactionDateKey, TransactionDateMonthID, PaymentFromIplanKey, PaymentFromIplanID, CheckNumber, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, TransactionSameMonthFlag)
  VALUES (source.TransactionPaymentKey, source.MonthId, source.SnapShotDate, source.RegionKey, source.PracticeKey, TRIM(source.PracticeID), TRIM(source.Coid), TRIM(source.GLDepartment), source.Claimkey, source.ClaimNumber, source.VisitNumber, source.TransactionNumber, source.PatientKey, source.PatientID, source.ServicingProviderKey, TRIM(source.ServicingProviderID), source.RenderingProviderKey, TRIM(source.RenderingProviderID), source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ClaimDateMonthID, source.ServiceDateKey, source.ServiceDateMonthID, source.Iplan1IplanKey, TRIM(source.Iplan1ID), source.FinancialClassKey, source.PaymentTypeKey, source.CheckDateKey, TRIM(source.CheckType), TRIM(source.EncounterID), source.EncounterKey, source.PaymentDetailID, TRIM(source.PaymentID), source.CheckAmt, source.PostedDateKey, TRIM(source.TransactionType), TRIM(source.TransactionTypeDesc), TRIM(source.TransactionID), source.TransactionAmt, source.TransactionDateKey, source.TransactionDateMonthID, source.PaymentFromIplanKey, TRIM(source.PaymentFromIplanID), TRIM(source.CheckNumber), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.TransactionSameMonthFlag));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, TransactionPaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotTransactionPayment
      GROUP BY SnapShotDate, TransactionPaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotTransactionPayment');
ELSE
  COMMIT TRANSACTION;
END IF;
