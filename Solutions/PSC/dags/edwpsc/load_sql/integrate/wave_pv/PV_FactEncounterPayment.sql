
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactEncounterPayment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactEncounterPayment AS source
ON target.EncounterPaymentKey = source.EncounterPaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterPaymentKey = source.EncounterPaymentKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.Clinic = TRIM(source.Clinic),
 target.Practice = TRIM(source.Practice),
 target.PracticeKey = source.PracticeKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.EncounterKey = source.EncounterKey,
 target.PatientKey = source.PatientKey,
 target.PatientNumber = source.PatientNumber,
 target.PaymentCategory = TRIM(source.PaymentCategory),
 target.PaymentAmt = source.PaymentAmt,
 target.PaymentType = TRIM(source.PaymentType),
 target.ClaimDateKey = source.ClaimDateKey,
 target.VisitStatus = TRIM(source.VisitStatus),
 target.PaymentCreateDateKey = source.PaymentCreateDateKey,
 target.CreatedByUserId = TRIM(source.CreatedByUserId),
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.ClaimCreatedFlag = TRIM(source.ClaimCreatedFlag),
 target.ClaimBilledFlag = TRIM(source.ClaimBilledFlag),
 target.RebillFlag = TRIM(source.RebillFlag),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterPaymentKey, RegionKey, Coid, Clinic, Practice, PracticeKey, ServiceDateKey, ClaimKey, ClaimNumber, EncounterKey, PatientKey, PatientNumber, PaymentCategory, PaymentAmt, PaymentType, ClaimDateKey, VisitStatus, PaymentCreateDateKey, CreatedByUserId, CreatedByUserKey, ClaimCreatedFlag, ClaimBilledFlag, RebillFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterPaymentKey, source.RegionKey, TRIM(source.Coid), TRIM(source.Clinic), TRIM(source.Practice), source.PracticeKey, source.ServiceDateKey, source.ClaimKey, source.ClaimNumber, source.EncounterKey, source.PatientKey, source.PatientNumber, TRIM(source.PaymentCategory), source.PaymentAmt, TRIM(source.PaymentType), source.ClaimDateKey, TRIM(source.VisitStatus), source.PaymentCreateDateKey, TRIM(source.CreatedByUserId), source.CreatedByUserKey, TRIM(source.ClaimCreatedFlag), TRIM(source.ClaimBilledFlag), TRIM(source.RebillFlag), TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterPaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactEncounterPayment
      GROUP BY EncounterPaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactEncounterPayment');
ELSE
  COMMIT TRANSACTION;
END IF;
