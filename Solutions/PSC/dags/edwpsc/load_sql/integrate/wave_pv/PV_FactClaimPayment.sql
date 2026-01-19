
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimPayment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimPayment AS source
ON target.ClaimPaymentKey = source.ClaimPaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimPaymentKey = source.ClaimPaymentKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.EncounterKey = source.EncounterKey,
 target.IplanKey = source.IplanKey,
 target.PatientKey = source.PatientKey,
 target.PaymentTypeKey = source.PaymentTypeKey,
 target.ContractualAdjustmentAmt = source.ContractualAdjustmentAmt,
 target.AllowedAmt = source.AllowedAmt,
 target.CoInsuranceAmt = source.CoInsuranceAmt,
 target.MemberBalanceAmt = source.MemberBalanceAmt,
 target.PaymentAmt = source.PaymentAmt,
 target.DeductibleAmt = source.DeductibleAmt,
 target.WithHeldAmt = source.WithHeldAmt,
 target.MsgCode = TRIM(source.MsgCode),
 target.CreatedDateKey = source.CreatedDateKey,
 target.CreatedTime = source.CreatedTime,
 target.ModifiedDateKey = source.ModifiedDateKey,
 target.ModifiedTime = source.ModifiedTime,
 target.CheckAmt = source.CheckAmt,
 target.CheckNumber = TRIM(source.CheckNumber),
 target.CheckTypeDesc = TRIM(source.CheckTypeDesc),
 target.PostedDateKey = source.PostedDateKey,
 target.CheckDateKey = source.CheckDateKey,
 target.EOMB1 = TRIM(source.EOMB1),
 target.Memo = TRIM(source.Memo),
 target.DepositDateKey = source.DepositDateKey,
 target.PaymentFacilityKey = source.PaymentFacilityKey,
 target.UnpostedAmt = source.UnpostedAmt,
 target.BatchKey = source.BatchKey,
 target.DeletedFlag = source.DeletedFlag,
 target.ERADetailID = source.ERADetailID,
 target.ERAFlag = source.ERAFlag,
 target.ERAFileID = source.ERAFileID,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PaymentLock = source.PaymentLock,
 target.PaymentModifiedDateKey = source.PaymentModifiedDateKey,
 target.ERAClaimStatusCLP02 = source.ERAClaimStatusCLP02,
 target.PaymentComboID = TRIM(source.PaymentComboID),
 target.Practice = TRIM(source.Practice),
 target.PostedToUACAmt = source.PostedToUACAmt,
 target.PaymentDescription = TRIM(source.PaymentDescription),
 target.TreasuryBatchNumber = TRIM(source.TreasuryBatchNumber),
 target.TreasuryBatchDepositDate = source.TreasuryBatchDepositDate,
 target.TreasuryBatchPayerName = TRIM(source.TreasuryBatchPayerName),
 target.PaymentType = TRIM(source.PaymentType),
 target.CollectionStatus = TRIM(source.CollectionStatus),
 target.CollectionCodeChangeDate = TRIM(source.CollectionCodeChangeDate),
 target.CollectionCode = TRIM(source.CollectionCode),
 target.CollectionCodeChangeDateDATE = source.CollectionCodeChangeDateDATE,
 target.CollectionStatusAging = source.CollectionStatusAging
WHEN NOT MATCHED THEN
  INSERT (ClaimPaymentKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, EncounterKey, IplanKey, PatientKey, PaymentTypeKey, ContractualAdjustmentAmt, AllowedAmt, CoInsuranceAmt, MemberBalanceAmt, PaymentAmt, DeductibleAmt, WithHeldAmt, MsgCode, CreatedDateKey, CreatedTime, ModifiedDateKey, ModifiedTime, CheckAmt, CheckNumber, CheckTypeDesc, PostedDateKey, CheckDateKey, EOMB1, Memo, DepositDateKey, PaymentFacilityKey, UnpostedAmt, BatchKey, DeletedFlag, ERADetailID, ERAFlag, ERAFileID, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PaymentLock, PaymentModifiedDateKey, ERAClaimStatusCLP02, PaymentComboID, Practice, PostedToUACAmt, PaymentDescription, TreasuryBatchNumber, TreasuryBatchDepositDate, TreasuryBatchPayerName, PaymentType, CollectionStatus, CollectionCodeChangeDate, CollectionCode, CollectionCodeChangeDateDATE, CollectionStatusAging)
  VALUES (source.ClaimPaymentKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.EncounterKey, source.IplanKey, source.PatientKey, source.PaymentTypeKey, source.ContractualAdjustmentAmt, source.AllowedAmt, source.CoInsuranceAmt, source.MemberBalanceAmt, source.PaymentAmt, source.DeductibleAmt, source.WithHeldAmt, TRIM(source.MsgCode), source.CreatedDateKey, source.CreatedTime, source.ModifiedDateKey, source.ModifiedTime, source.CheckAmt, TRIM(source.CheckNumber), TRIM(source.CheckTypeDesc), source.PostedDateKey, source.CheckDateKey, TRIM(source.EOMB1), TRIM(source.Memo), source.DepositDateKey, source.PaymentFacilityKey, source.UnpostedAmt, source.BatchKey, source.DeletedFlag, source.ERADetailID, source.ERAFlag, source.ERAFileID, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PaymentLock, source.PaymentModifiedDateKey, source.ERAClaimStatusCLP02, TRIM(source.PaymentComboID), TRIM(source.Practice), source.PostedToUACAmt, TRIM(source.PaymentDescription), TRIM(source.TreasuryBatchNumber), source.TreasuryBatchDepositDate, TRIM(source.TreasuryBatchPayerName), TRIM(source.PaymentType), TRIM(source.CollectionStatus), TRIM(source.CollectionCodeChangeDate), TRIM(source.CollectionCode), source.CollectionCodeChangeDateDATE, source.CollectionStatusAging);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimPaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaimPayment
      GROUP BY ClaimPaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaimPayment');
ELSE
  COMMIT TRANSACTION;
END IF;
