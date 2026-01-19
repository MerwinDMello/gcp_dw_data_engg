
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactConcuityCalcDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactConcuityCalcDetail AS source
ON target.ConcuityCalcDetailKey = source.ConcuityCalcDetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.ConcuityCalcDetailKey = source.ConcuityCalcDetailKey,
 target.LoadId = source.LoadId,
 target.LoadDetail = TRIM(source.LoadDetail),
 target.ActionTaken = TRIM(source.ActionTaken),
 target.ActionStatus = TRIM(source.ActionStatus),
 target.ActionDateKey = source.ActionDateKey,
 target.CalcInitRequestId = source.CalcInitRequestId,
 target.AccountNumber = TRIM(source.AccountNumber),
 target.UnitNumber = TRIM(source.UnitNumber),
 target.Practitioner = TRIM(source.Practitioner),
 target.PayerCode = TRIM(source.PayerCode),
 target.TotalExpectedPaymentAmt = source.TotalExpectedPaymentAmt,
 target.DetailType = TRIM(source.DetailType),
 target.Amount = source.Amount,
 target.Rate = source.Rate,
 target.ServiceDescription = TRIM(source.ServiceDescription),
 target.ServiceType = TRIM(source.ServiceType),
 target.CPTUnits = source.CPTUnits,
 target.AdditionalDescription = TRIM(source.AdditionalDescription),
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTStartServiceDateKey = source.CPTStartServiceDateKey,
 target.CPTEndServiceDateKey = source.CPTEndServiceDateKey,
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.Quantity = source.Quantity,
 target.CPTCharges = source.CPTCharges,
 target.BaseRate = source.BaseRate,
 target.InterestAmt = source.InterestAmt,
 target.StateTaxAmt = source.StateTaxAmt,
 target.ExpectedPaymentAmt = source.ExpectedPaymentAmt,
 target.Active = source.Active,
 target.ClaimKey = source.ClaimKey,
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ConcuityCalcDetailKey, LoadId, LoadDetail, ActionTaken, ActionStatus, ActionDateKey, CalcInitRequestId, AccountNumber, UnitNumber, Practitioner, PayerCode, TotalExpectedPaymentAmt, DetailType, Amount, Rate, ServiceDescription, ServiceType, CPTUnits, AdditionalDescription, CPTCode, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTModifier1, CPTModifier2, Quantity, CPTCharges, BaseRate, InterestAmt, StateTaxAmt, ExpectedPaymentAmt, Active, ClaimKey, ClaimLineChargeKey, Coid, RegionKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ConcuityCalcDetailKey, source.LoadId, TRIM(source.LoadDetail), TRIM(source.ActionTaken), TRIM(source.ActionStatus), source.ActionDateKey, source.CalcInitRequestId, TRIM(source.AccountNumber), TRIM(source.UnitNumber), TRIM(source.Practitioner), TRIM(source.PayerCode), source.TotalExpectedPaymentAmt, TRIM(source.DetailType), source.Amount, source.Rate, TRIM(source.ServiceDescription), TRIM(source.ServiceType), source.CPTUnits, TRIM(source.AdditionalDescription), TRIM(source.CPTCode), source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), source.Quantity, source.CPTCharges, source.BaseRate, source.InterestAmt, source.StateTaxAmt, source.ExpectedPaymentAmt, source.Active, source.ClaimKey, source.ClaimLineChargeKey, TRIM(source.Coid), source.RegionKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ConcuityCalcDetailKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactConcuityCalcDetail
      GROUP BY ConcuityCalcDetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactConcuityCalcDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
