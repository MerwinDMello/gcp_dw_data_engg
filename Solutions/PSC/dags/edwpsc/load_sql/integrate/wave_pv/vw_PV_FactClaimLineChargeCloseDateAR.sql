
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.vw_PV_FactClaimLineChargeCloseDateAR AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimLineChargeCloseDateAR AS source
ON target.ClaimLineChargeKey = source.ClaimLineChargeKey AND target.ClosingDateKey = source.ClosingDateKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.COID = TRIM(source.COID),
 target.ClosingDateKey = source.ClosingDateKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimType = TRIM(source.ClaimType),
 target.Payer1IplanKey = source.Payer1IplanKey,
 target.Payer1IplanID = source.Payer1IplanID,
 target.Payer1FinancialClassKey = source.Payer1FinancialClassKey,
 target.LiabilityIplanKey = TRIM(source.LiabilityIplanKey),
 target.LiabilityIplanID = source.LiabilityIplanID,
 target.LiabilityFinancialClassKey = source.LiabilityFinancialClassKey,
 target.FacilityKey = source.FacilityKey,
 target.Practice = TRIM(source.Practice),
 target.CPTStartServiceDateKey = source.CPTStartServiceDateKey,
 target.CPTEndServiceDateKey = source.CPTEndServiceDateKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTPOSKey = source.CPTPOSKey,
 target.CPTOrder = source.CPTOrder,
 target.ChargesAmt = source.ChargesAmt,
 target.PatBalanceAmt = source.PatBalanceAmt,
 target.InsBalanceAmt = source.InsBalanceAmt,
 target.BalanceAmt = source.BalanceAmt,
 target.AdjustmentAmt = source.AdjustmentAmt,
 target.PaymentAmt = source.PaymentAmt,
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.CPTModifier3 = TRIM(source.CPTModifier3),
 target.RebillFlag = TRIM(source.RebillFlag),
 target.CrgDetailPK = TRIM(source.CrgDetailPK),
 target.CrgHeaderPK = TRIM(source.CrgHeaderPK),
 target.ARCtrlNum = source.ARCtrlNum,
 target.ARHeaderPK = TRIM(source.ARHeaderPK),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimLineChargeKey, COID, ClosingDateKey, ClaimKey, ClaimNumber, RegionKey, CoidConfigurationKey, ServicingProviderKey, ClaimType, Payer1IplanKey, Payer1IplanID, Payer1FinancialClassKey, LiabilityIplanKey, LiabilityIplanID, LiabilityFinancialClassKey, FacilityKey, Practice, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTCode, CPTCodeKey, CPTPOSKey, CPTOrder, ChargesAmt, PatBalanceAmt, InsBalanceAmt, BalanceAmt, AdjustmentAmt, PaymentAmt, CPTModifier1, CPTModifier2, CPTModifier3, RebillFlag, CrgDetailPK, CrgHeaderPK, ARCtrlNum, ARHeaderPK, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM)
  VALUES (source.ClaimLineChargeKey, TRIM(source.COID), source.ClosingDateKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, source.CoidConfigurationKey, source.ServicingProviderKey, TRIM(source.ClaimType), source.Payer1IplanKey, source.Payer1IplanID, source.Payer1FinancialClassKey, TRIM(source.LiabilityIplanKey), source.LiabilityIplanID, source.LiabilityFinancialClassKey, source.FacilityKey, TRIM(source.Practice), source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, TRIM(source.CPTCode), source.CPTCodeKey, source.CPTPOSKey, source.CPTOrder, source.ChargesAmt, source.PatBalanceAmt, source.InsBalanceAmt, source.BalanceAmt, source.AdjustmentAmt, source.PaymentAmt, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), TRIM(source.CPTModifier3), TRIM(source.RebillFlag), TRIM(source.CrgDetailPK), TRIM(source.CrgHeaderPK), source.ARCtrlNum, TRIM(source.ARHeaderPK), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLineChargeKey, ClosingDateKey
      FROM {{ params.param_psc_core_dataset_name }}.vw_PV_FactClaimLineChargeCloseDateAR
      GROUP BY ClaimLineChargeKey, ClosingDateKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.vw_PV_FactClaimLineChargeCloseDateAR');
ELSE
  COMMIT TRANSACTION;
END IF;
