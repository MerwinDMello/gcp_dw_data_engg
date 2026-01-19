
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotInitialDenials AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotInitialDenials AS source
ON target.SnapShotDate = source.SnapShotDate AND target.claimkey = source.claimkey
WHEN MATCHED THEN
  UPDATE SET
  target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.coid = TRIM(source.coid),
 target.claimnumber = source.claimnumber,
 target.claimkey = source.claimkey,
 target.regionkey = source.regionkey,
 target.servicingproviderkey = source.servicingproviderkey,
 target.renderingproviderkey = source.renderingproviderkey,
 target.patientkey = source.patientkey,
 target.claimstatuskey = source.claimstatuskey,
 target.ClaimServiceDate = source.ClaimServiceDate,
 target.ClaimServiceDateYear = source.ClaimServiceDateYear,
 target.ClaimServiceDateMonth = source.ClaimServiceDateMonth,
 target.ClaimBalance = source.ClaimBalance,
 target.ClaimCharge = source.ClaimCharge,
 target.VoidFlag = source.VoidFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.ARClaimFlag = source.ARClaimFlag,
 target.facilitykey = source.facilitykey,
 target.firstinsurancebilldatekey = source.firstinsurancebilldatekey,
 target.lastbilldatekey = source.lastbilldatekey,
 target.numberofbills = source.numberofbills,
 target.LiabilityDate = source.LiabilityDate,
 target.ClaimDate = source.ClaimDate,
 target.LiabilityOwner = TRIM(source.LiabilityOwner),
 target.RHHoldCode = TRIM(source.RHHoldCode),
 target.claimlinechargekey = source.claimlinechargekey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTServiceStartDate = source.CPTServiceStartDate,
 target.CPTServiceEndDate = source.CPTServiceEndDate,
 target.cptdeleteflag = source.cptdeleteflag,
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.CPTModifier3 = TRIM(source.CPTModifier3),
 target.CPTModifier4 = TRIM(source.CPTModifier4),
 target.LineCharge = source.LineCharge,
 target.LineChargeFinal = source.LineChargeFinal,
 target.LineInsPayment = source.LineInsPayment,
 target.LineInsPaymentFirstDate = source.LineInsPaymentFirstDate,
 target.LineInsPaymentFlag = source.LineInsPaymentFlag,
 target.LinePatPayment = source.LinePatPayment,
 target.LinePatPaymentFlag = source.LinePatPaymentFlag,
 target.LineInsContractual = source.LineInsContractual,
 target.LineInsContractualFirstDate = source.LineInsContractualFirstDate,
 target.LineInsContractualFlag = source.LineInsContractualFlag,
 target.LineAdjustment = source.LineAdjustment,
 target.LineAdjustmentFlag = source.LineAdjustmentFlag,
 target.LineBalance = source.LineBalance,
 target.LineInBalanceFlag = source.LineInBalanceFlag,
 target.LineAdminAdj = source.LineAdminAdj,
 target.LineDenialAdj = source.LineDenialAdj,
 target.LineCharityAdj = source.LineCharityAdj,
 target.LineContractualAdj = source.LineContractualAdj,
 target.LineUninsuredDiscountAdj = source.LineUninsuredDiscountAdj,
 target.LineRefundAdj = source.LineRefundAdj,
 target.LineNSFAdj = source.LineNSFAdj,
 target.LineEscheatAdj = source.LineEscheatAdj,
 target.LineBadDebtAdj = source.LineBadDebtAdj,
 target.LineOtherAdj = source.LineOtherAdj,
 target.VoidDate = source.VoidDate,
 target.PriorClaimKey = source.PriorClaimKey,
 target.VoidFirstBilledClaimKey = source.VoidFirstBilledClaimKey,
 target.VoidFirstBilledClaimNumber = source.VoidFirstBilledClaimNumber,
 target.VoidFirstBilledDateKey = source.VoidFirstBilledDateKey,
 target.OnFirstBilledFlag = source.OnFirstBilledFlag,
 target.OnFirstBilledCharge = source.OnFirstBilledCharge,
 target.VoidFirstBilledClaimLineChargeKey = source.VoidFirstBilledClaimLineChargeKey,
 target.FirstDenialCarcRarcCode = TRIM(source.FirstDenialCarcRarcCode),
 target.FirstDenialERADate = source.FirstDenialERADate,
 target.FirstDenialCARCCodes = TRIM(source.FirstDenialCARCCodes),
 target.FirstDenialRARCCodes = TRIM(source.FirstDenialRARCCodes),
 target.FirstDenialPayerName = TRIM(source.FirstDenialPayerName),
 target.FirstDenialCategory = TRIM(source.FirstDenialCategory),
 target.InitialDeniedCharges = source.InitialDeniedCharges,
 target.FirstDenialFlag = source.FirstDenialFlag,
 target.LineDenialAdjFlag = source.LineDenialAdjFlag,
 target.FirstDenialCarcRarcType = TRIM(source.FirstDenialCarcRarcType),
 target.FirstDenialCARCIsDenial = TRIM(source.FirstDenialCARCIsDenial),
 target.FirstDenialCARCRequiresRarc = TRIM(source.FirstDenialCARCRequiresRarc),
 target.FirstDenialRARCIsDenial = TRIM(source.FirstDenialRARCIsDenial),
 target.FirstERACarcRarcCode = TRIM(source.FirstERACarcRarcCode),
 target.FirstERADate = source.FirstERADate,
 target.FirstERACARCCodes = TRIM(source.FirstERACARCCodes),
 target.FirstERARARCCodes = TRIM(source.FirstERARARCCodes),
 target.FirstERAPayerName = TRIM(source.FirstERAPayerName),
 target.FirstERACategory = TRIM(source.FirstERACategory),
 target.FirstERACarcRarcType = TRIM(source.FirstERACarcRarcType),
 target.FirstERACARCIsDenial = TRIM(source.FirstERACARCIsDenial),
 target.FirstERACARCRequiresRarc = TRIM(source.FirstERACARCRequiresRarc),
 target.FirstERARARCIsDenial = TRIM(source.FirstERARARCIsDenial),
 target.ArtivaPool = TRIM(source.ArtivaPool),
 target.ArtivaLiabilityLastReviewed = source.ArtivaLiabilityLastReviewed,
 target.ArtivaLiabilityFollowupDate = source.ArtivaLiabilityFollowupDate,
 target.LastAdjustmentCode = TRIM(source.LastAdjustmentCode),
 target.LastAdjustmentName = TRIM(source.LastAdjustmentName),
 target.LastAdjustmentCategory = TRIM(source.LastAdjustmentCategory),
 target.LastAdjustmentDate = source.LastAdjustmentDate,
 target.LineAdminAdjLastDate = source.LineAdminAdjLastDate,
 target.LineDenialAdjLastDate = source.LineDenialAdjLastDate,
 target.LineCharityAdjLastDate = source.LineCharityAdjLastDate,
 target.LineContractualAdjLastDate = source.LineContractualAdjLastDate,
 target.LineUninsuredDiscountAdjLastDate = source.LineUninsuredDiscountAdjLastDate,
 target.LineRefundAdjLastDate = source.LineRefundAdjLastDate,
 target.LineNSFAdjLastDate = source.LineNSFAdjLastDate,
 target.LineEscheatAdjLastDate = source.LineEscheatAdjLastDate,
 target.LineBadDebtAdjLastDate = source.LineBadDebtAdjLastDate,
 target.LineOtherAdjLastDate = source.LineOtherAdjLastDate,
 target.IETFlag = source.IETFlag,
 target.IETCreateDate = source.IETCreateDate,
 target.IETLastModifiedDate = source.IETLastModifiedDate,
 target.DenialCount = source.DenialCount,
 target.DenialLastEraDate = source.DenialLastEraDate,
 target.InitialIplanKey = source.InitialIplanKey
WHEN NOT MATCHED THEN
  INSERT (MonthID, SnapShotDate, coid, claimnumber, claimkey, regionkey, servicingproviderkey, renderingproviderkey, patientkey, claimstatuskey, ClaimServiceDate, ClaimServiceDateYear, ClaimServiceDateMonth, ClaimBalance, ClaimCharge, VoidFlag, DeleteFlag, ARClaimFlag, facilitykey, firstinsurancebilldatekey, lastbilldatekey, numberofbills, LiabilityDate, ClaimDate, LiabilityOwner, RHHoldCode, claimlinechargekey, CPTCodeKey, CPTCode, CPTServiceStartDate, CPTServiceEndDate, cptdeleteflag, CPTModifier1, CPTModifier2, CPTModifier3, CPTModifier4, LineCharge, LineChargeFinal, LineInsPayment, LineInsPaymentFirstDate, LineInsPaymentFlag, LinePatPayment, LinePatPaymentFlag, LineInsContractual, LineInsContractualFirstDate, LineInsContractualFlag, LineAdjustment, LineAdjustmentFlag, LineBalance, LineInBalanceFlag, LineAdminAdj, LineDenialAdj, LineCharityAdj, LineContractualAdj, LineUninsuredDiscountAdj, LineRefundAdj, LineNSFAdj, LineEscheatAdj, LineBadDebtAdj, LineOtherAdj, VoidDate, PriorClaimKey, VoidFirstBilledClaimKey, VoidFirstBilledClaimNumber, VoidFirstBilledDateKey, OnFirstBilledFlag, OnFirstBilledCharge, VoidFirstBilledClaimLineChargeKey, FirstDenialCarcRarcCode, FirstDenialERADate, FirstDenialCARCCodes, FirstDenialRARCCodes, FirstDenialPayerName, FirstDenialCategory, InitialDeniedCharges, FirstDenialFlag, LineDenialAdjFlag, FirstDenialCarcRarcType, FirstDenialCARCIsDenial, FirstDenialCARCRequiresRarc, FirstDenialRARCIsDenial, FirstERACarcRarcCode, FirstERADate, FirstERACARCCodes, FirstERARARCCodes, FirstERAPayerName, FirstERACategory, FirstERACarcRarcType, FirstERACARCIsDenial, FirstERACARCRequiresRarc, FirstERARARCIsDenial, ArtivaPool, ArtivaLiabilityLastReviewed, ArtivaLiabilityFollowupDate, LastAdjustmentCode, LastAdjustmentName, LastAdjustmentCategory, LastAdjustmentDate, LineAdminAdjLastDate, LineDenialAdjLastDate, LineCharityAdjLastDate, LineContractualAdjLastDate, LineUninsuredDiscountAdjLastDate, LineRefundAdjLastDate, LineNSFAdjLastDate, LineEscheatAdjLastDate, LineBadDebtAdjLastDate, LineOtherAdjLastDate, IETFlag, IETCreateDate, IETLastModifiedDate, DenialCount, DenialLastEraDate, InitialIplanKey)
  VALUES (source.MonthID, source.SnapShotDate, TRIM(source.coid), source.claimnumber, source.claimkey, source.regionkey, source.servicingproviderkey, source.renderingproviderkey, source.patientkey, source.claimstatuskey, source.ClaimServiceDate, source.ClaimServiceDateYear, source.ClaimServiceDateMonth, source.ClaimBalance, source.ClaimCharge, source.VoidFlag, source.DeleteFlag, source.ARClaimFlag, source.facilitykey, source.firstinsurancebilldatekey, source.lastbilldatekey, source.numberofbills, source.LiabilityDate, source.ClaimDate, TRIM(source.LiabilityOwner), TRIM(source.RHHoldCode), source.claimlinechargekey, source.CPTCodeKey, TRIM(source.CPTCode), source.CPTServiceStartDate, source.CPTServiceEndDate, source.cptdeleteflag, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), TRIM(source.CPTModifier3), TRIM(source.CPTModifier4), source.LineCharge, source.LineChargeFinal, source.LineInsPayment, source.LineInsPaymentFirstDate, source.LineInsPaymentFlag, source.LinePatPayment, source.LinePatPaymentFlag, source.LineInsContractual, source.LineInsContractualFirstDate, source.LineInsContractualFlag, source.LineAdjustment, source.LineAdjustmentFlag, source.LineBalance, source.LineInBalanceFlag, source.LineAdminAdj, source.LineDenialAdj, source.LineCharityAdj, source.LineContractualAdj, source.LineUninsuredDiscountAdj, source.LineRefundAdj, source.LineNSFAdj, source.LineEscheatAdj, source.LineBadDebtAdj, source.LineOtherAdj, source.VoidDate, source.PriorClaimKey, source.VoidFirstBilledClaimKey, source.VoidFirstBilledClaimNumber, source.VoidFirstBilledDateKey, source.OnFirstBilledFlag, source.OnFirstBilledCharge, source.VoidFirstBilledClaimLineChargeKey, TRIM(source.FirstDenialCarcRarcCode), source.FirstDenialERADate, TRIM(source.FirstDenialCARCCodes), TRIM(source.FirstDenialRARCCodes), TRIM(source.FirstDenialPayerName), TRIM(source.FirstDenialCategory), source.InitialDeniedCharges, source.FirstDenialFlag, source.LineDenialAdjFlag, TRIM(source.FirstDenialCarcRarcType), TRIM(source.FirstDenialCARCIsDenial), TRIM(source.FirstDenialCARCRequiresRarc), TRIM(source.FirstDenialRARCIsDenial), TRIM(source.FirstERACarcRarcCode), source.FirstERADate, TRIM(source.FirstERACARCCodes), TRIM(source.FirstERARARCCodes), TRIM(source.FirstERAPayerName), TRIM(source.FirstERACategory), TRIM(source.FirstERACarcRarcType), TRIM(source.FirstERACARCIsDenial), TRIM(source.FirstERACARCRequiresRarc), TRIM(source.FirstERARARCIsDenial), TRIM(source.ArtivaPool), source.ArtivaLiabilityLastReviewed, source.ArtivaLiabilityFollowupDate, TRIM(source.LastAdjustmentCode), TRIM(source.LastAdjustmentName), TRIM(source.LastAdjustmentCategory), source.LastAdjustmentDate, source.LineAdminAdjLastDate, source.LineDenialAdjLastDate, source.LineCharityAdjLastDate, source.LineContractualAdjLastDate, source.LineUninsuredDiscountAdjLastDate, source.LineRefundAdjLastDate, source.LineNSFAdjLastDate, source.LineEscheatAdjLastDate, source.LineBadDebtAdjLastDate, source.LineOtherAdjLastDate, source.IETFlag, source.IETCreateDate, source.IETLastModifiedDate, source.DenialCount, source.DenialLastEraDate, source.InitialIplanKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, claimkey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotInitialDenials
      GROUP BY SnapShotDate, claimkey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotInitialDenials');
ELSE
  COMMIT TRANSACTION;
END IF;
