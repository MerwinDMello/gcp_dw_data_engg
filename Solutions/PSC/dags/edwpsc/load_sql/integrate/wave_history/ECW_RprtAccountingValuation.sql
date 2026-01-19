
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtAccountingValuation AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtAccountingValuation AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.COID = TRIM(source.COID),
 target.CoidName = TRIM(source.CoidName),
 target.COIDandCOIDName = TRIM(source.COIDandCOIDName),
 target.ServicingProviderName = TRIM(source.ServicingProviderName),
 target.Ins1PayerFinancialClassNum = source.Ins1PayerFinancialClassNum,
 target.Ins1InsPayerFinancialClassName = TRIM(source.Ins1InsPayerFinancialClassName),
 target.Ins1InsPayerGroupName = TRIM(source.Ins1InsPayerGroupName),
 target.ResolvedClaimCharges = source.ResolvedClaimCharges,
 target.ResolvedContractualAdjustments = source.ResolvedContractualAdjustments,
 target.ResolvedAdministrationAdjustments = source.ResolvedAdministrationAdjustments,
 target.ResolvedDenialAdjustments = source.ResolvedDenialAdjustments,
 target.ResolvedInsurancePayments = source.ResolvedInsurancePayments,
 target.ResolvedPatientPayments = source.ResolvedPatientPayments,
 target.UnresolvedClaimChargesCredit = source.UnresolvedClaimChargesCredit,
 target.UnresolvedContractualAdjustmentsCredit = source.UnresolvedContractualAdjustmentsCredit,
 target.UnresolvedAdministrationAdjustmentsCredit = source.UnresolvedAdministrationAdjustmentsCredit,
 target.UnResolvedDenialAdjustmentsCredit = source.UnResolvedDenialAdjustmentsCredit,
 target.UnResolvedInsurancePaymentsCredit = source.UnResolvedInsurancePaymentsCredit,
 target.UnResolvedPatientPaymentsCredit = source.UnResolvedPatientPaymentsCredit,
 target.UnresolvedInsuranceBalanceCredit = source.UnresolvedInsuranceBalanceCredit,
 target.UnresolvedPatientBalanceCredit = source.UnresolvedPatientBalanceCredit,
 target.UnresolvedClaimChargesDebit = source.UnresolvedClaimChargesDebit,
 target.UnresolvedContractualAdjustmentsDebit = source.UnresolvedContractualAdjustmentsDebit,
 target.UnresolvedAdministrationAdjustmentsDebit = source.UnresolvedAdministrationAdjustmentsDebit,
 target.UnResolvedDenialAdjustmentsDebit = source.UnResolvedDenialAdjustmentsDebit,
 target.UnResolvedInsurancePaymentsDebit = source.UnResolvedInsurancePaymentsDebit,
 target.UnResolvedPatientPaymentsDebit = source.UnResolvedPatientPaymentsDebit,
 target.UnresolvedInsuranceBalanceDebit = source.UnresolvedInsuranceBalanceDebit,
 target.UnresolvedPatientBalanceDebit = source.UnresolvedPatientBalanceDebit,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, GroupName, DivisionName, MarketName, COID, CoidName, COIDandCOIDName, ServicingProviderName, Ins1PayerFinancialClassNum, Ins1InsPayerFinancialClassName, Ins1InsPayerGroupName, ResolvedClaimCharges, ResolvedContractualAdjustments, ResolvedAdministrationAdjustments, ResolvedDenialAdjustments, ResolvedInsurancePayments, ResolvedPatientPayments, UnresolvedClaimChargesCredit, UnresolvedContractualAdjustmentsCredit, UnresolvedAdministrationAdjustmentsCredit, UnResolvedDenialAdjustmentsCredit, UnResolvedInsurancePaymentsCredit, UnResolvedPatientPaymentsCredit, UnresolvedInsuranceBalanceCredit, UnresolvedPatientBalanceCredit, UnresolvedClaimChargesDebit, UnresolvedContractualAdjustmentsDebit, UnresolvedAdministrationAdjustmentsDebit, UnResolvedDenialAdjustmentsDebit, UnResolvedInsurancePaymentsDebit, UnResolvedPatientPaymentsDebit, UnresolvedInsuranceBalanceDebit, UnresolvedPatientBalanceDebit, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.COID), TRIM(source.CoidName), TRIM(source.COIDandCOIDName), TRIM(source.ServicingProviderName), source.Ins1PayerFinancialClassNum, TRIM(source.Ins1InsPayerFinancialClassName), TRIM(source.Ins1InsPayerGroupName), source.ResolvedClaimCharges, source.ResolvedContractualAdjustments, source.ResolvedAdministrationAdjustments, source.ResolvedDenialAdjustments, source.ResolvedInsurancePayments, source.ResolvedPatientPayments, source.UnresolvedClaimChargesCredit, source.UnresolvedContractualAdjustmentsCredit, source.UnresolvedAdministrationAdjustmentsCredit, source.UnResolvedDenialAdjustmentsCredit, source.UnResolvedInsurancePaymentsCredit, source.UnResolvedPatientPaymentsCredit, source.UnresolvedInsuranceBalanceCredit, source.UnresolvedPatientBalanceCredit, source.UnresolvedClaimChargesDebit, source.UnresolvedContractualAdjustmentsDebit, source.UnresolvedAdministrationAdjustmentsDebit, source.UnResolvedDenialAdjustmentsDebit, source.UnResolvedInsurancePaymentsDebit, source.UnResolvedPatientPaymentsDebit, source.UnresolvedInsuranceBalanceDebit, source.UnresolvedPatientBalanceDebit, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtAccountingValuation
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtAccountingValuation');
ELSE
  COMMIT TRANSACTION;
END IF;
