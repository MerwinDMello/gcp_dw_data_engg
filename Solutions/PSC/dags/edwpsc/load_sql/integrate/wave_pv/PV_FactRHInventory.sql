
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactRHInventory AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactRHInventory AS source
ON target.RHInvKey = source.RHInvKey
WHEN MATCHED THEN
  UPDATE SET
  target.SnapshotDate = source.SnapshotDate,
 target.DataType = TRIM(source.DataType),
 target.RHInvKey = source.RHInvKey,
 target.RHClaimID = TRIM(source.RHClaimID),
 target.ClaimKey = source.ClaimKey,
 target.RHControlNo = TRIM(source.RHControlNo),
 target.RHStatus = TRIM(source.RHStatus),
 target.RHClaimDate = source.RHClaimDate,
 target.RHTotalAmt = source.RHTotalAmt,
 target.RHHoldCode = TRIM(source.RHHoldCode),
 target.RHHoldRuleEnabled = TRIM(source.RHHoldRuleEnabled),
 target.RHHoldRuleCreated = source.RHHoldRuleCreated,
 target.RHHoldRuleLastUpdate = source.RHHoldRuleLastUpdate,
 target.RHHoldCodeTranslated = TRIM(source.RHHoldCodeTranslated),
 target.RHDupeCount = TRIM(source.RHDupeCount),
 target.RHInvStatus = TRIM(source.RHInvStatus),
 target.RHHasError = TRIM(source.RHHasError),
 target.RHHasException = TRIM(source.RHHasException),
 target.RHHasEDIError = TRIM(source.RHHasEDIError),
 target.RHHasServiceOrder = TRIM(source.RHHasServiceOrder),
 target.RHHasUnmappedError = TRIM(source.RHHasUnmappedError),
 target.RHClaimLastBilledPaper = TRIM(source.RHClaimLastBilledPaper),
 target.IETOpenPrevBilled = TRIM(source.IETOpenPrevBilled),
 target.SystemName = TRIM(source.SystemName),
 target.PVRegionId = TRIM(source.PVRegionId),
 target.COID = TRIM(source.COID),
 target.CoidDeptNumber = TRIM(source.CoidDeptNumber),
 target.CoidName = TRIM(source.CoidName),
 target.LobCode = TRIM(source.LobCode),
 target.GroupName = TRIM(source.GroupName),
 target.DivisonName = TRIM(source.DivisonName),
 target.MarketName = TRIM(source.MarketName),
 target.PracticeName = TRIM(source.PracticeName),
 target.RenderingProviderFederalTaxID = TRIM(source.RenderingProviderFederalTaxID),
 target.RenderingProviderGroupNPI = TRIM(source.RenderingProviderGroupNPI),
 target.ClaimNumber = source.ClaimNumber,
 target.PatientName = TRIM(source.PatientName),
 target.TotalCharges = source.TotalCharges,
 target.TotalBalance = source.TotalBalance,
 target.BalanceBucket = TRIM(source.BalanceBucket),
 target.TotalPatientBalance = source.TotalPatientBalance,
 target.TotalInsuranceBalance = source.TotalInsuranceBalance,
 target.TotalPayments = source.TotalPayments,
 target.TotalInsurancePayments = source.TotalInsurancePayments,
 target.TotalPatientPayments = source.TotalPatientPayments,
 target.TotalAdjustments = source.TotalAdjustments,
 target.BilltoIndicatorDescription = TRIM(source.BilltoIndicatorDescription),
 target.BilltoIndicatorLastUpdated = TRIM(source.BilltoIndicatorLastUpdated),
 target.BilltoName = TRIM(source.BilltoName),
 target.BilltoIndicatorFinancialClass = TRIM(source.BilltoIndicatorFinancialClass),
 target.FinancialClass = TRIM(source.FinancialClass),
 target.RenderingProviderName = TRIM(source.RenderingProviderName),
 target.RenderingProviderNPI = TRIM(source.RenderingProviderNPI),
 target.FileStatusDesc = TRIM(source.FileStatusDesc),
 target.ClaimStatusLastModifiedDate = TRIM(source.ClaimStatusLastModifiedDate),
 target.ArtivaInsLiablity1PoolNumber = TRIM(source.ArtivaInsLiablity1PoolNumber),
 target.ArtivaInsLiablity1PoolName = TRIM(source.ArtivaInsLiablity1PoolName),
 target.ClaimIns1Id = TRIM(source.ClaimIns1Id),
 target.ClaimIns1Name = TRIM(source.ClaimIns1Name),
 target.ClaimIns1FinancialClass = TRIM(source.ClaimIns1FinancialClass),
 target.ArtivaInsLiablity2PoolNumber = TRIM(source.ArtivaInsLiablity2PoolNumber),
 target.ArtivaInsLiablity2PoolName = TRIM(source.ArtivaInsLiablity2PoolName),
 target.ClaimIns2Id = TRIM(source.ClaimIns2Id),
 target.ClaimIns2Name = TRIM(source.ClaimIns2Name),
 target.ClaimIns2FinancialClass = TRIM(source.ClaimIns2FinancialClass),
 target.ArtivaInsLiablity3PoolNumber = TRIM(source.ArtivaInsLiablity3PoolNumber),
 target.ArtivaInsLiablity3PoolName = TRIM(source.ArtivaInsLiablity3PoolName),
 target.ClaimIns3Id = TRIM(source.ClaimIns3Id),
 target.ClaimIns3Name = TRIM(source.ClaimIns3Name),
 target.ClaimIns3FinancialClass = TRIM(source.ClaimIns3FinancialClass),
 target.ArtivaPatLiablityPoolNumber = TRIM(source.ArtivaPatLiablityPoolNumber),
 target.ArtivaPatLiablityPoolName = TRIM(source.ArtivaPatLiablityPoolName),
 target.ServiceDate = TRIM(source.ServiceDate),
 target.ClaimAge = TRIM(source.ClaimAge),
 target.ClaimAgingBucket = TRIM(source.ClaimAgingBucket),
 target.ClaimDate = source.ClaimDate,
 target.IetSubcategoryOpen = TRIM(source.IetSubcategoryOpen),
 target.IETSubLastClosedDate = source.IETSubLastClosedDate,
 target.ClaimActionLastModifiedDate = source.ClaimActionLastModifiedDate,
 target.ClaimActionLastModifiedUsername = TRIM(source.ClaimActionLastModifiedUsername),
 target.POS = TRIM(source.POS),
 target.VoidedClaim = TRIM(source.VoidedClaim),
 target.DeletedClaim = TRIM(source.DeletedClaim),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (SnapshotDate, DataType, RHInvKey, RHClaimID, ClaimKey, RHControlNo, RHStatus, RHClaimDate, RHTotalAmt, RHHoldCode, RHHoldRuleEnabled, RHHoldRuleCreated, RHHoldRuleLastUpdate, RHHoldCodeTranslated, RHDupeCount, RHInvStatus, RHHasError, RHHasException, RHHasEDIError, RHHasServiceOrder, RHHasUnmappedError, RHClaimLastBilledPaper, IETOpenPrevBilled, SystemName, PVRegionId, COID, CoidDeptNumber, CoidName, LobCode, GroupName, DivisonName, MarketName, PracticeName, RenderingProviderFederalTaxID, RenderingProviderGroupNPI, ClaimNumber, PatientName, TotalCharges, TotalBalance, BalanceBucket, TotalPatientBalance, TotalInsuranceBalance, TotalPayments, TotalInsurancePayments, TotalPatientPayments, TotalAdjustments, BilltoIndicatorDescription, BilltoIndicatorLastUpdated, BilltoName, BilltoIndicatorFinancialClass, FinancialClass, RenderingProviderName, RenderingProviderNPI, FileStatusDesc, ClaimStatusLastModifiedDate, ArtivaInsLiablity1PoolNumber, ArtivaInsLiablity1PoolName, ClaimIns1Id, ClaimIns1Name, ClaimIns1FinancialClass, ArtivaInsLiablity2PoolNumber, ArtivaInsLiablity2PoolName, ClaimIns2Id, ClaimIns2Name, ClaimIns2FinancialClass, ArtivaInsLiablity3PoolNumber, ArtivaInsLiablity3PoolName, ClaimIns3Id, ClaimIns3Name, ClaimIns3FinancialClass, ArtivaPatLiablityPoolNumber, ArtivaPatLiablityPoolName, ServiceDate, ClaimAge, ClaimAgingBucket, ClaimDate, IetSubcategoryOpen, IETSubLastClosedDate, ClaimActionLastModifiedDate, ClaimActionLastModifiedUsername, POS, VoidedClaim, DeletedClaim, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.SnapshotDate, TRIM(source.DataType), source.RHInvKey, TRIM(source.RHClaimID), source.ClaimKey, TRIM(source.RHControlNo), TRIM(source.RHStatus), source.RHClaimDate, source.RHTotalAmt, TRIM(source.RHHoldCode), TRIM(source.RHHoldRuleEnabled), source.RHHoldRuleCreated, source.RHHoldRuleLastUpdate, TRIM(source.RHHoldCodeTranslated), TRIM(source.RHDupeCount), TRIM(source.RHInvStatus), TRIM(source.RHHasError), TRIM(source.RHHasException), TRIM(source.RHHasEDIError), TRIM(source.RHHasServiceOrder), TRIM(source.RHHasUnmappedError), TRIM(source.RHClaimLastBilledPaper), TRIM(source.IETOpenPrevBilled), TRIM(source.SystemName), TRIM(source.PVRegionId), TRIM(source.COID), TRIM(source.CoidDeptNumber), TRIM(source.CoidName), TRIM(source.LobCode), TRIM(source.GroupName), TRIM(source.DivisonName), TRIM(source.MarketName), TRIM(source.PracticeName), TRIM(source.RenderingProviderFederalTaxID), TRIM(source.RenderingProviderGroupNPI), source.ClaimNumber, TRIM(source.PatientName), source.TotalCharges, source.TotalBalance, TRIM(source.BalanceBucket), source.TotalPatientBalance, source.TotalInsuranceBalance, source.TotalPayments, source.TotalInsurancePayments, source.TotalPatientPayments, source.TotalAdjustments, TRIM(source.BilltoIndicatorDescription), TRIM(source.BilltoIndicatorLastUpdated), TRIM(source.BilltoName), TRIM(source.BilltoIndicatorFinancialClass), TRIM(source.FinancialClass), TRIM(source.RenderingProviderName), TRIM(source.RenderingProviderNPI), TRIM(source.FileStatusDesc), TRIM(source.ClaimStatusLastModifiedDate), TRIM(source.ArtivaInsLiablity1PoolNumber), TRIM(source.ArtivaInsLiablity1PoolName), TRIM(source.ClaimIns1Id), TRIM(source.ClaimIns1Name), TRIM(source.ClaimIns1FinancialClass), TRIM(source.ArtivaInsLiablity2PoolNumber), TRIM(source.ArtivaInsLiablity2PoolName), TRIM(source.ClaimIns2Id), TRIM(source.ClaimIns2Name), TRIM(source.ClaimIns2FinancialClass), TRIM(source.ArtivaInsLiablity3PoolNumber), TRIM(source.ArtivaInsLiablity3PoolName), TRIM(source.ClaimIns3Id), TRIM(source.ClaimIns3Name), TRIM(source.ClaimIns3FinancialClass), TRIM(source.ArtivaPatLiablityPoolNumber), TRIM(source.ArtivaPatLiablityPoolName), TRIM(source.ServiceDate), TRIM(source.ClaimAge), TRIM(source.ClaimAgingBucket), source.ClaimDate, TRIM(source.IetSubcategoryOpen), source.IETSubLastClosedDate, source.ClaimActionLastModifiedDate, TRIM(source.ClaimActionLastModifiedUsername), TRIM(source.POS), TRIM(source.VoidedClaim), TRIM(source.DeletedClaim), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT RHInvKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactRHInventory
      GROUP BY RHInvKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactRHInventory');
ELSE
  COMMIT TRANSACTION;
END IF;
