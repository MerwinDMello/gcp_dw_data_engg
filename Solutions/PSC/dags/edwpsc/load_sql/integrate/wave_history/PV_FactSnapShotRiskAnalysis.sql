
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotRiskAnalysis AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotRiskAnalysis AS source
ON target.SnapShotDate = source.SnapShotDate AND target.RiskAnalysisKey = source.RiskAnalysisKey
WHEN MATCHED THEN
  UPDATE SET
  target.RiskAnalysisKey = source.RiskAnalysisKey,
 target.SnapShotDate = source.SnapShotDate,
 target.RiskLoadDateKey = source.RiskLoadDateKey,
 target.ClaimCategory = TRIM(source.ClaimCategory),
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimKey = source.ClaimKey,
 target.RegionKey = source.RegionKey,
 target.COID = TRIM(source.COID),
 target.COIDLOB = TRIM(source.COIDLOB),
 target.COIDDeptNum = TRIM(source.COIDDeptNum),
 target.COIDName = TRIM(source.COIDName),
 target.MarketName = TRIM(source.MarketName),
 target.DivisionName = TRIM(source.DivisionName),
 target.GroupName = TRIM(source.GroupName),
 target.RenderingProviderNPI = TRIM(source.RenderingProviderNPI),
 target.PayerFinancialClass = TRIM(source.PayerFinancialClass),
 target.CPID = TRIM(source.CPID),
 target.FacilityID = TRIM(source.FacilityID),
 target.PracticeFederalTaxID = TRIM(source.PracticeFederalTaxID),
 target.ArtivaRuleID = source.ArtivaRuleID,
 target.ArtivaRuleDesc = TRIM(source.ArtivaRuleDesc),
 target.ArtivaRuleCreateDate = source.ArtivaRuleCreateDate,
 target.ArtivaRuleLastUpdateDate = source.ArtivaRuleLastUpdateDate,
 target.ArtivaRuleGlobalFlag = source.ArtivaRuleGlobalFlag,
 target.ArtivaHoldCodeID = TRIM(source.ArtivaHoldCodeID),
 target.ArtivaHoldCodeDescription = TRIM(source.ArtivaHoldCodeDescription),
 target.ArtivaHoldCodeType = TRIM(source.ArtivaHoldCodeType),
 target.ClaimPayorID = TRIM(source.ClaimPayorID),
 target.ClaimPayorName = TRIM(source.ClaimPayorName),
 target.ClaimPayorAuthorizationNumber = TRIM(source.ClaimPayorAuthorizationNumber),
 target.ServiceDateKey = source.ServiceDateKey,
 target.ClaimAge = source.ClaimAge,
 target.ClaimLastReleasedDate = source.ClaimLastReleasedDate,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.TotalChargesAmt = source.TotalChargesAmt,
 target.RenderingProvider = TRIM(source.RenderingProvider),
 target.ProviderKey = TRIM(source.ProviderKey),
 target.CactusDivisionName = TRIM(source.CactusDivisionName),
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderMiddleName = TRIM(source.ProviderMiddleName),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderStartDate = source.ProviderStartDate,
 target.ProviderNotificationDate = source.ProviderNotificationDate,
 target.ProviderLastRFCReceivedDate = source.ProviderLastRFCReceivedDate,
 target.GroupKey = TRIM(source.GroupKey),
 target.ProviderGroupName = TRIM(source.ProviderGroupName),
 target.GroupDBA = TRIM(source.GroupDBA),
 target.GroupEnrollmentOperation = TRIM(source.GroupEnrollmentOperation),
 target.ProviderAddressType = TRIM(source.ProviderAddressType),
 target.PracticeAddressLine1 = TRIM(source.PracticeAddressLine1),
 target.PracticeAddressLine2 = TRIM(source.PracticeAddressLine2),
 target.PracticeCity = TRIM(source.PracticeCity),
 target.PracticeState = TRIM(source.PracticeState),
 target.PracticeZip = TRIM(source.PracticeZip),
 target.PracticeTaxID = TRIM(source.PracticeTaxID),
 target.PPIKey = TRIM(source.PPIKey),
 target.PPIEffectiveDate = source.PPIEffectiveDate,
 target.PPIDepartmentFlag = TRIM(source.PPIDepartmentFlag),
 target.PPICPIDPayorName = TRIM(source.PPICPIDPayorName),
 target.PPICPIDPayorID = TRIM(source.PPICPIDPayorID),
 target.PayorSplitByLocationFlag = TRIM(source.PayorSplitByLocationFlag),
 target.PPIType = TRIM(source.PPIType),
 target.PPIStatus = TRIM(source.PPIStatus),
 target.PPIStatusDescription = TRIM(source.PPIStatusDescription),
 target.PPIPhase = TRIM(source.PPIPhase),
 target.PPIEnrollmentAction = TRIM(source.PPIEnrollmentAction),
 target.PPILeadGroupTaxID = TRIM(source.PPILeadGroupTaxID),
 target.PPIAlternateEffectiveDate = source.PPIAlternateEffectiveDate,
 target.PPIStatusDate = source.PPIStatusDate,
 target.PPILastWorkedDate = source.PPILastWorkedDate,
 target.PPIApplicationSentToPayorDate = source.PPIApplicationSentToPayorDate,
 target.PPIApplicationReceivedFromPayorDate = source.PPIApplicationReceivedFromPayorDate,
 target.PPIApplicationSentToProviderDate = source.PPIApplicationSentToProviderDate,
 target.PPIApplicationReceivedFromProviderDate = source.PPIApplicationReceivedFromProviderDate,
 target.RiskStatus = TRIM(source.RiskStatus),
 target.RiskSubcategory = TRIM(source.RiskSubcategory),
 target.PSGRiskStatus = TRIM(source.PSGRiskStatus),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (RiskAnalysisKey, SnapShotDate, RiskLoadDateKey, ClaimCategory, ClaimNumber, ClaimKey, RegionKey, COID, COIDLOB, COIDDeptNum, COIDName, MarketName, DivisionName, GroupName, RenderingProviderNPI, PayerFinancialClass, CPID, FacilityID, PracticeFederalTaxID, ArtivaRuleID, ArtivaRuleDesc, ArtivaRuleCreateDate, ArtivaRuleLastUpdateDate, ArtivaRuleGlobalFlag, ArtivaHoldCodeID, ArtivaHoldCodeDescription, ArtivaHoldCodeType, ClaimPayorID, ClaimPayorName, ClaimPayorAuthorizationNumber, ServiceDateKey, ClaimAge, ClaimLastReleasedDate, TotalBalanceAmt, TotalChargesAmt, RenderingProvider, ProviderKey, CactusDivisionName, ProviderFirstName, ProviderLastName, ProviderMiddleName, ProviderNPI, ProviderStartDate, ProviderNotificationDate, ProviderLastRFCReceivedDate, GroupKey, ProviderGroupName, GroupDBA, GroupEnrollmentOperation, ProviderAddressType, PracticeAddressLine1, PracticeAddressLine2, PracticeCity, PracticeState, PracticeZip, PracticeTaxID, PPIKey, PPIEffectiveDate, PPIDepartmentFlag, PPICPIDPayorName, PPICPIDPayorID, PayorSplitByLocationFlag, PPIType, PPIStatus, PPIStatusDescription, PPIPhase, PPIEnrollmentAction, PPILeadGroupTaxID, PPIAlternateEffectiveDate, PPIStatusDate, PPILastWorkedDate, PPIApplicationSentToPayorDate, PPIApplicationReceivedFromPayorDate, PPIApplicationSentToProviderDate, PPIApplicationReceivedFromProviderDate, RiskStatus, RiskSubcategory, PSGRiskStatus, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.RiskAnalysisKey, source.SnapShotDate, source.RiskLoadDateKey, TRIM(source.ClaimCategory), source.ClaimNumber, source.ClaimKey, source.RegionKey, TRIM(source.COID), TRIM(source.COIDLOB), TRIM(source.COIDDeptNum), TRIM(source.COIDName), TRIM(source.MarketName), TRIM(source.DivisionName), TRIM(source.GroupName), TRIM(source.RenderingProviderNPI), TRIM(source.PayerFinancialClass), TRIM(source.CPID), TRIM(source.FacilityID), TRIM(source.PracticeFederalTaxID), source.ArtivaRuleID, TRIM(source.ArtivaRuleDesc), source.ArtivaRuleCreateDate, source.ArtivaRuleLastUpdateDate, source.ArtivaRuleGlobalFlag, TRIM(source.ArtivaHoldCodeID), TRIM(source.ArtivaHoldCodeDescription), TRIM(source.ArtivaHoldCodeType), TRIM(source.ClaimPayorID), TRIM(source.ClaimPayorName), TRIM(source.ClaimPayorAuthorizationNumber), source.ServiceDateKey, source.ClaimAge, source.ClaimLastReleasedDate, source.TotalBalanceAmt, source.TotalChargesAmt, TRIM(source.RenderingProvider), TRIM(source.ProviderKey), TRIM(source.CactusDivisionName), TRIM(source.ProviderFirstName), TRIM(source.ProviderLastName), TRIM(source.ProviderMiddleName), TRIM(source.ProviderNPI), source.ProviderStartDate, source.ProviderNotificationDate, source.ProviderLastRFCReceivedDate, TRIM(source.GroupKey), TRIM(source.ProviderGroupName), TRIM(source.GroupDBA), TRIM(source.GroupEnrollmentOperation), TRIM(source.ProviderAddressType), TRIM(source.PracticeAddressLine1), TRIM(source.PracticeAddressLine2), TRIM(source.PracticeCity), TRIM(source.PracticeState), TRIM(source.PracticeZip), TRIM(source.PracticeTaxID), TRIM(source.PPIKey), source.PPIEffectiveDate, TRIM(source.PPIDepartmentFlag), TRIM(source.PPICPIDPayorName), TRIM(source.PPICPIDPayorID), TRIM(source.PayorSplitByLocationFlag), TRIM(source.PPIType), TRIM(source.PPIStatus), TRIM(source.PPIStatusDescription), TRIM(source.PPIPhase), TRIM(source.PPIEnrollmentAction), TRIM(source.PPILeadGroupTaxID), source.PPIAlternateEffectiveDate, source.PPIStatusDate, source.PPILastWorkedDate, source.PPIApplicationSentToPayorDate, source.PPIApplicationReceivedFromPayorDate, source.PPIApplicationSentToProviderDate, source.PPIApplicationReceivedFromProviderDate, TRIM(source.RiskStatus), TRIM(source.RiskSubcategory), TRIM(source.PSGRiskStatus), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, RiskAnalysisKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotRiskAnalysis
      GROUP BY SnapShotDate, RiskAnalysisKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotRiskAnalysis');
ELSE
  COMMIT TRANSACTION;
END IF;
