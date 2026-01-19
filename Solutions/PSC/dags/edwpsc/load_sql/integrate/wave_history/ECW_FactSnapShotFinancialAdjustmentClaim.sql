
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentClaim AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentClaim AS source
ON target.SnapShotDate = source.SnapShotDate AND target.AdjustmentClaimKey = source.AdjustmentClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.AdjustmentClaimKey = source.AdjustmentClaimKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.GLDepartment = TRIM(source.GLDepartment),
 target.PatientID = source.PatientID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = source.ServicingProviderID,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = source.RenderingProviderID,
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = source.Iplan1ID,
 target.FinancialClassKey = source.FinancialClassKey,
 target.AdjustmentID = source.AdjustmentID,
 target.AdjustmentCode = TRIM(source.AdjustmentCode),
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.AdjustmentAmt = source.AdjustmentAmt,
 target.UnpostedCPTAmt = source.UnpostedCPTAmt,
 target.UnpostedClaimAmt = source.UnpostedClaimAmt,
 target.AdjustmentCreateDateKey = source.AdjustmentCreateDateKey,
 target.AdjustmentModifiedDateKey = source.AdjustmentModifiedDateKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (AdjustmentClaimKey, MonthID, SnapShotDate, Coid, RegionKey, ClaimKey, ClaimNumber, GLDepartment, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ServiceDateKey, Iplan1IplanKey, Iplan1ID, FinancialClassKey, AdjustmentID, AdjustmentCode, AdjustmentCodeKey, AdjustmentAmt, UnpostedCPTAmt, UnpostedClaimAmt, AdjustmentCreateDateKey, AdjustmentModifiedDateKey, DWLastUpdateDateTime)
  VALUES (source.AdjustmentClaimKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.RegionKey, source.ClaimKey, source.ClaimNumber, TRIM(source.GLDepartment), source.PatientID, source.ServicingProviderKey, source.ServicingProviderID, source.RenderingProviderKey, source.RenderingProviderID, source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1IplanKey, source.Iplan1ID, source.FinancialClassKey, source.AdjustmentID, TRIM(source.AdjustmentCode), source.AdjustmentCodeKey, source.AdjustmentAmt, source.UnpostedCPTAmt, source.UnpostedClaimAmt, source.AdjustmentCreateDateKey, source.AdjustmentModifiedDateKey, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, AdjustmentClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentClaim
      GROUP BY SnapShotDate, AdjustmentClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentClaim');
ELSE
  COMMIT TRANSACTION;
END IF;
