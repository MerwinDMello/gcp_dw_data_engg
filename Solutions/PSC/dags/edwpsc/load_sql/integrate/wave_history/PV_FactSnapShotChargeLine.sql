
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotChargeLine AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotChargeLine AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ChargeLineKey = source.ChargeLineKey
WHEN MATCHED THEN
  UPDATE SET
  target.ChargeLineKey = source.ChargeLineKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.PracticeKey = source.PracticeKey,
 target.PracticeID = TRIM(source.PracticeID),
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
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.FinancialClassKey = source.FinancialClassKey,
 target.CPTID = TRIM(source.CPTID),
 target.CPTOrder = source.CPTOrder,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTStartServiceDateKey = source.CPTStartServiceDateKey,
 target.CPTEndServiceDateKey = source.CPTEndServiceDateKey,
 target.CPTTypeOfService = TRIM(source.CPTTypeOfService),
 target.CPTChargesAmt = source.CPTChargesAmt,
 target.CPTChargesPerUnitAmt = source.CPTChargesPerUnitAmt,
 target.CPTUnits = source.CPTUnits,
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.CPTModifier3 = TRIM(source.CPTModifier3),
 target.CPTModifier4 = TRIM(source.CPTModifier4),
 target.CPTDeleteFlag = source.CPTDeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.CPTPOSKey = TRIM(source.CPTPOSKey),
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.CPTBalance = source.CPTBalance
WHEN NOT MATCHED THEN
  INSERT (ChargeLineKey, MonthID, SnapShotDate, Coid, RegionKey, PracticeKey, PracticeID, ClaimKey, ClaimNumber, GLDepartment, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ServiceDateKey, Iplan1IplanKey, Iplan1ID, FinancialClassKey, CPTID, CPTOrder, CPTCode, CPTCodeKey, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTTypeOfService, CPTChargesAmt, CPTChargesPerUnitAmt, CPTUnits, CPTModifier1, CPTModifier2, CPTModifier3, CPTModifier4, CPTDeleteFlag, DWLastUpdateDateTime, CPTPOSKey, ClaimLineChargeKey, CPTBalance)
  VALUES (source.ChargeLineKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.RegionKey, source.PracticeKey, TRIM(source.PracticeID), source.ClaimKey, source.ClaimNumber, TRIM(source.GLDepartment), source.PatientID, source.ServicingProviderKey, source.ServicingProviderID, source.RenderingProviderKey, source.RenderingProviderID, source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1IplanKey, TRIM(source.Iplan1ID), source.FinancialClassKey, TRIM(source.CPTID), source.CPTOrder, TRIM(source.CPTCode), source.CPTCodeKey, source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, TRIM(source.CPTTypeOfService), source.CPTChargesAmt, source.CPTChargesPerUnitAmt, source.CPTUnits, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), TRIM(source.CPTModifier3), TRIM(source.CPTModifier4), source.CPTDeleteFlag, source.DWLastUpdateDateTime, TRIM(source.CPTPOSKey), source.ClaimLineChargeKey, source.CPTBalance);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ChargeLineKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotChargeLine
      GROUP BY SnapShotDate, ChargeLineKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotChargeLine');
ELSE
  COMMIT TRANSACTION;
END IF;
