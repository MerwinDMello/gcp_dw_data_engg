
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineChargeValue AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimLineChargeValue AS source
ON target.ClaimLineChargeValueKey = source.ClaimLineChargeValueKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLineChargeValueKey = source.ClaimLineChargeValueKey,
 target.ProcessedDate = source.ProcessedDate,
 target.ProcessedChargeValuePriorityNum = source.ProcessedChargeValuePriorityNum,
 target.ProcessedChargeValueType = TRIM(source.ProcessedChargeValueType),
 target.ProcessedLastRunFlag = source.ProcessedLastRunFlag,
 target.ChargeValueKey = source.ChargeValueKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.Coid = TRIM(source.Coid),
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.AdjCode = TRIM(source.AdjCode),
 target.CPTBalance = source.CPTBalance,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (ClaimLineChargeValueKey, ProcessedDate, ProcessedChargeValuePriorityNum, ProcessedChargeValueType, ProcessedLastRunFlag, ChargeValueKey, ClaimKey, ClaimNumber, Coid, ClaimLineChargeKey, PracticeID, AdjCode, CPTBalance, DWLastUpdateDateTime)
  VALUES (source.ClaimLineChargeValueKey, source.ProcessedDate, source.ProcessedChargeValuePriorityNum, TRIM(source.ProcessedChargeValueType), source.ProcessedLastRunFlag, source.ChargeValueKey, source.ClaimKey, source.ClaimNumber, TRIM(source.Coid), source.ClaimLineChargeKey, TRIM(source.PracticeID), TRIM(source.AdjCode), source.CPTBalance, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLineChargeValueKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineChargeValue
      GROUP BY ClaimLineChargeValueKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineChargeValue');
ELSE
  COMMIT TRANSACTION;
END IF;
