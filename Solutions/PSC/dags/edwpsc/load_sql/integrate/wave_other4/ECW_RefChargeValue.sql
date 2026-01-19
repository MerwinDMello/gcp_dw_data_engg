
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefChargeValue AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefChargeValue AS source
ON target.ChargeValueKey = source.ChargeValueKey
WHEN MATCHED THEN
  UPDATE SET
  target.ChargeValueKey = source.ChargeValueKey,
 target.ChargeValueName = TRIM(source.ChargeValueName),
 target.ChargeValueType = TRIM(source.ChargeValueType),
 target.ChargeValueDesc = TRIM(source.ChargeValueDesc),
 target.ChargeValuePriorityNum = source.ChargeValuePriorityNum,
 target.ChargeValueFrequency = TRIM(source.ChargeValueFrequency),
 target.ChargeValueQuery = TRIM(source.ChargeValueQuery),
 target.ChargeValueConfidenceLevelPercent = source.ChargeValueConfidenceLevelPercent,
 target.ChargeValueConfidenceLastValidatedDate = source.ChargeValueConfidenceLastValidatedDate,
 target.ChargeValueCreatedBy = TRIM(source.ChargeValueCreatedBy),
 target.ChargeValueCreatedDateKey = source.ChargeValueCreatedDateKey,
 target.ChargeValueLastModifiedBy = TRIM(source.ChargeValueLastModifiedBy),
 target.ChargeValueLastProcessedDate = source.ChargeValueLastProcessedDate,
 target.ChargeValueLastErrorMessage = TRIM(source.ChargeValueLastErrorMessage),
 target.Enabled = source.Enabled,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime,
 target.DeveloperEmail = TRIM(source.DeveloperEmail),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (ChargeValueKey, ChargeValueName, ChargeValueType, ChargeValueDesc, ChargeValuePriorityNum, ChargeValueFrequency, ChargeValueQuery, ChargeValueConfidenceLevelPercent, ChargeValueConfidenceLastValidatedDate, ChargeValueCreatedBy, ChargeValueCreatedDateKey, ChargeValueLastModifiedBy, ChargeValueLastProcessedDate, ChargeValueLastErrorMessage, Enabled, SysStartTime, SysEndTime, DeveloperEmail, DWLastUpdateDateTime)
  VALUES (source.ChargeValueKey, TRIM(source.ChargeValueName), TRIM(source.ChargeValueType), TRIM(source.ChargeValueDesc), source.ChargeValuePriorityNum, TRIM(source.ChargeValueFrequency), TRIM(source.ChargeValueQuery), source.ChargeValueConfidenceLevelPercent, source.ChargeValueConfidenceLastValidatedDate, TRIM(source.ChargeValueCreatedBy), source.ChargeValueCreatedDateKey, TRIM(source.ChargeValueLastModifiedBy), source.ChargeValueLastProcessedDate, TRIM(source.ChargeValueLastErrorMessage), source.Enabled, source.SysStartTime, source.SysEndTime, TRIM(source.DeveloperEmail), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ChargeValueKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefChargeValue
      GROUP BY ChargeValueKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefChargeValue');
ELSE
  COMMIT TRANSACTION;
END IF;
