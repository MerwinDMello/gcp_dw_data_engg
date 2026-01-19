
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_RefChargeValue ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_RefChargeValue (ChargeValueKey, ChargeValueName, ChargeValueType, ChargeValueDesc, ChargeValuePriorityNum, ChargeValueFrequency, ChargeValueQuery, ChargeValueConfidenceLevelPercent, ChargeValueConfidenceLastValidatedDate, ChargeValueCreatedBy, ChargeValueCreatedDateKey, ChargeValueLastModifiedBy, ChargeValueLastProcessedDate, ChargeValueLastErrorMessage, Enabled, SysStartTime, SysEndTime, DeveloperEmail)
SELECT source.ChargeValueKey, TRIM(source.ChargeValueName), TRIM(source.ChargeValueType), TRIM(source.ChargeValueDesc), source.ChargeValuePriorityNum, TRIM(source.ChargeValueFrequency), TRIM(source.ChargeValueQuery), source.ChargeValueConfidenceLevelPercent, source.ChargeValueConfidenceLastValidatedDate, TRIM(source.ChargeValueCreatedBy), source.ChargeValueCreatedDateKey, TRIM(source.ChargeValueLastModifiedBy), source.ChargeValueLastProcessedDate, TRIM(source.ChargeValueLastErrorMessage), source.Enabled, source.SysStartTime, source.SysEndTime, TRIM(source.DeveloperEmail)
FROM {{ params.param_psc_stage_dataset_name }}.PV_RefChargeValue as source;
