
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.CCU_RprtRolloutScheduleList ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtRolloutScheduleList (RolloutScheduleListKey, COID, CCUGoLiveDate, CCUGoLiveStatusValue, CCUDiscontinuedDate, PKActivationStatusValue, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, GME)
SELECT source.RolloutScheduleListKey, TRIM(source.COID), source.CCUGoLiveDate, TRIM(source.CCUGoLiveStatusValue), source.CCUDiscontinuedDate, TRIM(source.PKActivationStatusValue), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.GME)
FROM {{ params.param_psc_stage_dataset_name }}.CCU_RprtRolloutScheduleList as source;