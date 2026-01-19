
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_RefIplanCrossWalkV2 ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_RefIplanCrossWalkV2 (PVClass, PVType, FinancialClassKey, IplanGroupName)
SELECT TRIM(source.PVClass), TRIM(source.PVType), source.FinancialClassKey, TRIM(source.IplanGroupName)
FROM {{ params.param_psc_stage_dataset_name }}.PV_RefIplanCrossWalkV2 as source;
