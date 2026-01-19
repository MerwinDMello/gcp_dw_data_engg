
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefJobDeptCode ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefJobDeptCode (DeptCode, DeptCodeName)
SELECT TRIM(source.DeptCode), TRIM(source.DeptCodeName)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefJobDeptCode as source;
