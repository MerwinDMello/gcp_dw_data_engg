
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_RefIplanFinancialGroupCrosswalk ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefIplanFinancialGroupCrosswalk (Payor_Name, IplanGroupFinancialName)
SELECT TRIM(source.Payor_Name), TRIM(source.IplanGroupFinancialName)
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_RefIplanFinancialGroupCrosswalk as source;
