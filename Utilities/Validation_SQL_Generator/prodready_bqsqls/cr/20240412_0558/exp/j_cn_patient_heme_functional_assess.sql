-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_heme_functional_assess.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_functional_assess_stg AS stg
LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_test_type AS REF ON upper(rtrim(ref.test_sub_type_desc)) = upper(rtrim(stg.testtype))
AND upper(rtrim(ref.test_type_desc)) = 'FUNCTIONAL ASSESSMENT'
WHERE upper(rtrim(stg.hbsource)) NOT IN
    (SELECT upper(rtrim(cn_patient_heme_func_assess.hashbite_ssk)) AS hashbite_ssk
     FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme_func_assess
     FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central'))