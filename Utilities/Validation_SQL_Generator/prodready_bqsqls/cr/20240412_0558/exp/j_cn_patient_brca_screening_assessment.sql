-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_brca_screening_assessment.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT cn_patient_brca_screening_assessment_stg.hashbite_ssk
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_brca_screening_assessment_stg
   WHERE upper(cn_patient_brca_screening_assessment_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_brca_screening_assessment.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_brca_screening_assessment
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a