-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_rad_onc_patient_course.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_stage_dataset_name }}.stg_dimcourse AS dp
INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site AS rs
FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rs.source_site_id = dp.dimsiteid
LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.rad_onc_patient AS ra
FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON dp.dimpatientid = ra.source_patient_id
AND rs.site_sk = ra.site_sk