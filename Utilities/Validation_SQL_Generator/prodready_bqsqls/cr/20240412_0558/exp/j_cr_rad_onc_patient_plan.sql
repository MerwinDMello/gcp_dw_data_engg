-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_rad_onc_patient_plan.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_stage_dataset_name }}.stg_dimplan AS dp
INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_rad_onc_plan_purpose AS rpp
FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(dp.planintent)) = upper(rtrim(rpp.plan_purpose_name))
INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site AS rr
FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rr.source_site_id = CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(dp.dimsiteid) AS FLOAT64)
LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.rad_onc_patient_course AS rpc
FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rpc.source_patient_course_id = CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(dp.dimcourseid) AS FLOAT64)
LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.rad_onc_patient_plan AS core
FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rr.site_sk = core.site_sk
AND CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(dp.dimplanid) AS FLOAT64) = core.source_patient_plan_id
WHERE core.patient_plan_sk IS NULL