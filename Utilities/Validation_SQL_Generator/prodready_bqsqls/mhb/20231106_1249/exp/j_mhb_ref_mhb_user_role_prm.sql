-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_user_role_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT x.*
   FROM
     (SELECT DISTINCT user_role_stg.mhb_user_role_desc
      FROM {{ params.param_clinical_ci_stage_dataset_name }}.user_role_stg) AS x
   WHERE upper(x.mhb_user_role_desc) NOT IN
       (SELECT upper(ref_mhb_user_role.mhb_user_role_desc) AS mhb_user_role_desc
        FROM {{ params.param_clinical_ci_core_dataset_name }}.ref_mhb_user_role
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS q