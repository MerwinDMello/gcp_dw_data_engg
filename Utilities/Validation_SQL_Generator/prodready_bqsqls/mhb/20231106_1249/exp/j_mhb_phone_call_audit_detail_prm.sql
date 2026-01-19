-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_phone_call_audit_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT rdc.rdc_sid
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwuserphonecalls AS pc
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rdc.rdc_desc) = upper(replace(pc.databasename, 'heartbeatDW_', ''))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS mu
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(pc.user_name) = upper(mu.user_login_name)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS ur
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(pc.user_role)) = upper(trim(ur.mhb_user_role_desc))
   AND ur.mhb_user_role_sid = mu.mhb_user_role_sid
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf.facility_mnemonic_cs) = trim(pc.facilitycode)
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl.location_mnemonic_cs) = trim(substr(pc.unitcode, strpos(pc.unitcode, '_') + 1))
   WHERE mu.eff_to_date = DATE '9999-12-31'
     AND upper(mu.active_dw_ind) = 'Y' ) AS q