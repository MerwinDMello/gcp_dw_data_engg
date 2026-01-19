-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_user_group_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT x.*
   FROM
     (SELECT rdc.rdc_sid,
             ug.usergroup_id AS mhb_user_group_id,
             ug.usergroupname AS user_group_name,
             coalesce(cf.company_code, 'H') AS company_code,
             coalesce(cf.coid, '99999') AS coid,
             'B' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwusergroups AS ug
      INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rdc.rdc_desc) = upper(replace(ug.databasename, 'heartbeatDW_', ''))
      LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf.facility_mnemonic_cs) = trim(ug.unitcode)
      WHERE (rdc.rdc_sid,
             ug.usergroup_id) NOT IN
          (SELECT AS STRUCT ref_mhb_user_group.rdc_sid,
                            ref_mhb_user_group.mhb_user_group_id
           FROM {{ params.param_clinical_ci_core_dataset_name }}.ref_mhb_user_group
           FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS x) AS q