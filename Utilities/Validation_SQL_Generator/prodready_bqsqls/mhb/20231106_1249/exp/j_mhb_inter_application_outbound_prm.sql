-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_inter_application_outbound_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT rdc.rdc_sid,
          max(u10.user_login_name) AS outbound_user_login_name,
          stg.launch_datetime AS outbound_launch_date_time,
          coalesce(ck.patient_dw_id, NUMERIC '999999999999999999') AS patient_dw_id,
          max(coalesce(ck.company_code, 'H')) AS company_code,
          max(coalesce(ck.coid, '99999')) AS coid,
          coalesce(ck.pat_acct_num, NUMERIC '999999999999') AS pat_acct_num,
          stg.trail_id AS mhb_audit_trail_num,
          rmu2.mhb_unit_id,
          max(coalesce(cf22.coid, '99999')) AS outbound_coid,
          coalesce(cfl2.location_mnemonic_cs, 'Unknown') AS location_mnemonic_cs,
          u10.mhb_user_role_sid,
          stg.internaldevice_id AS mhb_phone_id,
          max(stg.launch_app_name) AS launch_application_name,
          max(stg.launch_action) AS launch_action_name,
          max(stg.launch_status) AS launch_status_code,
          max(CASE upper(stg.launch_sso_enabled)
                  WHEN 'TRUE' THEN 'Y'
                  WHEN 'FALSE' THEN 'N'
              END) AS launch_sso_enabled_ind,
          ROUND(CASE trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999'))
                    WHEN '' THEN NUMERIC '0'
                    ELSE CAST(trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999')) AS NUMERIC)
                END, 0, 'ROUND_HALF_EVEN') AS launch_pat_acct_num,
          max(coalesce(cf31.coid, '99999')) AS launch_patient_coid,
          rmu32.mhb_unit_id AS launch_unit_id,
          coalesce(cfl33.location_mnemonic_cs, 'Unknown') AS launch_location_mnemonic_cs,
          max(coalesce(cf34.coid, '99999')) AS launch_coid,
          stg.inferredupdate AS inferred_update_sw,
          stg.lasttimestamp AS mhb_last_enter_date_time,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwinterappoutbound AS stg
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(substr(trim(stg.databasename), strpos(trim(stg.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS u10
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.user_name) = upper(u10.user_login_name)
   AND rdc.rdc_sid = u10.rdc_sid
   LEFT OUTER JOIN -- AND U10.Active_DW_Ind = 'Y'
 {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS r10
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(trim(stg.user_role), 'Unknown')) = upper(trim(r10.mhb_user_role_desc))
   AND r10.mhb_user_role_sid = u10.mhb_user_role_sid
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf22
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf22.facility_mnemonic_cs) = trim(stg.facilitycode)
   AND upper(cf22.facility_active_ind) = 'Y'
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility AS cf2 ON trim(cf2.facility_mnemonic_cs) = trim(stg.launch_patient_facility_code)
   AND upper(cf2.facility_active_ind) = 'Y'
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm_base_views.clinical_acctkeys AS ck ON upper(ck.coid) = upper(cf2.coid)
   AND ck.pat_acct_num = ROUND(CASE trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999'))
                                   WHEN '' THEN NUMERIC '0'
                                   ELSE CAST(trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999')) AS NUMERIC)
                               END, 0, 'ROUND_HALF_EVEN')
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu2.rdc_sid = rdc.rdc_sid
   AND stg.unit_id = rmu2.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf31
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf31.facility_mnemonic_cs) = trim(stg.launch_patient_facility_code)
   AND upper(cf31.facility_active_ind) = 'Y'
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl2.location_mnemonic_cs) = trim(substr(stg.unitcode, strpos(stg.unitcode, '_') + 1))
   AND upper(cf31.coid) = upper(cfl2.coid)
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu32
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu32.rdc_sid = rdc.rdc_sid
   AND stg.launch_unit_id = rmu32.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf34
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf34.facility_mnemonic_cs) = trim(stg.launch_facilitycode)
   AND upper(cf34.facility_active_ind) = 'Y'
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl33
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl33.location_mnemonic_cs) = trim(substr(stg.launch_unitcode, strpos(stg.launch_unitcode, '_') + 1))
   AND upper(cf34.coid) = upper(cfl33.coid)
   WHERE DATE(stg.launch_datetime) BETWEEN u10.eff_from_date AND u10.eff_to_date
   GROUP BY 1,
            upper(u10.user_login_name),
            3,
            4,
            upper(coalesce(ck.company_code, 'H')),
            upper(coalesce(ck.coid, '99999')),
            7,
            8,
            9,
            upper(coalesce(cf22.coid, '99999')),
            11,
            12,
            13,
            upper(stg.launch_app_name),
            upper(stg.launch_action),
            upper(stg.launch_status),
            upper(CASE upper(stg.launch_sso_enabled)
                      WHEN 'TRUE' THEN 'Y'
                      WHEN 'FALSE' THEN 'N'
                  END),
            18,
            upper(coalesce(cf31.coid, '99999')),
            20,
            21,
            upper(coalesce(cf34.coid, '99999')),
            23,
            24,
            25,
            26) AS a