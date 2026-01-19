-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_inter_application_inbound_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          rdc.rdc_sid,
          max(u10.user_login_name) AS inbound_user_login_name,
          stg.launch_datetime AS inbound_launch_date_time,
          coalesce(ck.patient_dw_id, NUMERIC '999999999999999999') AS patient_dw_id,
          max(coalesce(ck.company_code, 'H')) AS company_code,
          max(coalesce(ck.coid, '99999')) AS coid,
          coalesce(ck.pat_acct_num, NUMERIC '999999999999') AS pat_acct_num,
          stg.trail_id AS mhb_audit_trail_num,
          rmu2.mhb_unit_id,
          max(coalesce(cf22.coid, '99999')) AS inbound_coid,
          coalesce(cfl5.location_mnemonic_cs, 'Unknown') AS location_mnemonic_cs,
          u10.mhb_user_role_sid,
          stg.internaldevice_id AS mhb_phone_id,
          max(stg.launch_app_name) AS launch_application_name,
          max(stg.launch_action) AS launch_action_name,
          max(stg.launch_status) AS launch_status_code,
          ROUND(CASE
             trim(coalesce(bqutil.fn.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999'))
            WHEN '' THEN NUMERIC '0'
            ELSE CAST(trim(coalesce(bqutil.fn.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999')) as NUMERIC)
          END, 0, 'ROUND_HALF_EVEN') AS launch_pat_acct_num,
          max(coalesce(cf41.coid, '99999')) AS launch_patient_coid,
          rmu51.mhb_unit_id AS launch_unit_id,
          coalesce(cfl2.location_mnemonic_cs, 'Unknown') AS launch_location_mnemonic_cs,
          max(coalesce(cf53.coid, '99999')) AS launch_coid,
          stg.inferredupdate AS inferred_update_sw,
          stg.lasttimestamp AS mhb_last_enter_date_time,
          max(stg.recipient_phone_number) AS recipient_phone_num,
          max(u11.user_login_name) AS recipient_user_login_name,
          max(coalesce(cf10.coid, '99999')) AS recipient_coid,
          coalesce(cfl20.location_mnemonic_cs, 'Unknown') AS recipient_location_mnemonic_cs,
          rmu22.mhb_unit_id AS recipient_mhb_unit_id,
          u11.mhb_user_role_sid AS recipient_mhb_user_role_sid,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-clinical`.edwci_staging.vwinterappinbound AS stg
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(substr(trim(stg.databasename), strpos(trim(stg.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS u10 ON upper(stg.user_name) = upper(u10.user_login_name)
           AND rdc.rdc_sid = u10.rdc_sid
          LEFT OUTER JOIN -- AND U10.Active_DW_Ind = 'Y'
          `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_user_role AS r10 ON upper(coalesce(trim(stg.user_role), 'Unknown')) = upper(trim(r10.mhb_user_role_desc))
           AND r10.mhb_user_role_sid = u10.mhb_user_role_sid
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf22 ON trim(cf22.facility_mnemonic_cs) = trim(stg.facilitycode)
           AND upper(cf22.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility AS cf2 ON trim(cf2.facility_mnemonic_cs) = trim(stg.launch_patient_facility_code)
           AND upper(cf2.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm_base_views.clinical_acctkeys AS ck ON upper(ck.coid) = upper(cf2.coid)
           AND ck.pat_acct_num = ROUND(CASE
             trim(coalesce(bqutil.fn.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999'))
            WHEN '' THEN NUMERIC '0'
            ELSE CAST(trim(coalesce(bqutil.fn.cw_regexp_extract(stg.launch_patient_visitnumber, '[0-9]+'), '999999999999')) as NUMERIC)
          END, 0, 'ROUND_HALF_EVEN')
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS rmu2 ON rmu2.rdc_sid = rdc.rdc_sid
           AND rmu2.mhb_unit_id = stg.unit_id
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf41 ON trim(cf41.facility_mnemonic_cs) = trim(stg.launch_patient_facility_code)
           AND upper(cf41.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility_location AS cfl5 ON trim(cfl5.location_mnemonic_cs) = trim(substr(stg.unitcode, strpos(stg.unitcode, '_') + 1))
           AND upper(cfl5.coid) = upper(cf41.coid)
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS rmu51 ON rmu51.rdc_sid = rdc.rdc_sid
           AND rmu51.mhb_unit_id = stg.launch_unit_id
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf53 ON trim(cf53.facility_mnemonic_cs) = trim(stg.launch_facilitycode)
           AND upper(cf53.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility_location AS cfl2 ON trim(cfl2.location_mnemonic_cs) = trim(substr(stg.recipientunitcode, strpos(stg.recipientunitcode, '_') + 1))
           AND upper(cfl2.coid) = upper(cf53.coid)
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS u11 ON upper(stg.recipient_user_name) = upper(u11.user_login_name)
           AND u11.rdc_sid = rdc.rdc_sid
          LEFT OUTER JOIN -- AND U11.Active_DW_Ind = 'Y'
          `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_user_role AS r11 ON upper(coalesce(trim(stg.recipient_user_role), 'Unknown')) = upper(trim(r11.mhb_user_role_desc))
           AND r11.mhb_user_role_sid = u11.mhb_user_role_sid
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf10 ON trim(cf10.facility_mnemonic_cs) = trim(stg.recipient_facilitycode)
           AND upper(cf10.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility_location AS cfl20 ON trim(cfl20.location_mnemonic_cs) = trim(substr(stg.recipientunitcode, strpos(stg.recipientunitcode, '_') + 1))
           AND upper(cfl20.coid) = upper(cf10.coid)
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS rmu22 ON rmu22.rdc_sid = rdc.rdc_sid
           AND stg.recipientunit_id = rmu22.mhb_unit_id
        WHERE DATE(stg.launch_datetime) BETWEEN u10.eff_from_date AND u10.eff_to_date
         AND DATE(stg.launch_datetime) BETWEEN u11.eff_from_date AND u11.eff_to_date
        GROUP BY 1, upper(u10.user_login_name), 3, 4, upper(coalesce(ck.company_code, 'H')), upper(coalesce(ck.coid, '99999')), 7, 8, 9, upper(coalesce(cf22.coid, '99999')), 11, 12, 13, upper(stg.launch_app_name), upper(stg.launch_action), upper(stg.launch_status), 17, upper(coalesce(cf41.coid, '99999')), 19, 20, upper(coalesce(cf53.coid, '99999')), 22, 23, upper(stg.recipient_phone_number), upper(u11.user_login_name), upper(coalesce(cf10.coid, '99999')), 27, 28, 29, 30, 31
    ) AS a
;
