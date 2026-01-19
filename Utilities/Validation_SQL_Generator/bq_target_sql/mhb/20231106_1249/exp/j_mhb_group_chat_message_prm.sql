-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_group_chat_message_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          rdc.rdc_sid,
          max(u10.user_login_name) AS recipient_user_login_name,
          stg.messages_id AS mhb_group_chat_message_id,
          stg.sent_date_time AS message_sent_date_time,
          coalesce(ck.patient_dw_id, NUMERIC '999999999999999999') AS patient_dw_id,
          max(coalesce(ck.company_code, 'H')) AS company_code,
          max(coalesce(ck.coid, '99999')) AS coid,
          max(u20.user_login_name) AS sender_user_login_name,
          rmu2.mhb_unit_id AS sender_mhb_unit_id,
          max(coalesce(cf22.coid, '99999')) AS sender_coid,
          coalesce(cfl5.location_mnemonic_cs, 'Unknown') AS sender_location_mnemonic_cs,
          u20.mhb_user_role_sid AS sender_mhb_user_role_sid,
          max(stg.sender_platform) AS sender_platform_name,
          max(stg.group_chat_name) AS group_chat_name,
          max(stg.group_chat_displayname) AS group_chat_display_name,
          max(CASE
             upper(stg.quickpick)
            WHEN 'TRUE' THEN 'Y'
            WHEN 'FALSE' THEN 'N'
          END) AS quick_pick_ind,
          max(CASE
             upper(stg.urgent)
            WHEN 'TRUE' THEN 'Y'
            WHEN 'FALSE' THEN 'N'
          END) AS urgent_message_ind,
          ROUND(CASE
             trim(coalesce(bqutil.fn.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999'))
            WHEN '' THEN NUMERIC '0'
            ELSE CAST(trim(coalesce(bqutil.fn.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999')) as NUMERIC)
          END, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
          max(coalesce(cf41.coid, '99999')) AS patient_coid,
          u10.mhb_user_role_sid AS recipient_mhb_user_role_sid,
          rmu51.mhb_unit_id AS recipient_mhb_unit_id,
          coalesce(cfl2.location_mnemonic_cs, 'Unknown') AS recipient_location_mnemonic_cs,
          max(coalesce(cf53.coid, '99999')) AS recipient_coid,
          stg.inferredupdate AS inferred_update_sw,
          stg.lasttimestamp AS mhb_last_enter_date_time,
          -- TRIM(REGEXP_REPLACE(STG.MESSAGE_CONTENT, '[^a-z A-Z 0-9 ! ?]','',1,0,'i')) AS Message_Content_Text,
          max(trim(stg.message_content)) AS message_content_text,
          stg.delivered_date_time AS message_delivered_date_time,
          stg.read_date_time AS message_read_date_time,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-clinical`.edwci_staging.vwgroupchatmessages AS stg
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(substr(trim(stg.databasename), strpos(trim(stg.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS u10 ON upper(stg.recipient_username) = upper(u10.user_login_name)
           AND rdc.rdc_sid = u10.rdc_sid
          LEFT OUTER JOIN -- AND U10.Active_DW_Ind = 'Y'
          `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_user_role AS r10 ON upper(coalesce(trim(stg.recipient_role), 'Unknown')) = upper(trim(r10.mhb_user_role_desc))
          LEFT OUTER JOIN -- and R10.MHB_User_Role_Sid = U10.MHB_User_Role_Sid
          `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility AS cf2 ON trim(cf2.facility_mnemonic_cs) = stg.patient_facility_code
           AND upper(cf2.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm_base_views.clinical_acctkeys AS ck ON upper(ck.coid) = upper(cf2.coid)
           AND ck.pat_acct_num = ROUND(CASE
             trim(coalesce(bqutil.fn.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999'))
            WHEN '' THEN NUMERIC '0'
            ELSE CAST(trim(coalesce(bqutil.fn.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999')) as NUMERIC)
          END, 0, 'ROUND_HALF_EVEN')
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS u20 ON upper(stg.sender_username) = upper(u20.user_login_name)
           AND rdc.rdc_sid = u20.rdc_sid
          LEFT OUTER JOIN -- AND U20.Active_DW_Ind = 'Y'
          `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_user_role AS r20 ON upper(coalesce(trim(stg.sender_role), 'Unknown')) = upper(trim(r20.mhb_user_role_desc))
          INNER JOIN -- and R20.MHB_User_Role_Sid = U20.MHB_User_Role_Sid
          `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS rmu2 ON rmu2.rdc_sid = rdc.rdc_sid
           AND stg.sender_unit_id = rmu2.mhb_unit_id
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf22 ON trim(cf22.facility_mnemonic_cs) = trim(stg.sender_facilitycode)
           AND upper(cf22.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf41 ON trim(cf41.facility_mnemonic_cs) = trim(stg.patient_facility_code)
           AND upper(cf41.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility_location AS cfl5 ON trim(cfl5.location_mnemonic_cs) = trim(substr(stg.sender_unit_code, strpos(stg.sender_unit_code, '_') + 1))
           AND upper(cfl5.coid) = upper(cf41.coid)
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS rmu51 ON rmu51.rdc_sid = rdc.rdc_sid
           AND stg.recipient_unit_id = rmu51.mhb_unit_id
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf53 ON trim(cf53.facility_mnemonic_cs) = trim(stg.recipient_facilitycode)
           AND upper(cf53.facility_active_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility_location AS cfl2 ON trim(cfl2.location_mnemonic_cs) = trim(substr(stg.recipient_unit_code, strpos(stg.recipient_unit_code, '_') + 1))
           AND upper(cfl2.coid) = upper(cf53.coid)
        WHERE DATE(stg.sent_date_time) BETWEEN u10.eff_from_date AND u10.eff_to_date
         AND DATE(stg.sent_date_time) BETWEEN u20.eff_from_date AND u20.eff_to_date
        GROUP BY 1, upper(u10.user_login_name), 3, 4, 5, upper(coalesce(ck.company_code, 'H')), upper(coalesce(ck.coid, '99999')), upper(u20.user_login_name), 9, upper(coalesce(cf22.coid, '99999')), 11, 12, upper(stg.sender_platform), upper(stg.group_chat_name), upper(stg.group_chat_displayname), upper(CASE
           upper(stg.quickpick)
          WHEN 'TRUE' THEN 'Y'
          WHEN 'FALSE' THEN 'N'
        END), upper(CASE
           upper(stg.urgent)
          WHEN 'TRUE' THEN 'Y'
          WHEN 'FALSE' THEN 'N'
        END), 18, upper(coalesce(cf41.coid, '99999')), 20, 21, 22, upper(coalesce(cf53.coid, '99999')), 24, 25, upper(trim(stg.message_content)), 27, 28, 29, 30
    ) AS a
;
