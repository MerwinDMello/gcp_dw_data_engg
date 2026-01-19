-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_wctp_inbound_message_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT rdc.rdc_sid,
          stg.message_id,
          max(coalesce(cf.company_code, 'H')) AS company_code,
          max(coalesce(cf.coid, '99999')) AS coid,
          max(u.user_login_name) AS user_login_name,
          max(coalesce(cf.coid, '99999')) AS recipient_coid,
          coalesce(cfl.location_mnemonic_cs, 'Unknown') AS recipient_location_mnemonic_cs,
          rmu2.mhb_unit_id AS recipient_mhb_unit_id,
          r.mhb_user_role_sid AS recipient_mhb_user_role_sid,
          max(stg.sender_information) AS sender_information,
          max(stg.sender_message_id) AS sender_message_id,
          max(stg.sender_transaction_id) AS sender_transaction_id,
          CAST(trim(stg.received_date_time) AS DATETIME) AS message_received_date_time,
          CAST(trim(stg.sent_date_time) AS DATETIME) AS message_sent_date_time,
          CASE
              WHEN upper(trim(stg.delivered_date_time)) = '' THEN CAST(NULL AS DATETIME)
              ELSE CAST(trim(stg.delivered_date_time) AS DATETIME)
          END AS message_delivered_date_time,
          CASE
              WHEN upper(trim(stg.read_date_time)) = '' THEN CAST(NULL AS DATETIME)
              ELSE CAST(trim(stg.read_date_time) AS DATETIME)
          END AS message_read_date_time,
          stg.message_status,
          stg.message_parse_status,
          coalesce(mdp.message_delivery_priority_sid, -1) AS message_delivery_priority_sid,
          max(stg.message_payload) AS message_payload_text,
          coalesce(sn.wctp_source_name_sid, -1) AS wctp_source_name_sid,
          coalesce(st.wctp_source_type_sid, -1) AS wctp_source_type_sid,
          coalesce(uat.user_action_type_sid, -1) AS user_action_type_sid,
          stg.notify_when_queued AS notify_when_queued_sw,
          stg.notify_when_delivered AS notify_when_delivered_sw,
          stg.notify_when_read AS notify_when_read_sw,
          stg.inferredupdate AS inferred_update_num,
          max(stg.lasttimestamp) AS mhb_last_update_date_time,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwwctpinboundmessages AS stg
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(substr(trim(stg.databasename), strpos(trim(stg.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
   LEFT OUTER JOIN -- ON OREPLACE(TRIM(A.DATABASENAME),'HEARTBEATDW_','') = TRIM(RDC.RDC_DESC)
 {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf.facility_mnemonic_cs) = trim(stg.recipient_facilitycode)
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS u
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.recipient_username) = upper(u.user_login_name)
   AND rdc.rdc_sid = u.rdc_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS r
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(trim(stg.recipient_role), 'Unknown')) = upper(trim(r.mhb_user_role_desc))
   AND r.mhb_user_role_sid = u.mhb_user_role_sid
   LEFT OUTER JOIN /*Left Join Edwcl_Base_Views.Clinical_Facility CF2 FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time,'US/Central')
          on trim(CF2.Facility_Mnemonic_CS) = trim(stg.Recipient_FacilityCode)*/ {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl.location_mnemonic_cs) = trim(substr(stg.recipient_unit_code, strpos(stg.recipient_unit_code, '_') + 1))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu2.rdc_sid = rdc.rdc_sid
   AND stg.recipient_unit_id = rmu2.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_message_delivery_priority AS mdp
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.message_delivery_priority, 'Unknown')) = upper(mdp.message_delivery_priority_desc)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_wctp_source_name AS sn
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.source_name, 'Unknown')) = upper(sn.wctp_source_name)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_wctp_source_type AS st
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.source_type, 'Unknown')) = upper(st.wctp_source_type_desc)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_action_type AS uat
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.user_action, 'Unknown')) = upper(uat.user_action_type_desc)
   WHERE CAST(trim(substr(stg.received_date_time, 1, 10)) AS DATE) BETWEEN u.eff_from_date AND u.eff_to_date
   GROUP BY 1,
            2,
            upper(coalesce(cf.company_code, 'H')),
            upper(coalesce(cf.coid, '99999')),
            upper(u.user_login_name),
            upper(coalesce(cf.coid, '99999')),
            7,
            8,
            9,
            upper(stg.sender_information),
            upper(stg.sender_message_id),
            upper(stg.sender_transaction_id),
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            upper(stg.message_payload),
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            upper(stg.lasttimestamp),
            29,
            30) AS a