-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_wctp_outbound_message_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT rdc.rdc_sid,
          stg.message_id,
          max(coalesce(cf.company_code, 'H')) AS company_code,
          max(coalesce(cf.coid, '99999')) AS coid,
          max(u.user_login_name) AS user_login_name,
          max(coalesce(cf2.coid, '99999')) AS sender_coid,
          coalesce(cfl.location_mnemonic_cs, 'Unknown') AS sender_location_mnemonic_cs,
          rmu.mhb_unit_id AS sender_mhb_unit_id,
          usr.mhb_user_role_sid AS sender_mhb_user_role_sid,
          max(usr.user_login_name) AS recipient_user_login_name,
          max(coalesce(cf.coid, '99999')) AS recipient_coid,
          coalesce(cfl2.location_mnemonic_cs, 'Unknown') AS recipient_location_mnemonic_cs,
          rmu2.mhb_unit_id AS recipient_mhb_unit_id,
          usr.mhb_user_role_sid,
          coalesce(ref.wctp_destination_name_sid, -1) AS wctp_destination_name_sid,
          stg.sent_date_time AS message_sent_date_time,
          stg.delivered_date_time AS message_delivered_date_time,
          stg.read_date_time AS message_read_date_time,
          max(stg.messagepayload) AS message_payload_text,
          stg.acceptedstatus AS accepted_status_sw,
          stg.inferredupdate AS inferred_update_num,
          stg.lasttimestamp AS mhb_last_update_date_time,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwwctpoutboundmessages AS stg
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(substr(trim(stg.databasename), strpos(trim(stg.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
   LEFT OUTER JOIN -- ON OREPLACE(TRIM(A.DATABASENAME),'HEARTBEATDW_','') = TRIM(RDC.RDC_DESC)
 {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf.facility_mnemonic_cs) = trim(stg.recipient_facilitycode)
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS u
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.sender_username) = upper(u.user_login_name)
   AND rdc.rdc_sid = u.rdc_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS r
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(trim(stg.sender_role), 'Unknown')) = upper(trim(r.mhb_user_role_desc))
   AND r.mhb_user_role_sid = u.mhb_user_role_sid
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS usr
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.recipient_username) = upper(usr.user_login_name)
   AND rdc.rdc_sid = usr.rdc_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS ur
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(trim(stg.recipient_role), 'Unknown')) = upper(trim(ur.mhb_user_role_desc))
   AND ur.mhb_user_role_sid = usr.mhb_user_role_sid
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf2.facility_mnemonic_cs) = trim(stg.sender_facilitycode)
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl.location_mnemonic_cs) = trim(substr(stg.sender_unit_code, strpos(stg.sender_unit_code, '_') + 1))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu.rdc_sid = rdc.rdc_sid
   AND stg.sender_unit_id = rmu.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl2.location_mnemonic_cs) = trim(substr(stg.recipient_unit_code, strpos(stg.recipient_unit_code, '_') + 1))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu2.rdc_sid = rdc.rdc_sid
   AND stg.recipient_unit_id = rmu2.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_wctp_destination_name AS REF
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.destinationname, 'Unknown')) = upper(ref.wctp_destination_name)
   WHERE DATE(stg.sent_date_time) BETWEEN u.eff_from_date AND u.eff_to_date
     AND DATE(stg.sent_date_time) BETWEEN usr.eff_from_date AND usr.eff_to_date
   GROUP BY 1,
            2,
            upper(coalesce(cf.company_code, 'H')),
            upper(coalesce(cf.coid, '99999')),
            upper(u.user_login_name),
            upper(coalesce(cf2.coid, '99999')),
            7,
            8,
            9,
            upper(usr.user_login_name),
            upper(coalesce(cf.coid, '99999')),
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            upper(stg.messagepayload),
            20,
            21,
            22,
            23) AS a