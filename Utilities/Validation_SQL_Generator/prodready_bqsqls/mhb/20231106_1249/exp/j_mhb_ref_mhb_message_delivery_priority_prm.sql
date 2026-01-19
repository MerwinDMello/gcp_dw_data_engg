-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_message_delivery_priority_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY upper(a.message_delivery_priority)) +
     (SELECT coalesce(max(ref_mhb_message_delivery_priority.message_delivery_priority_sid), 0)
      FROM {{ params.param_clinical_ci_core_dataset_name }}.ref_mhb_message_delivery_priority
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS message_delivery_priority_sid,
                            a.message_delivery_priority AS message_delivery_priority_desc,
                            'B' AS source_system_code,
                            timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT vwwctpinboundmessages.message_delivery_priority
      FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwwctpinboundmessages
      WHERE upper(vwwctpinboundmessages.message_delivery_priority) NOT IN
          (SELECT upper(ref_mhb_message_delivery_priority.message_delivery_priority_desc) AS message_delivery_priority_desc
           FROM {{ params.param_clinical_ci_core_dataset_name }}.ref_mhb_message_delivery_priority
           FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a) AS wrk