-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/act/j_mhb_wctp_inbound_message_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT mhb_wctp_inbound_message.*
   FROM {{ params.param_clinical_ci_core_dataset_name }}.mhb_wctp_inbound_message
   WHERE mhb_wctp_inbound_message.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE ) AS q