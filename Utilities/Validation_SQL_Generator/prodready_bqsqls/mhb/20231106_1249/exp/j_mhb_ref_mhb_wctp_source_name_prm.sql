-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_wctp_source_name_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY upper(a.source_name)) +
     (SELECT coalesce(max(ref_mhb_wctp_source_name.wctp_source_name_sid), 0)
      FROM {{ params.param_clinical_ci_core_dataset_name }}.ref_mhb_wctp_source_name
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS wctp_source_name_sid,
                            a.source_name AS wctp_source_name,
                            'B' AS source_system_code,
                            timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT vwwctpinboundmessages.source_name
      FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwwctpinboundmessages
      WHERE upper(vwwctpinboundmessages.source_name) NOT IN
          (SELECT upper(ref_mhb_wctp_source_name.wctp_source_name) AS wctp_source_name
           FROM {{ params.param_clinical_ci_core_dataset_name }}.ref_mhb_wctp_source_name
           FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a) AS wrk