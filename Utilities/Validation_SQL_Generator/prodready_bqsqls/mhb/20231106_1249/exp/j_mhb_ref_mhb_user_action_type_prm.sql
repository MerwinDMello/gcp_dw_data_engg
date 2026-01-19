-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_user_action_type_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT CASE
              WHEN upper(coalesce(y.user_action_type_desc, 'Unknown')) = 'UNKNOWN' THEN -1
              ELSE
                     (SELECT coalesce(max(ref_mhb_user_action_type.user_action_type_sid), 0)
                      FROM {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_action_type
                      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) + row_number() OVER (
                                                                                                                ORDER BY upper(y.user_action_type_desc))
          END AS user_action_type_sid,
          y.user_action_type_desc AS user_action_type_desc,
          'B' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT x.*
      FROM
        (SELECT max(trim(vwwctpinboundmessages.user_action)) AS user_action_type_desc
         FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwwctpinboundmessages
         GROUP BY upper(trim(vwwctpinboundmessages.user_action))) AS x
      WHERE upper(x.user_action_type_desc) NOT IN
          (SELECT /*UNION
                     		SEL 'Unknown' As User_Action_Type_Desc
                     		FROM EDWCI_STAGING.vwWCTPInboundMessages*/ upper(ref_mhb_user_action_type.user_action_type_desc) AS user_action_type_desc
           FROM {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_action_type
           FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS y) AS a