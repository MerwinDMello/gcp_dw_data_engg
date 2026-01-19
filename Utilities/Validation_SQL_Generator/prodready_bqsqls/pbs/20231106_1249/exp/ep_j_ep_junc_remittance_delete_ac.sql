-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_junc_remittance_delete_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT DISTINCT pmt.check_num_an AS check_num_an,
                   pmt.check_date AS check_date,
                   pmt.check_amt AS check_amt,
                   pmt.interchange_sender_id AS interchange_sender_id,
                   pmt.provider_level_adj_id AS provider_adjustment_id,
                   clm.payment_guid AS payment_guid,
                   clm.claim_guid AS claim_guid,
                   svc.service_guid AS service_guid,
                   clm.delete_date AS delete_date,
                   pmt.coid AS coid,
                   pmt.unit_num AS unit_num,
                   clm.company_code AS company_code,
                   'E' AS source_system_code,
                   timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_pbs_base_views_dataset_name }}.remittance_claim AS clm
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')
   INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.remittance_payment AS pmt
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(clm.payment_guid) = upper(pmt.payment_guid)
   INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.remittance_service AS svc
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(svc.claim_guid) = upper(clm.claim_guid)) AS a