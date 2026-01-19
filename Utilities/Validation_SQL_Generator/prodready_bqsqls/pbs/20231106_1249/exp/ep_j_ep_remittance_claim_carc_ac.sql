-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_remittance_claim_carc_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT max(ccc.claim_guid) AS claim_guid,
          max(ccc.adj_group_code) AS adj_group_code,
          max(ccc.carc_code) AS carc_code,
          ccc.audit_date,
          max(ccc.delete_ind) AS delete_ind,
          ccc.delete_date,
          max(c.coid) AS coid,
          max(c.company_code) AS company_code,
          sum(ccc.adj_amt) AS adj_amt,
          sum(ccc.adj_qty) AS adj_qty,
          max(ccc.adj_category) AS adj_category,
          max(ccc.cc_adj_group_code) AS cc_adj_group_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
          'E' AS source_system_code
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim_carc AS ccc
   LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.remittance_claim AS c
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(c.claim_guid) = upper(ccc.claim_guid)
   WHERE ccc.audit_date =
       (SELECT max(remittance_claim_carc.audit_date)
        FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim_carc)
   GROUP BY upper(ccc.claim_guid),
            upper(ccc.adj_group_code),
            upper(ccc.carc_code),
            4,
            upper(ccc.delete_ind),
            6,
            upper(c.coid),
            upper(c.company_code),
            upper(ccc.adj_category),
            upper(ccc.cc_adj_group_code),
            13,
            14) AS a