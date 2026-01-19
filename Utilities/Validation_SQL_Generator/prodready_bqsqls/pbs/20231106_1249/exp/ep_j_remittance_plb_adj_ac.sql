-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_remittance_plb_adj_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT a.payment_guid,
          a.audit_date AS audit_date,
          a.adj_code AS adj_reason_code,
          a.adj_id AS adj_ref_id,
          row_number() OVER (PARTITION BY upper(a.payment_guid),
                                          upper(a.adj_code),
                                          a.audit_date,
                                          upper(a.adj_id)
                             ORDER BY upper(a.payment_guid)) AS adj_record_num,
                            a.delete_ind AS delete_ind, -- ,coalesce(delete_date,cast('1999-01-01' as date)) AS Delete_Date
 a.delete_date,
 a.adj_amt,
 a.adj_match AS adj_match_code,
 a.claim_control_num AS ep_calc_claim_control_num,
 a.fiscal_period_date,
 'E' AS source_system_code,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_plb_adj AS a) AS a