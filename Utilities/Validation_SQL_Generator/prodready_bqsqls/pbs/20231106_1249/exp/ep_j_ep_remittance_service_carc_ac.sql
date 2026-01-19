-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_remittance_service_carc_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT max(scc.service_guid) AS service_guid,
          max(scc.adj_group_code) AS adj_group_code,
          max(scc.carc_code) AS carc_code,
          scc.audit_date,
          max(scc.delete_ind) AS delete_ind,
          scc.delete_date,
          max(svc.coid) AS coid,
          max(svc.company_code) AS company_code,
          sum(scc.adj_amt) AS adj_amt,
          sum(scc.adj_qty) AS adj_qty,
          max(scc.adj_category) AS adj_category,
          max(scc.cc_adj_group_code) AS cc_adj_group_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
          'E' AS source_system_code
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service_carc AS scc
   LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.remittance_service AS svc
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(svc.service_guid) = upper(scc.service_guid)
   WHERE scc.audit_date =
       (SELECT max(remittance_service_carc.audit_date)
        FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service_carc)
   GROUP BY upper(scc.service_guid),
            upper(scc.adj_group_code),
            upper(scc.carc_code),
            4,
            upper(scc.delete_ind),
            6,
            upper(svc.coid),
            upper(svc.company_code),
            upper(scc.adj_category),
            upper(scc.cc_adj_group_code),
            13,
            14) AS a