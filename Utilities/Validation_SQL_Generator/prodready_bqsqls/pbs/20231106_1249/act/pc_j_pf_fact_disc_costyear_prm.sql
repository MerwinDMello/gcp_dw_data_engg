-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/pc_j_pf_fact_disc_costyear_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', a.row_count) AS source_string
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_pbs_stage_dataset_name }}.eis_dcrp_unit_cost_year_dim) AS a