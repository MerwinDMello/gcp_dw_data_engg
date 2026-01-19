-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/pa_j_pf_ref_ada_facility_metric_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(CAST(sum(ref_ada_facility_metric.ada_pct) AS STRING), CAST(sum(ref_ada_facility_metric.secondary_pct) AS STRING)) AS source_string
FROM {{ params.param_pbs_base_views_dataset_name }}.ref_ada_facility_metric
WHERE ref_ada_facility_metric.eff_to_date = DATE '9999-12-31'
  AND upper(ref_ada_facility_metric.unit_num) NOT IN('27102',
                                                     '00631',
                                                     '00460',
                                                     '00472')