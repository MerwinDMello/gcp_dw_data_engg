-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/pc_j_pf_fact_disc_costyear_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', a.row_count) AS source_string
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_auth_base_views_dataset_name }}.dim_organization AS dorg
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility AS fac ON CASE fac.unit_num
                                                                                          WHEN '' THEN 0.0
                                                                                          ELSE CAST(fac.unit_num AS FLOAT64)
                                                                                      END = dorg.org_sid
   WHERE upper(substr(dorg.org_name_child, 1, 1)) = 'Z'
     AND upper(dorg.org_hier_name) LIKE 'AR ORG HIER%' ) AS a