-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ref_housing_type.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(dis.housing_type_name)) + coalesce(
                                                                                          (SELECT max(coalesce(a.housing_type_id, 0)) AS max_key
                                                                                           FROM {{ params.param_cr_base_views_dataset_name }}.ref_housing_type AS a), 0) AS housing_type_id,
                                     trim(dis.housing_type_name) AS housing_type_name,
                                     'N' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT trim(st.housing_type_name) AS housing_type_name
      FROM {{ params.param_cr_stage_dataset_name }}.ref_housing_type_stg AS st
      WHERE st.housing_type_name IS NOT NULL ) AS dis
   WHERE NOT EXISTS
       (SELECT 1
        FROM {{ params.param_cr_base_views_dataset_name }}.ref_housing_type AS rcdc
        WHERE upper(trim(rcdc.housing_type_name)) = upper(trim(dis.housing_type_name)) ) ) AS a