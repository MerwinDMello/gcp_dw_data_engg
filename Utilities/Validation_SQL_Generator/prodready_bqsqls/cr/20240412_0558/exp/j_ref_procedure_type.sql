-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_procedure_type.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT procedure_type_stg.procedure_type_desc,
          procedure_type_stg.procedure_sub_type_desc,
          procedure_type_stg.source_system_code
   FROM {{ params.param_cr_stage_dataset_name }}.procedure_type_stg
   WHERE (upper(trim(procedure_type_stg.procedure_type_desc)),
          upper(trim(coalesce(procedure_type_stg.procedure_sub_type_desc, '')))) NOT IN
       (SELECT AS STRUCT upper(trim(ref_procedure_type.procedure_type_desc)),
                         upper(trim(coalesce(ref_procedure_type.procedure_sub_type_desc, '')))
        FROM {{ params.param_cr_base_views_dataset_name }}.ref_procedure_type
        WHERE ref_procedure_type.procedure_type_desc IS NOT NULL )
     AND procedure_type_stg.dw_last_update_date_time = current_date('US/Central') ) AS a