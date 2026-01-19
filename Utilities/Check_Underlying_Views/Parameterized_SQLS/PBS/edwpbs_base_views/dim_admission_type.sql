-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_admission_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_admission_type AS SELECT
    dim_admission_type.admission_type_sid,
    dim_admission_type.aso_bso_storage_code,
    dim_admission_type.admission_type_parent,
    dim_admission_type.admission_type_child,
    dim_admission_type.admission_type_alias_name,
    dim_admission_type.sort_key_num,
    dim_admission_type.two_pass_calc_code,
    dim_admission_type.consolidation_code,
    dim_admission_type.storage_code,
    dim_admission_type.formula_text,
    dim_admission_type.member_solve_order_num,
    dim_admission_type.admission_type_hier_name,
    dim_admission_type.admission_type_name_uda
  FROM
    {{ params.param_pbs_core_dataset_name }}.dim_admission_type
;
