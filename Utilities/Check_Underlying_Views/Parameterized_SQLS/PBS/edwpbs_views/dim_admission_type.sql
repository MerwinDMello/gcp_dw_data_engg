-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_admission_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_admission_type AS SELECT
    a.admission_type_sid,
    a.aso_bso_storage_code,
    a.admission_type_parent,
    a.admission_type_child,
    a.admission_type_alias_name,
    a.sort_key_num,
    a.two_pass_calc_code,
    a.consolidation_code,
    a.storage_code,
    a.formula_text,
    a.member_solve_order_num,
    a.admission_type_hier_name,
    a.admission_type_name_uda
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.dim_admission_type AS a
;
