-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_admission_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_admission_type AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpbs.dim_admission_type
;
