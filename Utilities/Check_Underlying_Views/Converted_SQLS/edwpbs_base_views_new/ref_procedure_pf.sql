-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_procedure_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_procedure_pf AS SELECT
    a.procedure_code,
    a.procedure_type_code,
    a.procedure_desc,
    a.eff_from_date,
    a.eff_to_date,
    a.sex_edit_indicator,
    a.icd9_procedure_code_formatted,
    a.procedure_long_desc,
    a.recognized_or_ind,
    a.inpatient_only_ind,
    a.icd10_section_code,
    a.icd10_body_system_code,
    a.icd10_root_operation_code,
    a.icd10_body_part_code,
    a.icd10_approach_code,
    a.icd10_device_code,
    a.icd10_qualifier_code,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_procedure AS a
;
