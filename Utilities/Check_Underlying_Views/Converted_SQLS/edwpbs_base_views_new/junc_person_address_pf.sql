-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_person_address_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_person_address_pf AS SELECT
    junc_person_address.person_dw_id,
    junc_person_address.address_type_code,
    junc_person_address.address_dw_id,
    junc_person_address.length_of_residency_code,
    junc_person_address.bad_address_ind,
    junc_person_address.bad_address_ind_date,
    junc_person_address.bad_address_release_date,
    junc_person_address.eff_from_date,
    junc_person_address.eff_to_date,
    junc_person_address.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.junc_person_address
;
