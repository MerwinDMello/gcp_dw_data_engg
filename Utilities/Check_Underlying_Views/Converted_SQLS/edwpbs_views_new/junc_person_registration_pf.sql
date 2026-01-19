-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_person_registration_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_person_registration_pf AS SELECT
    junc_person_registration_pf.person_dw_id,
    junc_person_registration_pf.coid,
    junc_person_registration_pf.company_code,
    junc_person_registration_pf.patient_dw_id,
    junc_person_registration_pf.person_role_code,
    junc_person_registration_pf.pat_relationship_code,
    junc_person_registration_pf.eff_from_date,
    junc_person_registration_pf.eff_to_date,
    junc_person_registration_pf.pat_acct_num,
    junc_person_registration_pf.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_person_registration_pf
;
