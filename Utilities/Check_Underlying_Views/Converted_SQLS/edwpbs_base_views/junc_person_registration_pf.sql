-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_person_registration_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_person_registration_pf AS SELECT
    ROUND(junc_person_registration.person_dw_id, 0, 'ROUND_HALF_EVEN') AS person_dw_id,
    junc_person_registration.coid,
    junc_person_registration.company_code,
    ROUND(junc_person_registration.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    junc_person_registration.person_role_code,
    junc_person_registration.pat_relationship_code,
    junc_person_registration.eff_from_date,
    junc_person_registration.eff_to_date,
    ROUND(junc_person_registration.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    junc_person_registration.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.junc_person_registration
;
