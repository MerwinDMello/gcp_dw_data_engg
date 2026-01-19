-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/junc_person_registration.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.junc_person_registration AS SELECT
    a.coid,
    a.company_code,
    a.eff_from_date,
    a.eff_to_date,
    a.patient_dw_id,
    a.pat_acct_num,
    a.pat_relationship_code,
    a.person_dw_id,
    a.person_role_code,
    a.source_system_code
  FROM
    {{ params.param_auth_base_views_dataset_name }}.junc_person_registration AS a
;
