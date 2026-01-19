-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_claim_physician.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_claim_physician AS SELECT
    rh_837_claim_physician.claim_id,
    rh_837_claim_physician.role_type_code,
    rh_837_claim_physician.patient_dw_id,
    rh_837_claim_physician.company_code,
    rh_837_claim_physician.coid,
    rh_837_claim_physician.pat_acct_num,
    rh_837_claim_physician.national_provider_id,
    rh_837_claim_physician.physician_last_name,
    rh_837_claim_physician.physician_first_name,
    rh_837_claim_physician.physician_taxonomy_code,
    rh_837_claim_physician.source_system_code,
    rh_837_claim_physician.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.rh_837_claim_physician
;
