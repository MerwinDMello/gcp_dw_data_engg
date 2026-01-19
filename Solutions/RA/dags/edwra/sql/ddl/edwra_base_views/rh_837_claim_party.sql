-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_claim_party.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_claim_party
   OPTIONS(description='The RH_837_Claim_Party table will hold specific data elements pertaining to the Patient and Responsible Party.')
  AS SELECT
      rh_837_claim_party.claim_id,
      rh_837_claim_party.claim_party_code,
      rh_837_claim_party.patient_dw_id,
      rh_837_claim_party.company_code,
      rh_837_claim_party.coid,
      rh_837_claim_party.pat_acct_num,
      rh_837_claim_party.party_last_name,
      rh_837_claim_party.party_first_name,
      rh_837_claim_party.party_full_name,
      rh_837_claim_party.party_address1,
      rh_837_claim_party.party_address2,
      rh_837_claim_party.party_city_name,
      rh_837_claim_party.party_state_code,
      rh_837_claim_party.party_zip_code,
      rh_837_claim_party.party_birth_date,
      rh_837_claim_party.party_gender_code,
      rh_837_claim_party.source_system_code,
      rh_837_claim_party.dw_last_update_date_time
    FROM
      {{ params.param_auth_base_views_dataset_name }}.rh_837_claim_party
  ;
