-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/rh_837_claim_party.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.rh_837_claim_party
   OPTIONS(description='The RH_837_Claim_Party table will hold specific data elements pertaining to the Patient and Responsible Party.')
  AS SELECT
      a.claim_id,
      a.claim_party_code,
      a.patient_dw_id,
      a.company_code,
      a.coid,
      a.pat_acct_num,
      CASE
        WHEN rtrim(session_user()) = rtrim(pn.userid) THEN '***'
        ELSE a.party_last_name
      END AS party_last_name,
      CASE
        WHEN rtrim(session_user()) = rtrim(pn.userid) THEN '***'
        ELSE a.party_first_name
      END AS party_first_name,
      CASE
        WHEN rtrim(session_user()) = rtrim(pn.userid) THEN '***'
        ELSE a.party_full_name
      END AS party_full_name,
      a.party_address1,
      a.party_address2,
      a.party_city_name,
      a.party_state_code,
      a.party_zip_code,
      a.party_birth_date,
      a.party_gender_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_claim_party AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edw_sec_base_views.security_mask_and_exception
          WHERE rtrim(security_mask_and_exception.userid) = rtrim(session_user())
           AND rtrim(security_mask_and_exception.masked_column_code) = 'PN'
      ) AS pn ON rtrim(pn.userid) = rtrim(session_user())
  ;
