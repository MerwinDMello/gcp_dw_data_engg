-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/rh_837_claim_insurance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.rh_837_claim_insurance
   OPTIONS(description='The RH_837_Claim_Insurance table will hold specific data elements pertaining to the Insurance or Payor.')
  AS SELECT
      a.claim_id,
      a.iplan_insurance_order_num,
      a.patient_dw_id,
      a.company_code,
      a.coid,
      a.pat_acct_num,
      a.iplan_id,
      a.payor_name,
      a.health_plan_id,
      a.other_provider_id,
      CASE
        WHEN rtrim(session_user()) = rtrim(so.userid) THEN a.hic_claim_num
        ELSE '***'
      END AS hic_claim_num,
      CASE
        WHEN rtrim(session_user()) = rtrim(pn.userid) THEN '***'
        ELSE a.insured_name
      END AS insured_name,
      a.pat_relationship_to_ins_code,
      a.insured_group_name,
      a.insured_group_num,
      a.treatment_auth_code,
      a.signed_pat_rel_on_file_ind,
      a.signed_assn_benf_on_file_ind,
      a.prior_pmt_smry_amt,
      a.estimated_rmd_due_amt,
      a.document_control_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_claim_insurance AS a
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
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edw_sec_base_views.security_mask_and_exception
          WHERE rtrim(security_mask_and_exception.userid) = rtrim(session_user())
           AND rtrim(security_mask_and_exception.masked_column_code) = 'SSN'
      ) AS so ON rtrim(so.userid) = rtrim(session_user())
  ;
