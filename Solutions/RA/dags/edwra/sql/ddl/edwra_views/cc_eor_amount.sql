-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_eor_amount.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_eor_amount AS SELECT
    a.company_code,
    a.coid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.log_sequence_num,
    a.eff_from_date,
    a.reimbursement_method_type_code,
    a.amount_category_code,
    a.unit_num,
    a.pat_acct_num,
    a.iplan_id,
    a.eor_reimbursement_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
  WHERE (a.company_code, a.coid) IN(
    SELECT AS STRUCT
        secref_facility.company_code,
        secref_facility.co_id
      FROM
        {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility
      WHERE rtrim(secref_facility.user_id) = rtrim(session_user())
  )
;
