-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/facility_iplan.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.facility_iplan AS SELECT
    a.payor_dw_id,
    a.plan_desc,
    a.payor_name,
    a.major_payor_group_id,
    a.coid,
    a.company_code,
    a.iplan_id,
    a.eff_from_date,
    a.eff_to_date,
    a.iplan_financial_class_code,
    a.iplan_inactive_date,
    a.payment_model_num,
    a.payor_auth_length_stay_days,
    a.length_of_stay_extension_num,
    a.verification_ind,
    a.precertification_ind,
    a.recertification_day_count,
    a.pro_review_ind,
    a.log_id,
    a.charge_benefit_code,
    a.op_coinsurance_code,
    a.emc_process_code,
    a.separate_ub82_ind,
    a.hold_claim_ind,
    a.ub92_format_code,
    a.b1500_format_code,
    a.b1500_coding_scheme_code,
    a.ancillary_chg_incl_room_ind,
    a.ancillary_maximum_amt,
    a.processor_id,
    a.processor_sub_id,
    a.accelerated_billing_code,
    a.source_system_code,
    a.bill_code,
    a.health_plan_id,
    a.inpatient_npi,
    a.outpatient_npi,
    a.hosp_acquired_cond_ind,
    a.inpatient_taxonomy_code,
    a.outpatient_taxonomy_code,
    a.admit_disch_date_flag,
    a.outpatient_pps_flag,
    a.outpatient_pps_flag_tricare,
    a.outpatient_apc_edit_ind,
    a.federal_tax_identifier,
    a.subunit_tax_identifier,
    a.type_of_claim_code,
    a.sub_payor_group_id,
    a.meditech_mnemonic,
    a.inpatient_provider_number,
    a.outpatient_provider_number,
    a.part_b_ind
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS a
    CROSS JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b
  WHERE rtrim(b.co_id) = rtrim(a.coid)
   AND rtrim(b.company_code) = rtrim(a.company_code)
   AND rtrim(b.user_id) = rtrim(session_user())
;
