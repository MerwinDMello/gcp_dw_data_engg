-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_patient.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS SELECT
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.sub_unit_num,
    a.admission_patient_type_code,
    a.pat_acct_num,
    a.patient_type_code_pos1,
    a.admission_date,
    a.discharge_date,
    a.final_bill_date,
    a.financial_class_code,
    a.account_status_code,
    a.admission_source_code,
    a.admission_type_code,
    a.drg_code_payor,
    a.drg_code_hcfa,
    a.drg_desc_hcfa,
    a.drg_payment_weight_amt,
    a.drg_code_classic,
    a.rdrg_code_pos4,
    CASE
      WHEN session_user() = zz.userid THEN substr(substr(a.social_security_num, 1, 20), length(substr(a.social_security_num, 1, 20)) - 9, 10)
      ELSE '***'
    END AS social_security_num,
    a.patient_person_dw_id,
    a.patient_discharge_month_age,
    a.patient_address_dw_id,
    a.patient_zip_code,
    a.resp_party_zip_code,
    a.employer_dw_id,
    a.facility_physician_num_attend,
    a.facility_physician_num_admit,
    a.facility_physician_num_pcp,
    a.payor_dw_id_ins1,
    a.iplan_id_ins1,
    a.ins_contract_id_ins1,
    a.major_payor_id_ins1,
    a.masterfacts_schema_id,
    a.ins_product_code_ins1,
    a.ins_product_name_ins1,
    a.payor_dw_id_ins2,
    a.iplan_id_ins2,
    a.payor_dw_id_ins3,
    a.iplan_id_ins3,
    a.calculated_los,
    a.total_account_balance_amt,
    a.total_billed_charges,
    a.total_anc_charges,
    a.total_rb_charges,
    a.total_payments,
    a.total_adjustment_amt,
    a.total_contract_allow_amt,
    a.total_write_off_bad_debt_amt,
    a.total_write_off_other_amt,
    a.total_write_off_other_adj_amt,
    a.total_uninsured_discount_amt,
    a.last_write_off_date,
    a.total_charity_write_off_amt,
    a.last_charity_write_off_date,
    a.total_patient_liability_amt,
    a.total_patient_payment_amt,
    a.last_patient_payment_date,
    a.exempt_indicator,
    a.casemix_exempt_indicator,
    a.expected_pmt,
    a.diag_code_admit,
    a.diag_code_final,
    a.proc_code_01,
    a.proc_code_02,
    a.proc_code_03,
    a.proc_code_04,
    a.proc_code_05,
    a.proc_code_06,
    a.proc_code_07,
    a.proc_code_08,
    a.proc_code_09,
    a.proc_code_10,
    a.proc_code_11,
    a.proc_code_12,
    a.proc_code_13,
    a.proc_code_14,
    a.proc_code_15,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_patient AS a
    LEFT OUTER JOIN (
      SELECT
          sme.userid,
          sme.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec.security_mask_and_exception AS sme
        WHERE session_user() = sme.userid
         AND upper(sme.masked_column_code) = 'SSN'
    ) AS zz ON session_user() = zz.userid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
;
