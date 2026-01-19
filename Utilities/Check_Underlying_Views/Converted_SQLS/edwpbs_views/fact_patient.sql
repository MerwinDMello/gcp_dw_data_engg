-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_patient.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS SELECT
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.sub_unit_num,
    a.admission_patient_type_code,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.patient_type_code_pos1,
    a.admission_date,
    a.discharge_date,
    a.final_bill_date,
    ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    a.account_status_code,
    a.admission_source_code,
    a.admission_type_code,
    a.drg_code_payor,
    a.drg_code_hcfa,
    a.drg_desc_hcfa,
    ROUND(a.drg_payment_weight_amt, 5, 'ROUND_HALF_EVEN') AS drg_payment_weight_amt,
    a.drg_code_classic,
    a.rdrg_code_pos4,
    CASE
      WHEN session_user() = zz.userid THEN substr(substr(a.social_security_num, 1, 20), length(substr(a.social_security_num, 1, 20)) - 9, 10)
      ELSE '***'
    END AS social_security_num,
    ROUND(a.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
    a.patient_discharge_month_age,
    ROUND(a.patient_address_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_address_dw_id,
    a.patient_zip_code,
    a.resp_party_zip_code,
    ROUND(a.employer_dw_id, 0, 'ROUND_HALF_EVEN') AS employer_dw_id,
    a.facility_physician_num_attend,
    a.facility_physician_num_admit,
    a.facility_physician_num_pcp,
    ROUND(a.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
    a.iplan_id_ins1,
    ROUND(a.ins_contract_id_ins1, 0, 'ROUND_HALF_EVEN') AS ins_contract_id_ins1,
    ROUND(a.major_payor_id_ins1, 0, 'ROUND_HALF_EVEN') AS major_payor_id_ins1,
    a.masterfacts_schema_id,
    a.ins_product_code_ins1,
    a.ins_product_name_ins1,
    ROUND(a.payor_dw_id_ins2, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins2,
    a.iplan_id_ins2,
    ROUND(a.payor_dw_id_ins3, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins3,
    a.iplan_id_ins3,
    a.calculated_los,
    ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    ROUND(a.total_billed_charges, 3, 'ROUND_HALF_EVEN') AS total_billed_charges,
    ROUND(a.total_anc_charges, 3, 'ROUND_HALF_EVEN') AS total_anc_charges,
    ROUND(a.total_rb_charges, 3, 'ROUND_HALF_EVEN') AS total_rb_charges,
    ROUND(a.total_payments, 3, 'ROUND_HALF_EVEN') AS total_payments,
    ROUND(a.total_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
    ROUND(a.total_contract_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_contract_allow_amt,
    ROUND(a.total_write_off_bad_debt_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_bad_debt_amt,
    ROUND(a.total_write_off_other_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_other_amt,
    ROUND(a.total_write_off_other_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_other_adj_amt,
    ROUND(a.total_uninsured_discount_amt, 3, 'ROUND_HALF_EVEN') AS total_uninsured_discount_amt,
    a.last_write_off_date,
    ROUND(a.total_charity_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_charity_write_off_amt,
    a.last_charity_write_off_date,
    ROUND(a.total_patient_liability_amt, 3, 'ROUND_HALF_EVEN') AS total_patient_liability_amt,
    ROUND(a.total_patient_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_patient_payment_amt,
    a.last_patient_payment_date,
    a.exempt_indicator,
    a.casemix_exempt_indicator,
    ROUND(a.expected_pmt, 3, 'ROUND_HALF_EVEN') AS expected_pmt,
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
;
