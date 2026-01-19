-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/unbilled_inventory_payor_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.unbilled_inventory_payor_dtl AS SELECT
    fp.company_code,
    fp.coid,
    fp.unit_num,
    ROUND(fp.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    CASE
      WHEN session_user() = z.userid THEN '***'
      ELSE p.person_full_name
    END AS person_full_name,
    fp.patient_type_code_pos1,
    format_date('%m/%d/%Y', fp.admission_date) AS admission_date,
    CASE
      WHEN upper(subq.account_process_ind) = 'I' THEN CAST(NULL as STRING)
      ELSE format_date('%m/%d/%Y', fp.discharge_date)
    END AS discharge_date,
    format_date('%m/%d/%Y', CASE
      WHEN fp.final_bill_date = DATE '0001-01-01'
       OR upper(subq.account_process_ind) IN(
        'I', 'D', 'S'
      ) THEN CAST(NULL as DATE)
      ELSE fp.final_bill_date
    END) AS final_bill_date,
    fp.iplan_id_ins1,
    fi.payor_name,
    fi.plan_desc,
    ROUND(fp.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(coalesce(subq.unbilled_charge_amt, fp.total_billed_charges, CAST(0 as NUMERIC)), 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
    ROUND(ROUND(coalesce(subq.unbilled_charge_amt, fp.total_billed_charges, CAST(0 as NUMERIC)) / subq.cnt, 3, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS gross_charge_amt_by_alert,
    ROUND(fp.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    format_date('%Y%m%d', subq.effective_date) AS effective_date,
    subq.account_process_ind,
    subq.unbilled_responsibility_ind,
    subq.ssi_unbilled_status_code,
    subq.ssi_unbilled_status_desc,
    subq.ssi_unbilled_reason_code,
    subq.ssi_unbilled_reason_desc,
    subq.ssi_unbilled_reason_type,
    subq.ssi_acct_type_desc,
    subq.ssi_queue_dept_id AS ssi_responsible_dept_id,
    subq.ssi_queue_dept_desc AS ssi_responsible_dept_desc,
    subq.ssi_current_payor_ind,
    ppl.claim_submit_date AS claim_submit_date,
    pat.final_bill_hold_ind,
    coalesce(pyr.payor_sid, 2) AS payor_sid,
    pyr.payor_gen03,
    pyr.payor_gen03_alias,
    pyr.payor_gen02,
    pyr.payor_gen02_alias,
    coalesce(pfc.payor_financial_class_sid, 24) AS payor_financial_class_sid,
    pfc.payor_financial_class_alias,
    subq.claim_date,
    subq.request_created_date,
    subq.queue_assigned_date,
    subq.last_activity_date,
    subq.type_of_bill_code,
    CASE
      WHEN upper(subq.type_of_bill_code) = '110' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '111' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '112' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '113' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '114' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '115' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '117' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '118' THEN 'Hospital Inpatient(Including Medicare Part A)'
      WHEN upper(subq.type_of_bill_code) = '121' THEN 'Hospital Inpatient(Medicare Part B only)'
      WHEN upper(subq.type_of_bill_code) = '127' THEN 'Hospital Inpatient(Medicare Part B only)'
      WHEN upper(subq.type_of_bill_code) = '130' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '131' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '132' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '133' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '134' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '135' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '137' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '138' THEN 'Hospital Outpatient'
      WHEN upper(subq.type_of_bill_code) = '141' THEN 'Hospital - laboratory Services Provide to Non-Patient'
      WHEN upper(subq.type_of_bill_code) = '147' THEN 'Hospital - laboratory Services Provide to Non-Patient'
      WHEN upper(subq.type_of_bill_code) = '181' THEN 'Hospital-Swing Beds'
      WHEN upper(subq.type_of_bill_code) = '182' THEN 'Hospital-Swing Beds'
      WHEN upper(subq.type_of_bill_code) = '184' THEN 'Hospital-Swing Beds'
      WHEN upper(subq.type_of_bill_code) = '187' THEN 'Hospital-Swing Beds'
      WHEN upper(subq.type_of_bill_code) = '210' THEN 'Skilled Nursing  - Inpatient(Including Medicare part A)'
      WHEN upper(subq.type_of_bill_code) = '211' THEN 'Skilled Nursing  - Inpatient(Including Medicare part A)'
      WHEN upper(subq.type_of_bill_code) = '212' THEN 'Skilled Nursing  - Inpatient(Including Medicare part A)'
      WHEN upper(subq.type_of_bill_code) = '213' THEN 'Skilled Nursing  - Inpatient(Including Medicare part A)'
      WHEN upper(subq.type_of_bill_code) = '214' THEN 'Skilled Nursing  - Inpatient(Including Medicare part A)'
      WHEN upper(subq.type_of_bill_code) = '215' THEN 'Skilled Nursing  - Inpatient(Including Medicare part A)'
      WHEN upper(subq.type_of_bill_code) = '217' THEN 'Skilled Nursing  - Inpatient(Including Medicare part A)'
      WHEN upper(subq.type_of_bill_code) = '221' THEN 'Skilled Nursing  - Inpatient(Including Medicare part B only)'
      WHEN upper(subq.type_of_bill_code) = '231' THEN 'Skilled Nursing - Outpatient'
      WHEN upper(subq.type_of_bill_code) = '721' THEN 'Clinic -Hospital based or Independent Renal Dialysis Center'
      WHEN upper(subq.type_of_bill_code) = '727' THEN 'Clinic -Hospital based or Independent Renal Dialysis Center'
      WHEN upper(subq.type_of_bill_code) = '831' THEN 'Ambulatory Service center'
      WHEN upper(subq.type_of_bill_code) = '851' THEN 'Critical Access Hospital'
      WHEN upper(subq.type_of_bill_code) = '862' THEN 'Residential Facility'
      WHEN upper(subq.type_of_bill_code) = ''
       OR subq.type_of_bill_code IS NULL THEN 'No Bill code'
      WHEN td_sysfnlib.to_number(subq.type_of_bill_code) IS NULL THEN 'Non-Standard Other'
      ELSE ''
    END AS bill_group_desc,
    CASE
      WHEN upper(subq.type_of_bill_code) = '110' THEN 'Non Payment/Zero'
      WHEN upper(subq.type_of_bill_code) = '111' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '112' THEN 'Interim-First claim'
      WHEN upper(subq.type_of_bill_code) = '113' THEN 'Interim-Continuing claim (b)'
      WHEN upper(subq.type_of_bill_code) = '114' THEN 'Interim-Last claim (b)'
      WHEN upper(subq.type_of_bill_code) = '115' THEN 'Late charge(s) only'
      WHEN upper(subq.type_of_bill_code) = '117' THEN 'Replacement of Prior claim (a)'
      WHEN upper(subq.type_of_bill_code) = '118' THEN 'Void/Cancel of Prior Claim (a)'
      WHEN upper(subq.type_of_bill_code) = '121' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '127' THEN 'Replacement of Prior claim (a)'
      WHEN upper(subq.type_of_bill_code) = '130' THEN 'Non Payment/Zero'
      WHEN upper(subq.type_of_bill_code) = '131' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '132' THEN 'Interim-First claim'
      WHEN upper(subq.type_of_bill_code) = '133' THEN 'Interim-Continuing claim (b)'
      WHEN upper(subq.type_of_bill_code) = '134' THEN 'Interim-Last claim (b)'
      WHEN upper(subq.type_of_bill_code) = '135' THEN 'Late charge(s) only'
      WHEN upper(subq.type_of_bill_code) = '137' THEN 'Replacement of Prior claim (a)'
      WHEN upper(subq.type_of_bill_code) = '138' THEN 'Void/Cancel of Prior Claim (a)'
      WHEN upper(subq.type_of_bill_code) = '141' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '147' THEN 'Replacement of Prior claim (a)'
      WHEN upper(subq.type_of_bill_code) = '181' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '182' THEN 'Interim-First claim'
      WHEN upper(subq.type_of_bill_code) = '184' THEN 'Interim-Last claim (b)'
      WHEN upper(subq.type_of_bill_code) = '187' THEN 'Replacement of Prior claim (a)'
      WHEN upper(subq.type_of_bill_code) = '210' THEN 'Non Payment/Zero'
      WHEN upper(subq.type_of_bill_code) = '211' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '212' THEN 'Interim-First claim'
      WHEN upper(subq.type_of_bill_code) = '213' THEN 'Interim-Continuing claim (b)'
      WHEN upper(subq.type_of_bill_code) = '214' THEN 'Interim-Last claim (b)'
      WHEN upper(subq.type_of_bill_code) = '215' THEN 'Late charge(s) only'
      WHEN upper(subq.type_of_bill_code) = '217' THEN 'Replacement of Prior claim (a)'
      WHEN upper(subq.type_of_bill_code) = '221' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '231' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '721' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '727' THEN 'Replacement of Prior claim (a)'
      WHEN upper(subq.type_of_bill_code) = '831' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '851' THEN 'Admit through Discharge claim'
      WHEN upper(subq.type_of_bill_code) = '862' THEN 'Interim-First claim'
      WHEN upper(subq.type_of_bill_code) = ''
       OR subq.type_of_bill_code IS NULL THEN 'No Bill code'
      WHEN td_sysfnlib.to_number(subq.type_of_bill_code) IS NULL THEN 'Non-Standard Other'
      ELSE ''
    END AS bill_code_desc,
    subq.claim_id,
    coalesce(subq.ssi_total_charge_amt, NUMERIC '0.000') AS ssi_total_charge_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient AS fp
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.person AS p ON fp.patient_person_dw_id = p.person_dw_id
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'PN'
    ) AS z ON session_user() = z.userid
    LEFT OUTER JOIN (
      SELECT
          coalesce(du.patient_dw_id, ssi.patient_dw_id) AS patient_dw_id,
          coalesce(du.effective_date, ssi.effective_date) AS effective_date,
          du.account_process_ind,
          du.unbilled_responsibility_ind,
          du.unbilled_charge_amt,
          ssi.ssi_unbilled_status_code,
          ssi.ssi_unbilled_status_desc,
          ssi.ssi_unbilled_reason_code,
          ssi.ssi_unbilled_reason_desc,
          ssi.ssi_unbilled_reason_type,
          ssi.ssi_acct_type_desc,
          ssi.ssi_queue_dept_id,
          ssi.ssi_queue_dept_desc,
          ssi.ssi_current_payor_ind,
          ssi.claim_date,
          ssi.request_created_date,
          ssi.queue_assigned_date,
          ssi.last_activity_date,
          ssi.type_of_bill_code,
          ssi.claim_id,
          coalesce(ssi.ssi_total_charge_amt, NUMERIC '0.000') AS ssi_total_charge_amt,
          coalesce(ssi.cnt, 1) AS cnt
        FROM
          (
            SELECT
                discharged_unbilled.patient_dw_id,
                discharged_unbilled.coid,
                discharged_unbilled.effective_date,
                discharged_unbilled.account_process_ind,
                discharged_unbilled.unbilled_responsibility_ind,
                discharged_unbilled.unbilled_charge_amt
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_base_views.discharged_unbilled
          ) AS du
          FULL OUTER JOIN (
            SELECT
                sup.patient_dw_id,
                sup.effective_date,
                sup.pat_acct_num,
                max(sup.ssi_unbilled_status_code) AS ssi_unbilled_status_code,
                max(sus.ssi_unbilled_status_desc) AS ssi_unbilled_status_desc,
                max(sup.ssi_unbilled_reason_code) AS ssi_unbilled_reason_code,
                max(sur.ssi_unbilled_reason_desc) AS ssi_unbilled_reason_desc,
                max(sur.ssi_unbilled_reason_type) AS ssi_unbilled_reason_type,
                max(sup.ssi_acct_type_desc) AS ssi_acct_type_desc,
                sup.ssi_queue_dept_id,
                srd.ssi_queue_dept_desc,
                sup.ssi_current_payor_ind,
                sup.claim_date,
                sup.request_created_date,
                sup.queue_assigned_date,
                sup.last_activity_date,
                max(sup.type_of_bill_code) AS type_of_bill_code,
                max(sup.claim_id) AS claim_id,
                coalesce(sup.ssi_total_charge_amt, NUMERIC '0.000') AS ssi_total_charge_amt,
                sq.cnt
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_base_views.ssi_unbilled_patient AS sup
                LEFT OUTER JOIN (
                  SELECT
                      ssi_unbilled_patient.pat_acct_num,
                      max(ssi_unbilled_patient.coid) AS coid,
                      ssi_unbilled_patient.effective_date,
                      count(*) AS cnt
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ssi_unbilled_patient
                    GROUP BY 1, upper(ssi_unbilled_patient.coid), 3
                ) AS sq ON sup.pat_acct_num = sq.pat_acct_num
                 AND upper(sup.coid) = upper(sq.coid)
                 AND sup.effective_date = sq.effective_date
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient AS fp_0 ON sup.patient_dw_id = fp_0.patient_dw_id
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ssi_responsible_dept AS srd ON sup.ssi_queue_dept_id = srd.ssi_queue_dept_id
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ssi_unbilled_reason AS sur ON upper(sup.ssi_unbilled_reason_code) = upper(sur.ssi_unbilled_reason_code)
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ssi_unbilled_status AS sus ON upper(sup.ssi_unbilled_status_code) = upper(sus.ssi_unbilled_status_code)
              WHERE sup.ssi_current_payor_ind = 1.0
               AND fp_0.final_bill_date <> DATE '0001-01-01'
               AND (upper(sup.ssi_unbilled_reason_code) NOT IN(
                'NP', 'DP'
              )
               OR sup.ssi_unbilled_reason_code IS NULL)
              GROUP BY 1, 2, 3, upper(sup.ssi_unbilled_status_code), upper(sus.ssi_unbilled_status_desc), upper(sup.ssi_unbilled_reason_code), upper(sur.ssi_unbilled_reason_desc), upper(sur.ssi_unbilled_reason_type), upper(sup.ssi_acct_type_desc), 10, 11, 12, 13, 14, 15, 16, upper(sup.type_of_bill_code), upper(sup.claim_id), 19, 20
          ) AS ssi ON du.patient_dw_id = ssi.patient_dw_id
           AND du.effective_date = ssi.effective_date
    ) AS subq ON fp.patient_dw_id = subq.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_prorated_liability AS ppl ON ppl.patient_dw_id = fp.patient_dw_id
     AND ppl.payor_dw_id = fp.payor_dw_id_ins1
     AND ppl.iplan_insurance_order_num = 1
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_discharge AS pat ON pat.patient_dw_id = fp.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_iplan AS fi ON fp.iplan_id_ins1 = fi.iplan_id
     AND upper(fp.coid) = upper(fi.coid)
    LEFT OUTER JOIN (
      SELECT
          substr(ltrim(format('%#20.0f', rcom_payor_dimension_eom.payor_dw_id)), 1, 20) AS payor_dw_id,
          rcom_payor_dimension_eom.payor_id,
          rcom_payor_dimension_eom.eff_from_date,
          rcom_payor_dimension_eom.eff_to_date
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom
        WHERE rcom_payor_dimension_eom.eff_from_date IS NULL
    ) AS payor_id_null ON upper(substr(ltrim(format('%#20.0f', fp.payor_dw_id_ins1)), 1, 20)) = upper(payor_id_null.payor_dw_id)
    LEFT OUTER JOIN (
      SELECT
          lkp_payor_id.payor_id,
          lkp_payor_id.payor_dw_id,
          lkp_payor_id.eff_from_date,
          lkp_payor_id.eff_to_date
        FROM
          (
            SELECT
                substr(ltrim(format('%#20.0f', rcom_payor_dimension_eom.payor_dw_id)), 1, 20) AS payor_dw_id,
                rcom_payor_dimension_eom.payor_id,
                rcom_payor_dimension_eom.eff_from_date,
                rcom_payor_dimension_eom.eff_to_date
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom
          ) AS lkp_payor_id
        WHERE lkp_payor_id.eff_from_date IS NOT NULL
    ) AS payor_id_nn ON upper(substr(ltrim(format('%#20.0f', fp.payor_dw_id_ins1)), 1, 20)) = upper(payor_id_nn.payor_dw_id)
     AND fp.admission_date BETWEEN payor_id_nn.eff_from_date AND payor_id_nn.eff_to_date
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS pyr ON upper(pyr.payor_member) = upper(coalesce(payor_id_nn.payor_id, payor_id_null.payor_id))
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_financial_class_dim AS pfc ON upper(pfc.payor_financial_class_member) = upper(CASE
       fp.financial_class_code
      WHEN 0 THEN 'PFC0000'
      WHEN 1 THEN 'PFC0101'
      WHEN 2 THEN 'PFC0202'
      WHEN 3 THEN 'PFC0303'
      WHEN 4 THEN 'PFC0404'
      WHEN 5 THEN 'PFC0505'
      WHEN 6 THEN 'PFC0606'
      WHEN 7 THEN 'PFC0707'
      WHEN 8 THEN 'PFC0808'
      WHEN 9 THEN 'PFC0909'
      WHEN 10 THEN 'PFC1810'
      WHEN 11 THEN 'PFC1811'
      WHEN 12 THEN 'PFC1212'
      WHEN 13 THEN 'PFC0713'
      WHEN 14 THEN 'PFC1414'
      WHEN 15 THEN 'PFC1415'
      WHEN 20 THEN 'PFC2000'
      WHEN 99 THEN 'PFC2000'
      ELSE 'PFC_None'
    END)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(fp.coid) = upper(sf.co_id)
     AND upper(fp.company_code) = upper(sf.company_code)
     AND sf.user_id = session_user()
  WHERE subq.patient_dw_id IS NOT NULL
;
