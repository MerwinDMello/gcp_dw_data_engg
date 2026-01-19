-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.unbilled_inventory AS SELECT
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
    ROUND(fp.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(coalesce(subq.unbilled_charge_amt, fp.total_billed_charges, CAST(0 as NUMERIC)), 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
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
    format_date('%m/%d/%Y', ppl.claim_submit_date) AS claim_submit_date,
    pat.final_bill_hold_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_patient AS fp
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.person AS p ON fp.patient_person_dw_id = p.person_dw_id
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
          ssi.ssi_current_payor_ind
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
                `hca-hin-dev-cur-parallon`.edwpf_base_views.discharged_unbilled
          ) AS du
          FULL OUTER JOIN (
            SELECT
                sup.patient_dw_id,
                sup.effective_date,
                sup.pat_acct_num,
                max(sup.ssi_unbilled_status_code) AS ssi_unbilled_status_code,
                max(sus.ssi_unbilled_status_desc) AS ssi_unbilled_status_desc,
                max(CASE
                  WHEN sup_d.rowcount = 1 THEN sup.ssi_unbilled_reason_code
                  ELSE '*****'
                END) AS ssi_unbilled_reason_code,
                max(CASE
                  WHEN sup_d.rowcount = 1 THEN sur.ssi_unbilled_reason_desc
                  ELSE 'Click here for more details'
                END) AS ssi_unbilled_reason_desc,
                max(sur.ssi_unbilled_reason_type) AS ssi_unbilled_reason_type,
                max(sup.ssi_acct_type_desc) AS ssi_acct_type_desc,
                sup.ssi_queue_dept_id,
                max(srd.ssi_queue_dept_desc) AS ssi_queue_dept_desc,
                sup.ssi_current_payor_ind
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_base_views.ssi_unbilled_patient AS sup
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_patient AS fp_0 ON sup.patient_dw_id = fp_0.patient_dw_id
                INNER JOIN (
                  SELECT
                      sup_0.patient_dw_id,
                      sup_0.effective_date,
                      sup_0.pat_acct_num,
                      max(sup_0.ssi_unbilled_status_code) AS ssi_unbilled_status_code,
                      max(sup_0.ssi_acct_type_desc) AS ssi_acct_type_desc,
                      sup_0.ssi_current_payor_ind,
                      max(sup_0.ssi_unbilled_reason_code) AS ssi_unbilled_reason_code,
                      count(*) AS rowcount
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpf_base_views.ssi_unbilled_patient AS sup_0
                      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_patient AS fp_1 ON sup_0.patient_dw_id = fp_1.patient_dw_id
                    WHERE sup_0.ssi_current_payor_ind = 1.0
                     AND fp_1.final_bill_date <> DATE '0001-01-01'
                     AND (upper(sup_0.ssi_unbilled_reason_code) NOT IN(
                      'NP', 'DP'
                    )
                     OR sup_0.ssi_unbilled_reason_code IS NULL)
                    GROUP BY 1, 2, 3, upper(sup_0.ssi_unbilled_status_code), upper(sup_0.ssi_acct_type_desc), 6
                ) AS sup_d ON sup_d.patient_dw_id = sup.patient_dw_id
                 AND sup_d.effective_date = sup.effective_date
                 AND sup_d.pat_acct_num = sup.pat_acct_num
                 AND upper(sup_d.ssi_unbilled_status_code) = upper(sup.ssi_unbilled_status_code)
                 AND upper(sup_d.ssi_acct_type_desc) = upper(sup.ssi_acct_type_desc)
                 AND sup_d.ssi_current_payor_ind = sup.ssi_current_payor_ind
                 AND coalesce(sup_d.ssi_unbilled_reason_code, format('%4d', 0)) = coalesce(sup.ssi_unbilled_reason_code, format('%4d', 0))
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_ssi_responsible_dept AS srd ON sup.ssi_queue_dept_id = srd.ssi_queue_dept_id
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_ssi_unbilled_reason AS sur ON upper(sup.ssi_unbilled_reason_code) = upper(sur.ssi_unbilled_reason_code)
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_ssi_unbilled_status AS sus ON upper(sup.ssi_unbilled_status_code) = upper(sus.ssi_unbilled_status_code)
              WHERE sup.ssi_current_payor_ind = 1.0
               AND fp_0.final_bill_date <> DATE '0001-01-01'
               AND (upper(sup.ssi_unbilled_reason_code) NOT IN(
                'NP', 'DP'
              )
               OR sup.ssi_unbilled_reason_code IS NULL)
              GROUP BY 1, 2, 3, upper(sup.ssi_unbilled_status_code), upper(sus.ssi_unbilled_status_desc), upper(CASE
                WHEN sup_d.rowcount = 1 THEN sup.ssi_unbilled_reason_code
                ELSE '*****'
              END), upper(CASE
                WHEN sup_d.rowcount = 1 THEN sur.ssi_unbilled_reason_desc
                ELSE 'Click here for more details'
              END), upper(sur.ssi_unbilled_reason_type), upper(sup.ssi_acct_type_desc), 10, upper(srd.ssi_queue_dept_desc), 12
          ) AS ssi ON du.patient_dw_id = ssi.patient_dw_id
           AND du.effective_date = ssi.effective_date
    ) AS subq ON fp.patient_dw_id = subq.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.payor_prorated_liability AS ppl ON ppl.patient_dw_id = fp.patient_dw_id
     AND ppl.payor_dw_id = fp.payor_dw_id_ins1
     AND ppl.iplan_insurance_order_num = 1
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.admission_discharge AS pat ON pat.patient_dw_id = fp.patient_dw_id
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS sf ON upper(fp.coid) = upper(sf.co_id)
     AND upper(fp.company_code) = upper(sf.company_code)
     AND sf.user_id = session_user()
  WHERE subq.patient_dw_id IS NOT NULL
;
