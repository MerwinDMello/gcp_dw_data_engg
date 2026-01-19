-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fte_bilirubin_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fte_bilirubin_dtl AS SELECT
    fd.coid,
    fd.unit_num,
    pa.pat_acct_num,
    pa.patient_type_code_pos1 AS pat_type,
    pa.financial_class_code,
    pa.admission_date,
    pa.discharge_date,
    pa.final_bill_date,
    pa.iplan_id_ins1 AS ins1_iplan_id,
    ip1.iplan_financial_class_code AS ins1_financial_class_code,
    ip1.plan_desc AS plan_desc1,
    pa.iplan_id_ins2 AS ins2_iplan_id,
    ip2.iplan_financial_class_code AS ins2_financial_class_code,
    ip2.plan_desc AS plan_desc2,
    pa.iplan_id_ins3 AS ins3_iplan_id,
    ip3.iplan_financial_class_code AS ins3_financial_class_code,
    ip3.plan_desc AS plan_desc3,
    pa.account_status_code,
    pa.total_billed_charges AS total_charge_amt,
    pa.total_account_balance_amt AS acct_bal,
    pa.total_payments AS total_pmt_amt,
    t1.totbilirubinchgs,
    rh.bill_date AS last_claim_date,
    pcc.condition_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS pa
    INNER JOIN (
      SELECT
          rs.patient_dw_id,
          rs.coid,
          rs.pat_acct_num,
          sum(CASE
            WHEN rs.revenue_code BETWEEN 300 AND 319 THEN rs.total_charge_amt
            ELSE CAST(0 as NUMERIC)
          END) AS totbilirubinchgs
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.patient_charge_rev_code_smry AS rs
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS pa_0 ON rs.patient_dw_id = pa_0.patient_dw_id
        WHERE rs.revenue_code BETWEEN 300 AND 319
         AND pa_0.patient_type_code_pos1 = 'I'
        GROUP BY 1, 2, 3
        HAVING totbilirubinchgs <> 0
    ) AS t1 ON pa.coid = t1.coid
     AND pa.pat_acct_num = t1.pat_acct_num
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.person AS p ON p.person_dw_id = pa.patient_person_dw_id
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS fd ON fd.coid = pa.coid
     AND fd.ssc_alias_code NOT IN(
      'CIN', 'UNK'
    )
     AND upper(fd.division_alias_name) <> 'SOLD AND CLOSED'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_iplan AS ip1 ON pa.iplan_id_ins1 = ip1.iplan_id
     AND fd.coid = ip1.coid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_iplan AS ip2 ON pa.iplan_id_ins2 = ip2.iplan_id
     AND fd.coid = ip2.coid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_iplan AS ip3 ON pa.iplan_id_ins3 = ip3.iplan_id
     AND fd.coid = ip3.coid
    INNER JOIN (
      SELECT
          lb.coid,
          lb.pat_acct_num,
          lb.bill_date,
          row_number() OVER (PARTITION BY lb.coid, lb.pat_acct_num ORDER BY lb.bill_date DESC) AS rn
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.rh_837_claim AS lb
        QUALIFY rn = 1
    ) AS rh ON rh.coid = fd.coid
     AND rh.pat_acct_num = pa.pat_acct_num
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.patient_condition_code AS pcc ON pcc.coid = pa.coid
     AND pcc.pat_acct_num = pa.pat_acct_num
     AND pcc.condition_code = 'NB'
  WHERE upper(fd.company_code) = 'H'
   AND pa.admission_date = p.person_birth_date
;
