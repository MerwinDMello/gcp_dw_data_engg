-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lc_scripted_potential_overpayments.memory
-- Translated from: Teradata
-- Translated to: BigQuery

DECLARE cw_const BIGNUMERIC DEFAULT (
  SELECT
      max(pass_eom.rptg_period)
    FROM
      `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom
);
DECLARE cw_const_0 BIGNUMERIC DEFAULT (
  SELECT
      max(pass_eom.rptg_period)
    FROM
      `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom
);
CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lc_scripted_potential_overpayments AS SELECT
    CASE
      WHEN upper(org.ssc_coid) = '08591' THEN 'NASHW'
      ELSE upper(substr(org.ssc_name, 1, 5))
    END AS ssc,
    org.unit_num AS unitnum,
    fp.pat_acct_num AS patnum,
    concat(substr(lpad(substr(CAST(fp.iplan_id_ins1 as STRING), 1, 5), 5, '0'), 1, 3), '-', substr(lpad(substr(CAST(fp.iplan_id_ins1 as STRING), 1, 5), 5, '0'), 4, 2)) AS iplan,
    CASE
      WHEN dsc.iplan_insurance_order_num IS NOT NULL THEN dsc.iplan_insurance_order_num
      WHEN cc.iplan_order_num IS NOT NULL THEN cc.iplan_order_num
      ELSE NULL
    END AS payer_rank,
    CASE
      WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN dsc.var_gross_reimbursement_amt
      WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN cc.var_gross_reimbursement_amt
      ELSE CAST(NULL as BIGNUMERIC)
    END AS potentialoverpaymentamt,
    rpdov.parent_payor_name AS majorpayor,
    fp.financial_class_code AS finclass,
    CASE
      WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Legacy'
      WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Concuity'
      ELSE CAST(NULL as STRING)
    END AS sourcesystem,
    CASE
      WHEN dsc.discrepancy_reason_code_1 IS NOT NULL THEN dsc.discrepancy_reason_code_1
      WHEN cc.project_name IS NOT NULL THEN cc.project_name
      ELSE NULL
    END AS project,
    dsc.discrepancy_reason_code_1,
    dsc.discrepancy_reason_code_3,
    CASE
      WHEN lc.late_charge_amt IS NOT NULL THEN 'Y'
      ELSE 'N'
    END AS late_chg_flag,
    CASE
      WHEN upper(CASE
        WHEN upper(rcpt.ip_op_ind) = 'O' THEN 'N'
        WHEN upper(cc.reason_code) = 'POTENTIAL BILLING ISSUE_{O}_[15]'
         AND date_diff(current_date('US/Central'), billissue.activity_create_date, DAY) < 60 THEN 'N'
        WHEN dsc.discrepancy_origination_date IS NOT NULL
         AND upper(dsc.discrepancy_origination_date) < '2020-11-01' THEN 'Y'
        WHEN cc.discrepancy_origination_date IS NOT NULL
         AND upper(cc.discrepancy_origination_date) < '2020-11-01' THEN 'Y'
        ELSE 'N'
      END) = 'Y'
       AND upper(CASE
        WHEN upper(rcpt.ip_op_ind) = 'O' THEN 'N'
        WHEN upper(cc.reason_code) = 'POTENTIAL BILLING ISSUE_{O}_[15]'
         AND date_diff(current_date('US/Central'), billissue.activity_create_date, DAY) < 60 THEN 'N'
        WHEN eom.patient_dw_id IS NULL THEN 'N'
        WHEN mineom.drg_code = maxeom.drg_code THEN 'N'
        WHEN mineom.drg_code <> maxeom.drg_code
         AND mineom.drg_code IS NOT NULL
         AND trim(mineom.drg_code) <> ''
         AND trim(maxeom.drg_code) <> ''
         AND maxeom.drg_code IS NOT NULL THEN 'Y'
        ELSE 'N'
      END) = 'Y' THEN 'Y'
      WHEN upper(CASE
        WHEN upper(rcpt.ip_op_ind) = 'O' THEN 'N'
        WHEN upper(cc.reason_code) = 'POTENTIAL BILLING ISSUE_{O}_[15]'
         AND date_diff(current_date('US/Central'), billissue.activity_create_date, DAY) < 60 THEN 'N'
        WHEN dsc.discrepancy_origination_date IS NOT NULL
         AND upper(dsc.discrepancy_origination_date) >= '2020-11-01' THEN 'Y'
        WHEN cc.discrepancy_origination_date IS NOT NULL
         AND upper(cc.discrepancy_origination_date) >= '2020-11-01' THEN 'Y'
        ELSE 'N'
      END) = 'Y'
       AND upper(CASE
        WHEN upper(rcpt.ip_op_ind) = 'O' THEN 'N'
        WHEN upper(cc.reason_code) = 'POTENTIAL BILLING ISSUE_{O}_[15]'
         AND date_diff(current_date('US/Central'), billissue.activity_create_date, DAY) < 60 THEN 'N'
        WHEN drg.patient_dw_id IS NOT NULL THEN 'Y'
        ELSE 'N'
      END) = 'Y' THEN 'Y'
      ELSE 'N'
    END AS drg_change_flag,
    CASE
      WHEN ar.pos_trans_count + ar.neg_trans_count + coalesce(ar2.pos_trans_count, 0) + coalesce(ar2.neg_trans_count, 0) + coalesce(ar3.pos_trans_count, 0) + coalesce(ar3.neg_trans_count, 0) > 1
       AND ar.pos_trans_count + ar.neg_trans_count <> ar.pos_trans_count + ar.neg_trans_count + coalesce(ar2.pos_trans_count, 0) + coalesce(ar2.neg_trans_count, 0) + coalesce(ar3.pos_trans_count, 0) + coalesce(ar3.neg_trans_count, 0) THEN 'Y'
      ELSE 'N'
    END AS mult_pymt_flag,
    CASE
      WHEN ar.pos_trans_count + ar.neg_trans_count = 1
       AND fp.total_billed_charges > fp.expected_pmt
       AND ar.net_total_trans_sum > fp.total_billed_charges THEN 'Y'
      ELSE 'N'
    END AS single_pymt_gt_tot_chg_flag,
    fp.patient_dw_id,
    current_date('US/Central') AS currentdate,
    ROUND(fp.total_account_balance_amt, 2, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    review_category AS review_category,
    per.person_full_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS fp
    LEFT OUTER JOIN (
      SELECT
          person.person_full_name,
          person.person_dw_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.person
    ) AS per ON per.person_dw_id = fp.patient_person_dw_id
    LEFT OUTER JOIN (
      SELECT
          ar_transaction_pf.company_code,
          ar_transaction_pf.coid,
          ar_transaction_pf.patient_dw_id,
          ar_transaction_pf.iplan_id,
          max(ar_transaction_pf.ar_transaction_enter_date) AS most_recent_payment,
          sum(ar_transaction_pf.ar_transaction_amt) AS net_total_trans_sum,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt > 0 THEN ar_transaction_pf.ar_transaction_amt
            ELSE CAST(0 as NUMERIC)
          END) AS abs_total_trans_sum,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt > 0 THEN 1
            ELSE 0
          END) AS pos_trans_count,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt < 0 THEN -1
            ELSE 0
          END) AS neg_trans_count
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.ar_transaction_pf
        WHERE ar_transaction_pf.iplan_id <> 0
         AND ar_transaction_pf.transaction_type_code = '1'
        GROUP BY 1, 2, 3, 4
    ) AS ar ON fp.patient_dw_id = ar.patient_dw_id
     AND fp.iplan_id_ins1 = ar.iplan_id
    LEFT OUTER JOIN (
      SELECT
          ar_transaction_pf.company_code,
          ar_transaction_pf.coid,
          ar_transaction_pf.patient_dw_id,
          ar_transaction_pf.iplan_id,
          max(ar_transaction_pf.ar_transaction_enter_date) AS most_recent_payment,
          sum(ar_transaction_pf.ar_transaction_amt) AS net_total_trans_sum,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt > 0 THEN ar_transaction_pf.ar_transaction_amt
            ELSE CAST(0 as NUMERIC)
          END) AS abs_total_trans_sum,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt > 0 THEN 1
            ELSE 0
          END) AS pos_trans_count,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt < 0 THEN -1
            ELSE 0
          END) AS neg_trans_count
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.ar_transaction_pf
        WHERE ar_transaction_pf.iplan_id <> 0
         AND ar_transaction_pf.transaction_type_code = '1'
        GROUP BY 1, 2, 3, 4
    ) AS ar2 ON fp.patient_dw_id = ar2.patient_dw_id
     AND fp.iplan_id_ins2 = ar2.iplan_id
    LEFT OUTER JOIN (
      SELECT
          ar_transaction_pf.company_code,
          ar_transaction_pf.coid,
          ar_transaction_pf.patient_dw_id,
          ar_transaction_pf.iplan_id,
          max(ar_transaction_pf.ar_transaction_enter_date) AS most_recent_payment,
          sum(ar_transaction_pf.ar_transaction_amt) AS net_total_trans_sum,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt > 0 THEN ar_transaction_pf.ar_transaction_amt
            ELSE CAST(0 as NUMERIC)
          END) AS abs_total_trans_sum,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt > 0 THEN 1
            ELSE 0
          END) AS pos_trans_count,
          sum(CASE
            WHEN ar_transaction_pf.ar_transaction_amt < 0 THEN -1
            ELSE 0
          END) AS neg_trans_count
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.ar_transaction_pf
        WHERE ar_transaction_pf.iplan_id <> 0
         AND ar_transaction_pf.transaction_type_code = '1'
        GROUP BY 1, 2, 3, 4
    ) AS ar3 ON fp.patient_dw_id = ar3.patient_dw_id
     AND fp.iplan_id_ins3 = ar3.iplan_id
    LEFT OUTER JOIN (
      SELECT
          rd.coid,
          rd.patient_dw_id,
          rd.payor_dw_id,
          rd.iplan_insurance_order_num,
          rd.discrepancy_reason_code_1,
          rd.discrepancy_reason_code_2,
          rd.discrepancy_reason_code_3,
          rd.discrepancy_origination_date,
          sum(rd.var_gross_reimbursement_amt) AS var_gross_reimbursement_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.reimbursement_discrepancy AS rd
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS fp_0 ON rd.patient_dw_id = fp_0.patient_dw_id
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS org_0 ON rd.coid = org_0.coid
        WHERE upper(CASE
          WHEN upper(org_0.customer_code) = 'OSG'
           AND org_0.unit_num IN(
            '03388', '03395', '25160'
          )
           AND rd.var_gross_reimbursement_amt <= -250 THEN 'In Threshold'
          WHEN upper(org_0.customer_code) = 'OSG'
           AND org_0.unit_num NOT IN(
            '03388', '03395', '25160'
          )
           AND rd.var_gross_reimbursement_amt <= -500 THEN 'In Threshold'
          WHEN fp_0.financial_class_code IN(
            4, 5, 7, 8, 11, 13, 14
          )
           AND rd.var_gross_reimbursement_amt <= -2000 THEN 'In Threshold'
          WHEN fp_0.financial_class_code IN(
            9, 12
          )
           AND rd.var_gross_reimbursement_amt <= -500 THEN 'In Threshold'
          ELSE CAST(NULL as STRING)
        END) = 'IN THRESHOLD'
         AND upper(rd.source_system_code) = 'P'
         AND upper(rd.company_code) = 'H'
         AND rd.iplan_insurance_order_num = 1
         AND fp_0.financial_class_code IN(
          4, 5, 7, 8, 9, 11, 12, 13, 14
        )
         AND (rd.discrepancy_reason_code_1 NOT IN(
          '301', '302', '701'
        )
         OR rd.discrepancy_reason_code_1 IS NULL)
         AND (rd.discrepancy_reason_code_3 NOT IN(
          '801', '821', '829', '847'
        )
         OR rd.discrepancy_reason_code_3 IS NULL)
         AND (rd.discrepancy_reason_code_4 NOT IN(
          '801', '821', '829', '847'
        )
         OR rd.discrepancy_reason_code_4 IS NULL)
         AND (rd.discrepancy_reason_code_3 NOT IN(
          '305', '306'
        )
         OR rd.discrepancy_reason_code_3 IS NULL)
         AND (rd.discrepancy_reason_code_2 NOT IN(
          '301', '302', '701'
        )
         OR rd.discrepancy_reason_code_2 IS NULL)
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
      UNION DISTINCT
      SELECT
          rd.coid,
          rd.patient_dw_id,
          rd.payor_dw_id,
          rd.iplan_insurance_order_num,
          rd.discrepancy_reason_code_1,
          rd.discrepancy_reason_code_2,
          rd.discrepancy_reason_code_3,
          rd.discrepancy_origination_date,
          sum(rd.var_gross_reimbursement_amt) AS var_gross_reimbursement_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.resolved_discrepancy_pf AS rd
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS fp_0 ON rd.patient_dw_id = fp_0.patient_dw_id
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS org_0 ON rd.coid = org_0.coid
        WHERE upper(CASE
          WHEN upper(org_0.customer_code) = 'OSG'
           AND (upper(org_0.unit_num) = '03388'
           OR upper(org_0.unit_num) = '03395'
           OR upper(org_0.unit_num) = '25160')
           AND rd.var_gross_reimbursement_amt <= -250 THEN 'In Threshold'
          WHEN upper(org_0.customer_code) = 'OSG'
           AND org_0.unit_num NOT IN(
            '03388', '03395', '25160'
          )
           AND rd.var_gross_reimbursement_amt <= -500 THEN 'In Threshold'
          WHEN fp_0.financial_class_code IN(
            4, 5, 7, 8, 11, 13, 14
          )
           AND rd.var_gross_reimbursement_amt <= -2000 THEN 'In Threshold'
          WHEN fp_0.financial_class_code IN(
            9, 12
          )
           AND rd.var_gross_reimbursement_amt <= -500 THEN 'In Threshold'
          ELSE CAST(NULL as STRING)
        END) = 'IN THRESHOLD'
         AND upper(rd.source_system_code) = 'P'
         AND upper(rd.company_code) = 'H'
         AND rd.iplan_insurance_order_num = 1
         AND fp_0.financial_class_code IN(
          4, 5, 7, 8, 9, 11, 12, 13, 14
        )
         AND (rd.discrepancy_reason_code_1 NOT IN(
          '301', '302', '701'
        )
         OR rd.discrepancy_reason_code_1 IS NULL)
         AND (rd.discrepancy_reason_code_3 NOT IN(
          '801', '821', '829', '847'
        )
         OR rd.discrepancy_reason_code_3 IS NULL)
         AND (rd.discrepancy_reason_code_4 NOT IN(
          '801', '821', '829', '847'
        )
         OR rd.discrepancy_reason_code_4 IS NULL)
         AND (rd.discrepancy_reason_code_3 NOT IN(
          '305', '306'
        )
         OR rd.discrepancy_reason_code_3 IS NULL)
         AND (rd.discrepancy_reason_code_2 NOT IN(
          '301', '302', '701'
        )
         OR rd.discrepancy_reason_code_2 IS NULL)
         AND rd.discrepancy_resolved_date >= date_sub(current_date('US/Central'), interval 2 DAY)
         AND (rd.patient_dw_id, rd.payor_dw_id, rd.iplan_insurance_order_num) NOT IN(
          SELECT AS STRUCT
              reimbursement_discrepancy.patient_dw_id,
              reimbursement_discrepancy.payor_dw_id,
              reimbursement_discrepancy.iplan_insurance_order_num
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs_views.reimbursement_discrepancy
            GROUP BY 1, 2, 3
        )
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        QUALIFY rank() OVER (PARTITION BY rd.patient_dw_id, rd.payor_dw_id ORDER BY rd.discrepancy_origination_date DESC) = 1
    ) AS dsc ON dsc.patient_dw_id = fp.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          dsc_0.coid,
          dsc_0.patient_dw_id,
          dsc_0.payor_dw_id,
          dsc_0.pat_acct_num,
          dsc_0.iplan_id,
          dsc_0.iplan_order_num,
          dsc_0.reason_code,
          dsc_0.project_name,
          dsc_0.status_desc,
          dsc_0.first_actv_create_date AS discrepancy_origination_date,
          sum(dsc_0.payor_due_amt) AS var_gross_reimbursement_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwra_views.cc_discrepancy AS dsc_0
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS org_0 ON dsc_0.coid = org_0.coid
        WHERE upper(CASE
          WHEN upper(org_0.customer_code) = 'OSG'
           AND (upper(org_0.unit_num) = '03388'
           OR upper(org_0.unit_num) = '03395'
           OR upper(org_0.unit_num) = '25160')
           AND dsc_0.payor_due_amt <= -250 THEN 'In Threshold'
          WHEN upper(org_0.customer_code) = 'OSG'
           AND upper(org_0.unit_num) <> '03388'
           AND upper(org_0.unit_num) <> '03395'
           AND upper(org_0.unit_num) <> '25160'
           AND dsc_0.payor_due_amt <= -500 THEN 'In Threshold'
          WHEN dsc_0.pa_financial_class_num IN(
            4, 5, 7, 8, 11, 13, 14
          )
           AND dsc_0.payor_due_amt <= -2000 THEN 'In Threshold'
          WHEN dsc_0.pa_financial_class_num IN(
            9, 12
          )
           AND dsc_0.payor_due_amt <= -500 THEN 'In Threshold'
        END) = 'IN THRESHOLD'
         AND (upper(dsc_0.discrepancy_group_name) = 'OVERPAYMENT'
         OR upper(dsc_0.discrepancy_group_name) = 'UNREVIEWED')
         AND upper(org_0.company_code) = 'H'
         AND dsc_0.extract_date_time = (
          SELECT
              max(cc_discrepancy.extract_date_time)
            FROM
              `hca-hin-dev-cur-parallon`.edwra_views.cc_discrepancy
        )
         AND dsc_0.iplan_order_num = 1
         AND dsc_0.pa_financial_class_num IN(
          4, 5, 7, 8, 9, 11, 12, 13, 14
        )
         AND (upper(dsc_0.reason_code) NOT LIKE '%{V}%'
         OR dsc_0.reason_code IS NULL
         OR dsc_0.reason_code NOT IN(
          'Cash Posting Error_{P}_[15]', 'Scripted Potential Overpayment_{R}_[15]'
        ))
         AND (dsc_0.project_name NOT IN(
          'PSU_Ovr_Pending Direct Connect Response', 'ATLP_Ovr_Solicited Refund Review', 'ORAP_Ovr_Solicited Refund Review', 'RICP_Ovr_Solicited Refund Review', 'TAMP_Ovr_Solicited Refund Review', 'DALP_Ovr_Solicited Refund Review', 'HOUP_Ovr_Solicited Refund Review', 'NASP_Ovr_Solicited Refund Review', 'SANP_Ovr_Solicited Refund Review'
        )
         OR dsc_0.project_name IS NULL)
         AND (upper(dsc_0.project_name) NOT LIKE '%CORP_DISPUTE%'
         OR dsc_0.project_name IS NULL)
         AND (upper(dsc_0.project_name) NOT LIKE '%CORP_DRT%'
         OR dsc_0.project_name IS NULL)
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
      UNION DISTINCT
      SELECT
          rd.coid,
          rd.patient_dw_id,
          rd.payor_dw_id,
          rd.pat_acct_num,
          rd.iplan_id,
          rd.iplan_insurance_order_num AS iplan_order_num,
          ccuicurr.reason_code,
          ccuicurr.project_name,
          ccuicurr.status_desc,
          rd.discrepancy_origination_date,
          sum(rd.var_gross_reimbursement_amt) AS var_gross_reimbursement_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.resolved_discrepancy_pf AS rd
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS fp_0 ON rd.patient_dw_id = fp_0.patient_dw_id
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS org_0 ON rd.coid = org_0.coid
          LEFT OUTER JOIN (
            SELECT
                cc_discrepancy.coid,
                cc_discrepancy.pat_acct_num,
                cc_discrepancy.project_name,
                cc_discrepancy.reason_code,
                cc_discrepancy.status_desc
              FROM
                `hca-hin-dev-cur-parallon`.edwra_views.cc_discrepancy
              WHERE cc_discrepancy.extract_date_time = (
                SELECT
                    max(cc_discrepancy_0.extract_date_time)
                  FROM
                    `hca-hin-dev-cur-parallon`.edwra_views.cc_discrepancy AS cc_discrepancy_0
              )
          ) AS ccuicurr ON rd.coid = ccuicurr.coid
           AND rd.pat_acct_num = ccuicurr.pat_acct_num
        WHERE upper(CASE
          WHEN upper(org_0.customer_code) = 'OSG'
           AND (upper(org_0.unit_num) = '03388'
           OR upper(org_0.unit_num) = '03395'
           OR upper(org_0.unit_num) = '25160')
           AND rd.var_gross_reimbursement_amt <= -250 THEN 'In Threshold'
          WHEN upper(org_0.customer_code) = 'OSG'
           AND upper(org_0.unit_num) <> '03388'
           AND upper(org_0.unit_num) <> '03395'
           AND upper(org_0.unit_num) <> '25160'
           AND rd.var_gross_reimbursement_amt <= -500 THEN 'In Threshold'
          WHEN fp_0.financial_class_code IN(
            4, 5, 7, 8, 11, 13, 14
          )
           AND rd.var_gross_reimbursement_amt <= -2000 THEN 'In Threshold'
          WHEN fp_0.financial_class_code IN(
            9, 12
          )
           AND rd.var_gross_reimbursement_amt <= -500 THEN 'In Threshold'
        END) = 'IN THRESHOLD'
         AND upper(rd.source_system_code) = 'N'
         AND upper(rd.company_code) = 'H'
         AND rd.iplan_insurance_order_num = 1
         AND fp_0.financial_class_code IN(
          4, 5, 7, 8, 9, 11, 12, 13, 14
        )
         AND (upper(ccuicurr.reason_code) NOT LIKE '%{V}%'
         OR ccuicurr.reason_code IS NULL
         OR ccuicurr.reason_code NOT IN(
          'Cash Posting Error_{P}_[15]', 'Scripted Potential Overpayment_{R}_[15]'
        ))
         AND (ccuicurr.project_name NOT IN(
          'PSU_Ovr_Pending Direct Connect Response', 'ATLP_Ovr_Solicited Refund Review', 'ORAP_Ovr_Solicited Refund Review', 'RICP_Ovr_Solicited Refund Review', 'TAMP_Ovr_Solicited Refund Review', 'DALP_Ovr_Solicited Refund Review', 'HOUP_Ovr_Solicited Refund Review', 'NASP_Ovr_Solicited Refund Review', 'SANP_Ovr_Solicited Refund Review'
        )
         OR ccuicurr.project_name IS NULL)
         AND (upper(ccuicurr.project_name) NOT LIKE '%CORP_DISPUTE%'
         OR ccuicurr.project_name IS NULL)
         AND (upper(ccuicurr.project_name) NOT LIKE '%CORP_DRT%'
         OR ccuicurr.project_name IS NULL)
         AND rd.discrepancy_resolved_date >= date_sub(current_date('US/Central'), interval 2 DAY)
         AND (rd.coid, rd.pat_acct_num, rd.iplan_id) NOT IN(
          SELECT AS STRUCT
              cc_discrepancy.coid,
              cc_discrepancy.pat_acct_num,
              cc_discrepancy.iplan_id
            FROM
              `hca-hin-dev-cur-parallon`.edwra_views.cc_discrepancy
            WHERE cc_discrepancy.extract_date_time = (
              SELECT
                  max(cc_discrepancy_0.extract_date_time)
                FROM
                  `hca-hin-dev-cur-parallon`.edwra_views.cc_discrepancy AS cc_discrepancy_0
            )
            GROUP BY 1, 2, 3
        )
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ) AS cc ON cc.pat_acct_num = fp.pat_acct_num
     AND cc.coid = fp.coid
     AND cc.iplan_id = fp.iplan_id_ins1
    LEFT OUTER JOIN (
      SELECT
          ar2_0.company_code,
          ar2_0.coid,
          ar2_0.patient_dw_id,
          ar2_0.iplan_id,
          ar2_0.pat_acct_num,
          ar2_0.payor_dw_id,
          ar2_0.ar_transaction_amt,
          ar2_0.transaction_code,
          ar2_0.transaction_type_code,
          ar3_0.total_ar_trans_sum,
          fp_0.expected_pmt,
          count(*) AS dup_trans_ct
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.ar_transaction_pf AS ar2_0
          INNER JOIN (
            SELECT
                fact_patient.coid,
                fact_patient.patient_dw_id,
                fact_patient.total_account_balance_amt,
                fact_patient.expected_pmt
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient
          ) AS fp_0 ON ar2_0.coid = fp_0.coid
           AND ar2_0.patient_dw_id = fp_0.patient_dw_id
          INNER JOIN (
            SELECT
                ar3_1.company_code,
                ar3_1.coid,
                ar3_1.patient_dw_id,
                ar3_1.iplan_id,
                ar3_1.pat_acct_num,
                ar3_1.payor_dw_id,
                sum(ar3_1.ar_transaction_amt) AS total_ar_trans_sum,
                ar3_1.transaction_type_code,
                count(*) AS total_ar_trans_count
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_views.ar_transaction_pf AS ar3_1
                INNER JOIN (
                  SELECT
                      fact_patient.coid,
                      fact_patient.patient_dw_id,
                      fact_patient.total_account_balance_amt,
                      fact_patient.expected_pmt
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient
                ) AS fp_1 ON ar3_1.coid = fp_1.coid
                 AND ar3_1.patient_dw_id = fp_1.patient_dw_id
              WHERE ar3_1.company_code = 'H'
               AND ar3_1.transaction_type_code = '1'
               AND ar3_1.iplan_id <> 0
              GROUP BY 1, 2, 3, 4, 5, 6, 8
              HAVING count(*) > 1
          ) AS ar3_0 ON ar2_0.coid = ar3_0.coid
           AND ar2_0.patient_dw_id = ar3_0.patient_dw_id
           AND ar2_0.iplan_id = ar3_0.iplan_id
        WHERE ar2_0.company_code = 'H'
         AND ar2_0.ar_transaction_amt <> 0
         AND ar2_0.transaction_type_code = '1'
         AND ar2_0.iplan_id <> 0
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        HAVING dup_trans_ct > 1
         AND ar3_0.total_ar_trans_sum = dup_trans_ct * ar2_0.ar_transaction_amt
    ) AS dt ON dt.coid = fp.coid
     AND dt.patient_dw_id = fp.patient_dw_id
    INNER JOIN (
      SELECT
          dim_rcm_organization.coid,
          dim_rcm_organization.coid_name,
          dim_rcm_organization.division_name,
          dim_rcm_organization.group_name,
          dim_rcm_organization.market_name,
          dim_rcm_organization.ssc_coid,
          dim_rcm_organization.ssc_name,
          dim_rcm_organization.unit_num
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization
    ) AS org ON org.coid = fp.coid
    INNER JOIN (
      SELECT
          facility_dimension.coid,
          facility_dimension.summary_7_member_ind,
          facility_dimension.pas_coid,
          facility_dimension.pas_current_name
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.facility_dimension
        WHERE upper(facility_dimension.summary_7_member_ind) = 'Y'
         OR upper(facility_dimension.pas_coid) = '25464'
    ) AS fac ON fac.coid = fp.coid
    LEFT OUTER JOIN (
      SELECT
          pass_eom.patient_dw_id,
          pass_eom.drg_code,
          pass_eom.rptg_period
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom
        QUALIFY rank() OVER (PARTITION BY pass_eom.patient_dw_id ORDER BY pass_eom.rptg_period DESC) = 1
    ) AS maxeom ON maxeom.patient_dw_id = fp.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          pass_eom.patient_dw_id,
          pass_eom.drg_code,
          pass_eom.rptg_period
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom
        WHERE upper(pass_eom.account_status_code) <> 'UB'
        QUALIFY rank() OVER (PARTITION BY pass_eom.patient_dw_id ORDER BY pass_eom.rptg_period) = 1
    ) AS mineom ON mineom.patient_dw_id = fp.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          a.patient_dw_id,
          a.pat_acct_num,
          a.rptg_period
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom AS a
          INNER JOIN (
            SELECT
                max(pass_eom.rptg_period) AS maxperiod
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom
          ) AS b ON b.maxperiod = a.rptg_period
    ) AS eom ON eom.patient_dw_id = fp.patient_dw_id
    LEFT OUTER JOIN (
      SELECT DISTINCT
          fp_0.patient_dw_id,
          fp_0.coid,
          fp_0.pat_acct_num,
          org_0.company_code,
          org_0.customer_short_name,
          org_0.ssc_name,
          fp_0.financial_class_code,
          claim.first_claim_drg,
          fp_0.drg_code_payor AS current_drg,
          curreom.drg_code AS curreom_drg,
          curreom.drg_code_hcfa AS curreom_drg_hcfa,
          curreom.rptg_period AS curreom_reporting_period,
          prioreom.drg_code_hcfa AS prioreom_drg_hcfa,
          prioreom.rptg_period AS prioreom_reporting_period,
          drg_change_ind AS drg_change_ind
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS fp_0
          LEFT OUTER JOIN (
            SELECT
                rh.coid,
                rh.unit_num,
                rh.pat_acct_num,
                rh.iplan_id,
                left(rh.drg_code, 3) AS first_claim_drg,
                rh.bill_date AS first_claim_bill_date
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.rh_837_claim AS rh
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_organization AS org_1 ON CAST(bqutil.fn.cw_td_normalize_number(org_1.coid) as INT64) = CAST(/* expression of unknown or erroneous type */ rh.coid as INT64)
              WHERE rh.drg_code IS NOT NULL
               AND CAST(/* expression of unknown or erroneous type */ rh.coid as INT64) = CAST(bqutil.fn.cw_td_normalize_number(org_1.coid) as INT64)
              QUALIFY row_number() OVER (PARTITION BY rh.pat_acct_num, rh.unit_num ORDER BY first_claim_bill_date) = 1
          ) AS claim ON claim.pat_acct_num = fp_0.pat_acct_num
           AND claim.coid = fp_0.coid
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_organization AS org_0 ON org_0.coid = fp_0.coid
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom AS curreom ON curreom.patient_dw_id = fp_0.patient_dw_id
           AND curreom.rptg_period = cw_const
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.pass_eom AS prioreom ON prioreom.patient_dw_id = fp_0.patient_dw_id
           AND prioreom.rptg_period + 1 = cw_const_0
          CROSS JOIN UNNEST(ARRAY[
            CASE
              WHEN prioreom.drg_code_hcfa IS NOT NULL
               AND trim(prioreom.drg_code_hcfa) = trim(fp_0.drg_code_payor) THEN 'Y'
              WHEN prioreom.drg_code_hcfa IS NOT NULL
               AND trim(prioreom.drg_code_hcfa) <> trim(fp_0.drg_code_payor) THEN 'N'
              ELSE 'Y'
            END
          ]) AS drg_change_ind
        WHERE fp_0.patient_type_code_pos1 = 'I'
         AND claim.first_claim_drg <> fp_0.drg_code_payor
         AND trim(fp_0.drg_code_payor) <> ''
         AND trim(claim.first_claim_drg) <> ''
         AND upper(drg_change_ind) = 'Y'
    ) AS drg ON drg.patient_dw_id = fp.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          a.patient_dw_id,
          sum(a.charge_amt) AS late_charge_amt,
          b.pat_acct_num,
          max(a.charge_posted_date) AS max_charge_posted_date
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.patient_charge_pf AS a
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS b ON a.patient_dw_id = b.patient_dw_id
           AND b.final_bill_date IS NOT NULL
           AND b.final_bill_date <> DATE '0001-01-01'
           AND a.charge_posted_date > b.final_bill_date
           AND a.charge_posted_date > DATE '2014-01-01'
        WHERE a.company_code = 'H'
         AND a.patient_charge_type_code IN(
          6, 8
        )
        GROUP BY 1, 3
        HAVING sum(a.charge_amt) <> 0
    ) AS lc ON lc.patient_dw_id = fp.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          rpdov_0.*
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.rcom_payor_dimension_eom AS rpdov_0
        WHERE (rpdov_0.payor_dw_id, rpdov_0.eff_from_date) IN(
          SELECT AS STRUCT
              rcom_payor_dimension_eom.payor_dw_id,
              max(rcom_payor_dimension_eom.eff_from_date) AS eff_from_date
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs_views.rcom_payor_dimension_eom
            GROUP BY 1
        )
    ) AS rpdov ON rpdov.payor_dw_id = fp.payor_dw_id_ins1
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_iplan AS fi ON fp.payor_dw_id_ins1 = fi.payor_dw_id
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.secref_facility AS b ON fp.company_code = b.company_code
     AND fp.coid = b.co_id
     AND b.user_id = session_user()
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwra_views.cc_account AS ccacc ON fp.patient_dw_id = ccacc.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwra_views.ref_cc_patient_type AS rcpt ON ccacc.cc_patient_type_id = rcpt.patient_type_id
    LEFT OUTER JOIN (
      SELECT
          cc_account_activity.patient_dw_id,
          CAST(/* expression of unknown or erroneous type */ cc_account_activity.activity_create_date_time as DATE) AS activity_create_date
        FROM
          `hca-hin-dev-cur-parallon`.edwra_views.cc_account_activity
        WHERE cc_account_activity.activity_type_id = 2
         AND upper(cc_account_activity.activity_subject_text) LIKE 'TO%POTENTIAL BILLING ISSUE%FROM%'
         AND cc_account_activity.iplan_insurance_order_num = 1
        QUALIFY row_number() OVER (PARTITION BY cc_account_activity.patient_dw_id ORDER BY cc_account_activity.activity_create_date_time DESC) = 1
    ) AS billissue ON fp.patient_dw_id = billissue.patient_dw_id
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN dt.dup_trans_ct > 1
         AND upper(CASE
          WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y' THEN 'Duplicate Payment Potential Overpayment'
        WHEN upper(CASE
          WHEN ar.pos_trans_count + ar.neg_trans_count + coalesce(ar2.pos_trans_count, 0) + coalesce(ar2.neg_trans_count, 0) + coalesce(ar3.pos_trans_count, 0) + coalesce(ar3.neg_trans_count, 0) > 1
           AND ar.pos_trans_count + ar.neg_trans_count <> ar.pos_trans_count + ar.neg_trans_count + coalesce(ar2.pos_trans_count, 0) + coalesce(ar2.neg_trans_count, 0) + coalesce(ar3.pos_trans_count, 0) + coalesce(ar3.neg_trans_count, 0) THEN 'Y'
          ELSE 'N'
        END) = 'Y'
         AND upper(CASE
          WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y' THEN 'Multi Payment Potential Overpayment'
        WHEN upper(CASE
          WHEN ar.pos_trans_count + ar.neg_trans_count + coalesce(ar2.pos_trans_count, 0) + coalesce(ar2.neg_trans_count, 0) + coalesce(ar3.pos_trans_count, 0) + coalesce(ar3.neg_trans_count, 0) > 1
           AND fp.total_billed_charges > fp.expected_pmt
           AND ar.net_total_trans_sum + ar2.net_total_trans_sum + ar3.net_total_trans_sum > fp.total_billed_charges THEN 'Y'
          ELSE 'N'
        END) = 'Y'
         AND upper(CASE
          WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y' THEN 'Multi Payment GT Total Charge Potential Overpayment'
        WHEN upper(CASE
          WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y'
         AND upper(CASE
          WHEN dsc.discrepancy_origination_date IS NOT NULL
           AND upper(dsc.discrepancy_origination_date) < '2020-11-01' THEN 'Y'
          WHEN cc.discrepancy_origination_date IS NOT NULL
           AND upper(cc.discrepancy_origination_date) < '2020-11-01' THEN 'Y'
          ELSE 'N'
        END) = 'Y'
         AND upper(CASE
          WHEN upper(rcpt.ip_op_ind) = 'O' THEN 'N'
          WHEN upper(cc.reason_code) = 'POTENTIAL BILLING ISSUE_{O}_[15]'
           AND date_diff(current_date('US/Central'), billissue.activity_create_date, DAY) < 60 THEN 'N'
          WHEN eom.patient_dw_id IS NULL THEN 'N'
          WHEN mineom.drg_code = maxeom.drg_code THEN 'N'
          WHEN mineom.drg_code <> maxeom.drg_code
           AND mineom.drg_code IS NOT NULL
           AND trim(mineom.drg_code) <> ''
           AND trim(maxeom.drg_code) <> ''
           AND maxeom.drg_code IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y' THEN 'DRG Change Potential Overpayment'
        WHEN upper(CASE
          WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y'
         AND upper(CASE
          WHEN dsc.discrepancy_origination_date IS NOT NULL
           AND upper(dsc.discrepancy_origination_date) >= '2020-11-01' THEN 'Y'
          WHEN cc.discrepancy_origination_date IS NOT NULL
           AND upper(cc.discrepancy_origination_date) >= '2020-11-01' THEN 'Y'
          ELSE 'N'
        END) = 'Y'
         AND upper(rcpt.ip_op_ind) = 'I'
         AND upper(CASE
          WHEN upper(cc.reason_code) = 'POTENTIAL BILLING ISSUE_{O}_[15]'
           AND date_diff(current_date('US/Central'), billissue.activity_create_date, DAY) < 60 THEN 'N'
          ELSE 'Y'
        END) = 'Y'
         AND upper(CASE
          WHEN drg.patient_dw_id IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y' THEN 'DRG Change Potential Overpayment'
        WHEN upper(CASE
          WHEN upper(CASE
            WHEN ar.pos_trans_count + ar.neg_trans_count = 1 THEN 'Y'
            ELSE 'N'
          END) = 'Y'
           AND fp.total_billed_charges > fp.expected_pmt
           AND ar.net_total_trans_sum > fp.total_billed_charges THEN 'Y'
          ELSE 'N'
        END) = 'Y'
         AND upper(CASE
          WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y' THEN 'Single Payment Greater than Total Charge Potential Overpayment'
        WHEN lc.late_charge_amt IS NOT NULL
         AND upper(CASE
          WHEN dsc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          WHEN cc.var_gross_reimbursement_amt IS NOT NULL THEN 'Y'
          ELSE 'N'
        END) = 'Y' THEN 'Late Charge Potential Overpayment'
        ELSE 'Other'
      END
    ]) AS review_category
  WHERE fp.company_code = 'H'
   AND upper(review_category) <> 'OTHER'
   AND fac.pas_coid IN(
    '08591', '08648', '08942', '08945', '08947', '08948', '08949', '08950', '25464'
  )
   AND substr(coalesce(trim(upper(trim(substr(fac.pas_current_name, 1, strpos(fac.pas_current_name, '-') - 1)))), 'UNKNOWN SSC'), 1, 3) <> 'UNK'
   AND upper(CASE
    WHEN upper(fac.pas_coid) = '08942'
     AND org.coid IN(
      '34222', '34223', '34224', '34242', '34296', '34241', '34206', '36243', '36244', '34293', '09391', '25070', '26620'
    )
     AND fp.iplan_id_ins1 IN(
      27765, 27865, 27965, 28165, 28265, 28465, 31205, 31211, 31212
    ) THEN 'NetSDRT'
    ELSE 'Include'
  END) = 'INCLUDE'
;
