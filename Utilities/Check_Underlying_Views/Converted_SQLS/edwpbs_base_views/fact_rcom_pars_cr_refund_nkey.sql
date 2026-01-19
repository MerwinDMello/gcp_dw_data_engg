-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_cr_refund_nkey.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_cr_refund_nkey AS SELECT
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.date_sid,
    date_sub(date_add(CAST(concat(substr(CAST(a.date_sid as STRING), 1, 4), '-', substr(CAST(a.date_sid as STRING), 5, 2), '-01') as DATE), interval 1 MONTH), interval 1 DAY) AS pe_date,
    a.unit_num_sid,
    h.org_name_child AS unit_num_member,
    h.org_alias_name AS unit_num_alias,
    a.account_status_sid,
    g.account_status_member,
    g.account_status_alias,
    a.patient_type_sid,
    f.patient_type_member,
    f.patient_type_alias,
    a.payor_sid,
    c.payor_member,
    c.payor_alias,
    NULL AS payor_sequence_sid,
    NULL AS payor_sequence_member,
    NULL AS payor_sequence_alias,
    CASE
      WHEN upper(a.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    l.metric_name_child AS same_store_name_child,
    l.metric_alias_name AS same_store_alias_name,
    a.payor_financial_class_sid,
    b.payor_financial_class_member,
    b.payor_financial_class_alias,
    a.refund_type_sid,
    e.refund_type_member,
    e.refund_type_alias,
    a.cr_refund_age_month_sid,
    k.cr_refund_age_member,
    k.cr_refund_age_alias,
    a.company_code,
    a.coid,
    a.credit_refund_id,
    a.age_of_refund,
    ROUND(a.refund_amount, 3, 'ROUND_HALF_EVEN') AS refund_amount,
    a.refund_count,
    ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
    ROUND(a.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
    ROUND(a.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
    ROUND(a.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
    ROUND(a.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
    a.discharge_date,
    a.credit_balance_date,
    a.resolved_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_credit_refund AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON upper(same_store.coid) = upper(a.coid)
     AND upper(same_store.gl_close_ind) = 'N'
     AND same_store.month_id = CASE
       format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END)
      WHEN '' THEN 0.0
      ELSE CAST(format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END) as FLOAT64)
    END
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_financial_class_dim AS b ON a.payor_financial_class_sid = b.payor_financial_class_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS c ON a.payor_sid = c.payor_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_cr_refund_age_dim AS k ON a.cr_refund_age_month_sid = k.cr_refund_age_month_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_patient_type_dim AS f ON a.patient_type_sid = f.patient_type_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_account_status_dim AS g ON a.account_status_sid = g.account_status_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS h ON a.unit_num_sid = h.org_sid
     AND upper(h.org_hier_name) LIKE '%PARS ORG HIER%'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_refund_type_dim AS e ON a.refund_type_sid = e.refund_type_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_metric AS l ON l.metric_sid = CASE
      WHEN upper(a.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END
     AND upper(l.metric_hier_name) = 'AR_PARS_SAME_STORE'
;
