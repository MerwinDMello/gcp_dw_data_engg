-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_cr_refund_nkey.memory
-- Translated from: Teradata
-- Translated to: BigQuery

DECLARE cw_const STRING DEFAULT bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', CASE
  WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
  ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
END), 1, 6));
CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_cr_refund_nkey AS SELECT
    a.patient_sid,
    a.date_sid,
    date_sub(date_add(CAST(trim(concat(substr(substr(CAST(a.date_sid as STRING), 1, 6), 1, 4), '-', substr(substr(CAST(a.date_sid as STRING), 1, 6), 5, 2), '-01')) as DATE), interval 1 MONTH), interval 1 DAY) AS pe_date,
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
      WHEN a.company_code = 'H'
       AND same_store.same_store_ind = 'Y' THEN 1
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
    a.refund_amount,
    a.refund_count,
    a.total_account_balance_amt,
    a.total_charge_amt,
    a.total_cash_pay_amt,
    a.total_allow_amt,
    a.total_policy_adj_amt,
    a.total_write_off_amt,
    a.discharge_date,
    a.credit_balance_date,
    a.resolved_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_credit_refund AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON same_store.coid = a.coid
     AND same_store.gl_close_ind = 'N'
     AND same_store.month_id = CAST(cw_const as FLOAT64)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_financial_class_dim AS b ON a.payor_financial_class_sid = b.payor_financial_class_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS c ON a.payor_sid = c.payor_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_cr_refund_age_dim AS k ON a.cr_refund_age_month_sid = k.cr_refund_age_month_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_patient_type_dim AS f ON a.patient_type_sid = f.patient_type_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_account_status_dim AS g ON a.account_status_sid = g.account_status_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS h ON a.unit_num_sid = h.org_sid
     AND upper(h.org_hier_name) LIKE '%PARS ORG HIER%'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_refund_type_dim AS e ON a.refund_type_sid = e.refund_type_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_metric AS l ON l.metric_sid = CASE
      WHEN a.company_code = 'H'
       AND same_store.same_store_ind = 'Y' THEN 1
      ELSE 0
    END
     AND upper(l.metric_hier_name) = 'AR_PARS_SAME_STORE'
;
