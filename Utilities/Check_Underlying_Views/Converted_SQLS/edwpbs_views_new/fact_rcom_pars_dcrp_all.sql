-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_dcrp_all.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_dcrp_all AS SELECT
    a.date_sid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    a.year_created_sid,
    aj.year_created_name_child AS year_created_member,
    aj.year_created_alias_name AS year_created_alias,
    a.cost_year_sid,
    a.cost_year_end_date,
    ai.medicare_cost_year_name_child AS cost_year_member,
    ai.medicare_cost_year_alias_name AS cost_year_alias,
    a.patient_sid,
    a.payor_financial_class_sid,
    c.payor_financial_class_member,
    c.payor_financial_class_alias,
    a.payor_sid,
    d.payor_member,
    d.payor_alias,
    d.payor_gen02 AS payor_gen_02,
    d.payor_gen03 AS payor_gen_03,
    d.payor_gen05 AS payor_gen_05,
    a.payor_sequence_sid,
    e.payor_sequence_member,
    e.payor_sequence_alias,
    e.payor_sequence_gen02,
    a.patient_type_sid,
    f.patient_type_member,
    f.patient_type_alias,
    f.patient_type_gen02,
    a.unit_num_sid,
    g.org_name_child AS unit_num_member,
    g.org_alias_name AS unit_num_alias,
    a.discrepancy_age_month_sid,
    h.age_name_child AS discrepancy_age_member,
    h.age_alias_name AS discrepancy_age_alias,
    a.discharge_age_month_sid,
    disch.age_name_child AS discharge_age_member,
    disch.age_alias_name AS discharge_age_alias,
    a.scenario_sid,
    i.scenario_member,
    i.scenario_alias,
    a.reason_code_1_sid,
    k.reason_code_member AS reason_code_1_member,
    k.reason_code_alias AS reason_code_1_alias,
    k.reason_code_gen03 AS reason_code_1_gen03,
    k.reason_code_gen03_alias AS reason_code_1_gen03_alias,
    k.reason_code_gen04 AS reason_code_1_gen04,
    k.reason_code_gen04_alias AS reason_code_1_gen04_alias,
    a.reason_code_2_sid,
    l.reason_code_member AS reason_code_2_member,
    l.reason_code_alias AS reason_code_2_alias,
    l.reason_code_gen03 AS reason_code_2_gen03,
    l.reason_code_gen03_alias AS reason_code_2_gen03_alias,
    l.reason_code_gen04 AS reason_code_2_gen04,
    l.reason_code_gen04_alias AS reason_code_2_gen04_alias,
    a.reason_code_3_sid,
    m.reason_code_member AS reason_code_3_member,
    m.reason_code_alias AS reason_code_3_alias,
    m.reason_code_gen03 AS reason_code_3_gen03,
    m.reason_code_gen03_alias AS reason_code_3_gen03_alias,
    m.reason_code_gen04 AS reason_code_3_gen04,
    m.reason_code_gen04_alias AS reason_code_3_gen04_alias,
    a.reason_code_4_sid,
    n.reason_code_member AS reason_code_4_member,
    n.reason_code_alias AS reason_code_4_alias,
    n.reason_code_gen03 AS reason_code_4_gen03,
    n.reason_code_gen03_alias AS reason_code_4_gen03_alias,
    n.reason_code_gen04 AS reason_code_4_gen04,
    n.reason_code_gen04_alias AS reason_code_4_gen04_alias,
    a.source_sid,
    z.source_child AS source_member,
    z.source_alias_name,
    a.cc_account_payer_status_id,
    cc_a.cc_status_short_desc,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    a.account_balance_amt,
    a.primary_reason_code_change_cnt,
    a.discrepancy_days,
    a.discrepancy_resolved_date,
    a.discrepany_type_code,
    a.discrepancy_type_sid,
    ac.discrepancy_type_name_child,
    ac.discrepancy_type_alias_name,
    a.same_store_sid,
    a.dollar_strtf_sid,
    j.dollar_strf_member,
    j.dollar_strf_alias,
    a.var_beg_amt,
    a.var_new_amt,
    a.var_resolved_amt,
    a.var_othr_cor_amt,
    a.var_end_amt,
    a.exp_crnt_amt,
    a.exp_rate_amt,
    a.var_beg_cnt,
    a.var_new_cnt,
    a.var_resolved_cnt,
    a.var_othr_cor_cnt,
    a.var_end_cnt,
    a.exp_crnt_cnt,
    a.exp_rate_cnt,
    a.resolved_days,
    a.end_days
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_dcrp_all AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_financial_class_dim AS c ON a.payor_financial_class_sid = c.payor_financial_class_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS d ON a.payor_sid = d.payor_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_sequence_dim AS e ON a.payor_sequence_sid = e.payor_sequence_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_patient_type_dim AS f ON a.patient_type_sid = f.patient_type_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS g ON a.unit_num_sid = g.org_sid
     AND upper(g.org_hier_name) LIKE '%PARS ORG HIER%'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_discrepancy_mc_cost_year AS ai ON a.cost_year_sid = ai.medicare_cost_year_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_discrepancy_cost_year AS aj ON a.year_created_sid = aj.year_created_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_age AS h ON a.discrepancy_age_month_sid = h.age_sid
     AND upper(h.age_hier_name) = 'PARS_AGE_HIER'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_age AS disch ON a.discharge_age_month_sid = disch.age_sid
     AND upper(disch.age_hier_name) = 'PARS_AGE_HIER'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_scenario_dim AS i ON a.scenario_sid = i.scenario_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dollar_strf_dim AS j ON a.dollar_strtf_sid = j.dollar_strf_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim AS k ON a.reason_code_1_sid = k.reason_code_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim AS l ON a.reason_code_2_sid = l.reason_code_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim AS m ON a.reason_code_3_sid = m.reason_code_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim AS n ON a.reason_code_4_sid = n.reason_code_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_source AS z ON a.source_sid = z.source_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_discrepancy_type AS ac ON a.discrepancy_type_sid = ac.discrepancy_type_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_cc_acct_payer_status AS cc_a ON a.cc_account_payer_status_id = cc_a.cc_status_id
     AND cc_a.company_code = a.company_code
  WHERE (a.company_code, a.coid) IN(
    SELECT AS STRUCT
        secref_facility.company_code,
        secref_facility.co_id
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility
      WHERE secref_facility.user_id = session_user()
  )
;
