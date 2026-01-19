-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_dcrp_all.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_rcom_pars_dcrp_all
   OPTIONS(description='This is Monthly Discrepancy Inventory for all Customers for all Cost year hierarchies')
  AS SELECT
      a.date_sid,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
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
      ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
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
      ROUND(a.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
      k.reason_code_member AS reason_code_1_member,
      k.reason_code_alias AS reason_code_1_alias,
      k.reason_code_gen03 AS reason_code_1_gen03,
      k.reason_code_gen03_alias AS reason_code_1_gen03_alias,
      k.reason_code_gen04 AS reason_code_1_gen04,
      k.reason_code_gen04_alias AS reason_code_1_gen04_alias,
      ROUND(a.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
      l.reason_code_member AS reason_code_2_member,
      l.reason_code_alias AS reason_code_2_alias,
      l.reason_code_gen03 AS reason_code_2_gen03,
      l.reason_code_gen03_alias AS reason_code_2_gen03_alias,
      l.reason_code_gen04 AS reason_code_2_gen04,
      l.reason_code_gen04_alias AS reason_code_2_gen04_alias,
      ROUND(a.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
      m.reason_code_member AS reason_code_3_member,
      m.reason_code_alias AS reason_code_3_alias,
      m.reason_code_gen03 AS reason_code_3_gen03,
      m.reason_code_gen03_alias AS reason_code_3_gen03_alias,
      m.reason_code_gen04 AS reason_code_3_gen04,
      m.reason_code_gen04_alias AS reason_code_3_gen04_alias,
      ROUND(a.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
      n.reason_code_member AS reason_code_4_member,
      n.reason_code_alias AS reason_code_4_alias,
      n.reason_code_gen03 AS reason_code_4_gen03,
      n.reason_code_gen03_alias AS reason_code_4_gen03_alias,
      n.reason_code_gen04 AS reason_code_4_gen04,
      n.reason_code_gen04_alias AS reason_code_4_gen04_alias,
      a.source_sid,
      z.source_child AS source_member,
      z.source_alias_name,
      ROUND(a.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
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
      ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
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
      ROUND(a.var_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
      ROUND(a.var_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
      ROUND(a.var_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
      ROUND(a.var_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
      ROUND(a.var_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
      ROUND(a.exp_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
      ROUND(a.exp_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_rate_amt,
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
      {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_pars_dcrp_all AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_financial_class_dim AS c ON a.payor_financial_class_sid = c.payor_financial_class_sid
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.eis_payor_dim AS d ON a.payor_sid = d.payor_sid
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_sequence_dim AS e ON a.payor_sequence_sid = e.payor_sequence_sid
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_patient_type_dim AS f ON a.patient_type_sid = f.patient_type_sid
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS g ON a.unit_num_sid = g.org_sid
       AND upper(g.org_hier_name) LIKE '%PARS ORG HIER%'
      INNER JOIN {{ params.param_pbs_views_dataset_name }}.dim_discrepancy_mc_cost_year AS ai ON a.cost_year_sid = ai.medicare_cost_year_sid
      INNER JOIN {{ params.param_pbs_views_dataset_name }}.dim_discrepancy_cost_year AS aj ON a.year_created_sid = aj.year_created_sid
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.dim_age AS h ON a.discrepancy_age_month_sid = h.age_sid
       AND upper(h.age_hier_name) = 'PARS_AGE_HIER'
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.dim_age AS disch ON a.discharge_age_month_sid = disch.age_sid
       AND upper(disch.age_hier_name) = 'PARS_AGE_HIER'
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_scenario_dim AS i ON a.scenario_sid = i.scenario_sid
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.eis_dollar_strf_dim AS j ON a.dollar_strtf_sid = j.dollar_strf_sid
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.eis_reason_code_dim AS k ON a.reason_code_1_sid = k.reason_code_sid
      LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.eis_reason_code_dim AS l ON a.reason_code_2_sid = l.reason_code_sid
      LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.eis_reason_code_dim AS m ON a.reason_code_3_sid = m.reason_code_sid
      LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.eis_reason_code_dim AS n ON a.reason_code_4_sid = n.reason_code_sid
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.dim_source AS z ON a.source_sid = z.source_sid
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.dim_discrepancy_type AS ac ON a.discrepancy_type_sid = ac.discrepancy_type_sid
      LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_cc_acct_payer_status AS cc_a ON a.cc_account_payer_status_id = cc_a.cc_status_id
       AND upper(cc_a.company_code) = upper(a.company_code)
    WHERE (upper(a.company_code), upper(a.coid)) IN(
      SELECT AS STRUCT
          upper(secref_facility.company_code) AS company_code,
          upper(secref_facility.co_id) AS co_id
        FROM
          {{ params.param_auth_base_views_dataset_name }}.secref_facility
        WHERE secref_facility.user_id = session_user()
    )
  ;
