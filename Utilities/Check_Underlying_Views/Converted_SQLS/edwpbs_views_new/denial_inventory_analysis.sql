-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/denial_inventory_analysis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

DECLARE cw_const STRING DEFAULT bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', CASE
  WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
  ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
END), 1, 6));
CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.denial_inventory_analysis AS SELECT
    date_sub(date_add(CAST(trim(concat(substr(substr(CAST(a.date_sid as STRING), 1, 6), 1, 4), '-', substr(substr(CAST(a.date_sid as STRING), 1, 6), 5, 2), '-01')) as DATE), interval 1 MONTH), interval 1 DAY) AS pe_date,
    a.date_sid,
    a.denial_orig_age_month_sid AS age_of_denial_sid,
    k.age_alias_name AS age_of_denial_member,
    a.appeal_age_month_sid,
    l.age_alias_name AS appeal_age_month_member,
    a.iplan_id,
    a.discharge_age_month_sid,
    m.age_alias_name AS discharge_age_month_member,
    a.appeal_closing_date AS appeal_close_date,
    a.appeal_level_origination_date AS appeal_date,
    a.appeal_deadline_date,
    a.appeal_level_num AS appeal_level,
    a.work_again_date AS appeal_work_again_date,
    CASE
      WHEN a.patient_sid = -1
       AND a.source_sid = 8
       AND a.patient_dw_id = -1
       AND a.attending_physician_name IS NOT NULL THEN a.attending_physician_name
      ELSE CAST(NULL as STRING)
    END AS lumpsum_payor_gen03,
    CASE
      WHEN a.attending_physician_name IS NOT NULL
       AND (a.patient_sid = -1
       AND a.source_sid = 8
       AND a.patient_dw_id = -1) THEN CAST(NULL as STRING)
      ELSE a.attending_physician_name
    END AS attending_physician_name,
    a.total_charge_amt AS charge_total,
    a.ending_balance_amt AS current_appeal_amount,
    date_diff(date_sub(date_add(CAST(trim(concat(substr(substr(CAST(a.date_sid as STRING), 1, 6), 1, 4), '-', substr(substr(CAST(a.date_sid as STRING), 1, 6), 5, 2), '-01')) as DATE), interval 1 MONTH), interval 1 DAY), a.appeal_origination_date, DAY) AS denial_age,
    a.last_update_id AS denial_assigned_to,
    a.appeal_code_sid AS denial_code_sid,
    i.appeal_code_alias_name AS denial_code_member,
    a.appeal_disp_sid AS denial_disposition_sid,
    j.appeal_disp_alias_name AS denial_disposition_alias,
    j.appeal_disp_child AS denial_disposition_member,
    a.appeal_root_cause_sid,
    n.appeal_root_cause_alias_name,
    rc_category.appeal_root_cause_alias_name_pt AS appeal_root_cause_category,
    a.source_sid,
    o.source_member,
    a.service_code_sid,
    p.service_code_member,
    a.last_update_date AS denial_last_updated_date,
    a.appeal_origination_date AS denial_origination_date,
    a.write_off_denial_account_amt,
    a.denied_charge_amt AS denied_charges_amount,
    a.discharge_date,
    a.payor_financial_class_sid AS financial_class_sid,
    c.payor_financial_class_member AS financial_class_member,
    c.payor_financial_class_alias AS financial_class_alias,
    a.company_code,
    a.unit_num_sid,
    d.org_name_child AS unit_num_member,
    CASE
      WHEN a.company_code = 'H'
       AND same_store.same_store_ind = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    adt.admission_type_child AS admission_type,
    dt.denial_type_child AS denial_type,
    los.los_name_child AS los,
    dg.ms_drg_name_child AS drg,
    a.resolved_denial_age_month_id,
    a.resolved_discharge_age_month_id,
    r_a.age_alias_name AS resolved_denial_age_month_member,
    rd_a.age_alias_name AS resolved_discharge_age_month_member,
    a.beginning_appeal_amt AS original_appeal_amount,
    a.corrections_account_amt,
    a.overturned_account_amt AS overturned_denied_amt_current,
    e.total_overturned_account_amt AS overturned_account_amt,
    a.payor_sid,
    a.payor_dw_id,
    f.payor_member,
    f.payor_gen02,
    f.payor_gen03,
    f.payor_gen05,
    a.account_balance_amt AS patient_account_balance,
    a.patient_dw_id,
    a.patient_sid AS patient_account_number,
    a.patient_type_sid,
    g.patient_type_gen02 AS patient_type_alias,
    g.patient_type_alias AS patient_type_member_alias,
    a.payor_sequence_sid AS payor_sequence_number,
    CASE
       a.payor_sequence_sid
      WHEN 1 THEN 'P'
      WHEN 2 THEN 'S'
      WHEN 3 THEN 'T'
    END AS payor_sequence_desc,
    a.denied_charge_amt - a.overturned_account_amt AS remaining_denied_charges,
    a.trans_next_party_amt,
    a.coid,
    a.write_off_denial_account_amt + a.corrections_account_amt + a.trans_next_party_amt AS upheld_denied_amount,
    a.beginning_balance_amt,
    a.beginning_balance_count,
    a.ending_balance_amt,
    a.ending_balance_count,
    a.write_off_denial_account_count,
    a.new_denial_account_amt,
    a.new_denial_account_count,
    a.not_true_denial_amt,
    a.not_true_denial_count,
    a.corrections_account_count,
    a.overturned_account_count,
    a.trans_next_party_count,
    a.unworked_conversion_amt,
    a.unworked_conversion_count,
    a.resolved_accounts_count,
    a.below_threshold_amt,
    a.below_threshold_count,
    a.write_off_denial_account_amt + a.corrections_account_amt + a.trans_next_party_amt + a.not_true_denial_amt + a.overturned_account_amt AS resolved_denials_amount,
    a.cash_adjustment_amt,
    a.contractual_allow_adj_amt,
    a.cc_appeal_num,
    a.cc_appeal_detail_seq_num,
    a.cc_appeal_crnt_bal_amt,
    vpe.p_employer_name,
    vpe.r_employer_name,
    vpe.s_employer_name,
    vpe.o_employer_name,
    a.patient_full_name,
    a.patient_birth_date,
    a.person_role_code,
    a.pa_vendor_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_denial AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON same_store.coid = a.coid
     AND same_store.gl_close_ind = 'N'
     AND same_store.month_id = CAST(cw_const as FLOAT64)
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_financial_class_dim AS c ON c.payor_financial_class_sid = a.payor_financial_class_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS d ON a.unit_num_sid = d.org_sid
     AND upper(d.org_hier_name) LIKE '%PARS ORG HIER%'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_los AS los ON los.los_sid = a.los_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_denial_type AS dt ON dt.denial_type_sid = a.denial_type_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_pbs_drg AS dg ON dg.ms_drg_sid = a.drg_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_admission_type AS adt ON adt.admission_type_sid = a.admission_type_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwfs_base_views.dim_age AS r_a ON r_a.age_sid = a.resolved_denial_age_month_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwfs_base_views.dim_age AS rd_a ON rd_a.age_sid = a.resolved_discharge_age_month_id
    LEFT OUTER JOIN (
      SELECT
          z.patient_sid AS patientsid,
          z.payor_sid AS payorsid,
          sum(z.overturned_account_amt) AS total_overturned_account_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_denial AS z
        GROUP BY 1, 2
    ) AS e ON e.patientsid = a.patient_sid
     AND e.payorsid = a.payor_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS f ON f.payor_sid = a.payor_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_patient_type_dim AS g ON g.patient_type_sid = a.patient_type_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_service_code_dim AS h ON h.service_code_sid = a.service_code_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_appeal_code AS i ON i.appeal_code_sid = a.appeal_code_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_appeal_disp AS j ON j.appeal_disp_sid = a.appeal_disp_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwfs_base_views.dim_age AS k ON k.age_sid = a.denial_orig_age_month_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwfs_base_views.dim_age AS l ON l.age_sid = a.appeal_age_month_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwfs_base_views.dim_age AS m ON m.age_sid = a.discharge_age_month_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_root_cause AS n ON n.appeal_root_cause_sid = a.appeal_root_cause_sid
     AND upper(n.appeal_root_cause_hier_name) = 'DENIAL HIERARCHY'
    LEFT OUTER JOIN (
      SELECT
          a_0.appeal_root_cause_sid,
          a_0.appeal_root_cause_alias_name,
          substr(b.appeal_root_cause_alias_name, 1, length(b.appeal_root_cause_alias_name) - 4) AS appeal_root_cause_alias_name_pt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_root_cause AS a_0
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_root_cause AS b ON upper(a_0.appeal_root_cause_parent) = upper(b.appeal_root_cause_child)
        WHERE upper(a_0.appeal_root_cause_hier_name) = 'DENIAL HIERARCHY CATEGORY'
    ) AS rc_category ON rc_category.appeal_root_cause_sid = a.appeal_root_cause_sid
    LEFT OUTER JOIN (
      SELECT
          q.patient_dw_id,
          q.date_sid,
          r.employer_name AS p_employer_name,
          s.employer_name AS r_employer_name,
          t.employer_name AS s_employer_name,
          u.employer_name AS o_employer_name,
          q.payor_sid,
          q.payor_financial_class_sid,
          q.payor_sequence_sid,
          q.patient_type_sid,
          q.appeal_disp_sid,
          q.appeal_code_sid,
          q.discharge_age_month_sid,
          q.denial_orig_age_month_sid,
          q.appeal_age_month_sid,
          q.service_code_sid,
          q.unit_num_sid,
          q.los_sid,
          q.drg_sid,
          q.admission_type_sid,
          q.denial_type_sid
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_denial AS q
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_employer AS r ON q.coid = r.coid
           AND q.patient_dw_id = r.patient_dw_id
           AND q.payor_sequence_sid = r.iplan_insurance_order_num
           AND r.employee_code = 'P'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_employer AS s ON q.coid = s.coid
           AND q.patient_dw_id = s.patient_dw_id
           AND q.payor_sequence_sid = s.iplan_insurance_order_num
           AND s.employee_code = 'R'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_employer AS t ON q.coid = t.coid
           AND q.patient_dw_id = t.patient_dw_id
           AND q.payor_sequence_sid = t.iplan_insurance_order_num
           AND t.employee_code = 'S'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_employer AS u ON q.coid = u.coid
           AND q.patient_dw_id = u.patient_dw_id
           AND q.payor_sequence_sid = u.iplan_insurance_order_num
           AND u.employee_code = 'O'
    ) AS vpe ON vpe.patient_dw_id = a.patient_dw_id
     AND vpe.date_sid = a.date_sid
     AND vpe.payor_sid = a.payor_sid
     AND vpe.payor_financial_class_sid = a.payor_financial_class_sid
     AND vpe.payor_sequence_sid = a.payor_sequence_sid
     AND vpe.patient_type_sid = a.patient_type_sid
     AND vpe.appeal_disp_sid = a.appeal_disp_sid
     AND vpe.appeal_code_sid = a.appeal_code_sid
     AND vpe.discharge_age_month_sid = a.discharge_age_month_sid
     AND vpe.denial_orig_age_month_sid = a.denial_orig_age_month_sid
     AND vpe.appeal_age_month_sid = a.appeal_age_month_sid
     AND vpe.service_code_sid = a.service_code_sid
     AND vpe.unit_num_sid = a.unit_num_sid
     AND vpe.los_sid = a.los_sid
     AND vpe.drg_sid = a.drg_sid
     AND vpe.admission_type_sid = a.admission_type_sid
     AND vpe.denial_type_sid = a.denial_type_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_source_dim AS o ON o.source_sid = a.source_sid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_service_code_dim AS p ON p.service_code_sid = a.service_code_sid
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b
  WHERE b.co_id = a.coid
   AND b.company_code = a.company_code
   AND b.user_id = session_user()
   AND a.patient_sid <> 0
;
