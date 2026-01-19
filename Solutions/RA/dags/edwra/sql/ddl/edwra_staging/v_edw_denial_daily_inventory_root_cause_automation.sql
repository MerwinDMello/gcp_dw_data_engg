-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/v_edw_denial_daily_inventory_root_cause_automation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

DECLARE cw_const STRING DEFAULT bqutil.fn.cw_td_normalize_number(CASE
  WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_hcfa_icd10_code IS NULL
   OR trim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_hcfa_icd10_code) = '' THEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_code_hcfa
  ELSE a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_hcfa_icd10_code
END);
CREATE OR REPLACE VIEW {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_denial_daily_inventory_root_cause_automation AS WITH a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri AS (
  SELECT
      row_number() OVER () AS rn,
      a11.*,
      fp.*,
      a12.*,
      org.*,
      fc.*,
      und.*,
      discr.*,
      concdenatdisch.*,
      pdu.*,
      cm_full_doc_ind.*,
      fpd.*,
      xfg.*,
      dcg.*,
      tmn.*,
      drg.*,
      btc.*,
      btc1.*,
      ri.*
    FROM
      {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_denial_inventory AS a11
      LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpf_views.fact_patient AS fp ON CAST(bqutil.fn.cw_td_normalize_number(a11.account_no) as FLOAT64) = fp.pat_acct_num
       AND rtrim(a11.coid) = rtrim(fp.coid)
      INNER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.facility_dimension AS a12 ON rtrim(a11.coid) = rtrim(a12.coid)
      INNER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.dim_rcm_organization AS org ON rtrim(a11.coid) = rtrim(org.coid)
      LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.eis_payor_financial_class_dim AS fc ON a11.payor_financial_class = CASE
        WHEN upper(left(fc.payor_financial_class_alias, 2)) IN(
          'NO', 'TR', 'PA'
        ) THEN CAST(NULL as INT64)
        ELSE CAST(bqutil.fn.cw_td_normalize_number(left(fc.payor_financial_class_alias, 2)) as INT64)
      END
      LEFT OUTER JOIN (
        SELECT
            cc_discrepancy.pat_acct_num,
            cc_discrepancy.coid,
            cc_discrepancy.iplan_id,
            cc_discrepancy.iplan_order_num,
            max(cc_discrepancy.underpayment_date) AS maxupymtdate,
            max(cc_discrepancy.extract_date_time) AS extract_date
          FROM
            {{ params.param_parallon_ra_views_dataset_name }}.cc_discrepancy
          WHERE cc_discrepancy.payor_due_amt > 0
          GROUP BY 1, 2, 3, 4
      ) AS und ON und.pat_acct_num = CAST(bqutil.fn.cw_td_normalize_number(a11.account_no) as FLOAT64)
       AND rtrim(und.coid) = rtrim(a11.coid)
       AND und.iplan_id = a11.iplan_id
       AND und.iplan_order_num = a11.payer_rank
       AND und.maxupymtdate < a11.first_denial_date
      LEFT OUTER JOIN (
        SELECT
            cw_win_ss.account_no,
            cw_win_ss.coid,
            cw_win_ss.iplan_id,
            cw_win_ss.payer_rank,
            cw_win_ss.maxdiscrdate
          FROM
            (
              SELECT
                  max(v_edw_daily_discrepancy_inventory.account_no) AS account_no,
                  v_edw_daily_discrepancy_inventory.coid,
                  v_edw_daily_discrepancy_inventory.iplan_id,
                  v_edw_daily_discrepancy_inventory.payer_rank,
                  CASE
                    WHEN v_edw_daily_discrepancy_inventory.payor_due_amount > 0 THEN v_edw_daily_discrepancy_inventory.underpayment_date
                    WHEN v_edw_daily_discrepancy_inventory.payor_due_amount < 0 THEN v_edw_daily_discrepancy_inventory.overpayment_date
                    ELSE v_edw_daily_discrepancy_inventory.non_fin_disc_date
                  END AS maxdiscrdate,
                  upper(v_edw_daily_discrepancy_inventory.account_no) AS cw_win_part_0
                FROM
                  {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_discrepancy_inventory
                GROUP BY 6, 2, 3, 4, 5
            ) AS cw_win_ss
          QUALIFY row_number() OVER (PARTITION BY cw_win_ss.cw_win_part_0, cw_win_ss.coid, cw_win_ss.iplan_id, cw_win_ss.payer_rank ORDER BY maxdiscrdate DESC) = 1
      ) AS discr ON upper(rtrim(discr.account_no)) = upper(rtrim(a11.account_no))
       AND rtrim(discr.coid) = rtrim(a11.coid)
       AND discr.iplan_id = a11.iplan_id
       AND discr.payer_rank = a11.payer_rank
       AND a11.close_date IS NULL
      LEFT OUTER JOIN (
        SELECT
            fp_0.patient_dw_id,
            min(CASE
              WHEN hcm_avoid_denied_day_info.date_of_denial IS NULL THEN DATE '1900-01-01'
              ELSE hcm_avoid_denied_day_info.date_of_denial
            END) AS min_date_of_denial
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwcm_views.ref_hcm_days_type AS hcm_avoid_denied_days_type
            RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_day_info ON hcm_avoid_denied_day_info.type_of_day_id = hcm_avoid_denied_days_type.hcm_days_type_id
             AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
             AND rtrim(hcm_avoid_denied_days_type.active_dw_ind) = 'Y'
            RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_day ON hcm_avoid_denied_day.hcm_avoid_denied_day_id = hcm_avoid_denied_day_info.hcm_avoid_denied_day_id
             AND rtrim(hcm_avoid_denied_day.active_dw_ind) = 'Y'
             AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
            RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_encounter ON hcm_avoid_denied_day.midas_encounter_id = cm_encounter.midas_encounter_id
             AND rtrim(hcm_avoid_denied_day.active_dw_ind) = 'Y'
             AND rtrim(cm_encounter.active_dw_ind) = 'Y'
            RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpf_views.fact_patient AS fp_0 ON fp_0.patient_dw_id = cm_encounter.patient_dw_id
             AND rtrim(cm_encounter.active_dw_ind) = 'Y'
            LEFT OUTER JOIN (
              SELECT
                  rfc.patient_dw_id,
                  rfc.financial_class_code,
                  rank() OVER (PARTITION BY rfc.patient_dw_id ORDER BY rfc.financial_class_date DESC) AS disch_fc_seq
                FROM
                  `{{ params.param_parallon_cur_project_id }}`.edwpf_views.fact_patient AS fp_1
                  INNER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.registration_financial_class AS rfc ON rfc.patient_dw_id = fp_1.patient_dw_id
                WHERE fp_1.discharge_date IS NOT NULL
                 AND rfc.financial_class_date <= fp_1.discharge_date
            ) AS disch_financial_class ON fp_0.patient_dw_id = disch_financial_class.patient_dw_id
             AND disch_financial_class.disch_fc_seq = 1
            LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_facility ON rtrim(cm_facility.company_code) = rtrim(cm_encounter.company_code)
             AND rtrim(cm_facility.coid) = rtrim(cm_encounter.coid)
             AND rtrim(cm_facility.midas_facility_code) = rtrim(cm_encounter.midas_facility_code)
             AND rtrim(cm_facility.active_dw_ind) = 'Y'
             AND rtrim(cm_encounter.active_dw_ind) = 'Y'
            CROSS JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.fact_facility
          WHERE rtrim(fact_facility.company_code) = 'H'
           AND rtrim(fact_facility.coid_status_code) = 'F'
           AND fp_0.admission_patient_type_code IN(
            'I', 'IB'
          )
           AND disch_financial_class.financial_class_code <> 99
           AND upper(hcm_avoid_denied_days_type.hcm_days_type_name) LIKE '%DEN %'
          GROUP BY 1
      ) AS concdenatdisch ON fp.patient_dw_id = concdenatdisch.patient_dw_id
      LEFT OUTER JOIN (
        SELECT DISTINCT
            pdu_0.pat_acct_num,
            pdu_0.unit_num,
            pdu_0.iplan_ins1_id,
            pdu_0.pilot_acct_ind,
            pdu_0.account_id AS pdu_accountid,
            pdu_0.discharge_date AS pdu_discharge_date,
            pdu_0.work_group_name AS pdu_work_group_name,
            coalesce(pdu_0.determination_reason_desc, ' ') AS pdu_determination_reason_desc,
            pdu_0.initial_auth_status_code AS pdu_initial_auth_status_code,
            pdu_0.clinical_submitted_date AS pdu_clinical_submitted_date,
            pdu_0.interqual_completed_ind AS pdu_interqual_completed_ind
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.prebill_denial_detail AS pdu_0
          WHERE pdu_0.rptg_date = (
            SELECT
                max(prebill_denial_detail.rptg_date)
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.prebill_denial_detail
          )
           AND upper(rtrim(pdu_0.initial_auth_status_code)) <> 'EXCEPTION'
           AND (pdu_0.pat_acct_num, pdu_0.coid, pdu_0.account_id) IN(
            SELECT AS STRUCT
                p.pat_acct_num,
                p.coid,
                p.max_account_id
              FROM
                (
                  SELECT
                      pdua.pat_acct_num,
                      pdua.coid,
                      pdua.iplan_ins1_id,
                      max(pdua.account_id) AS max_account_id,
                      max(pdua.rptg_date) AS max_rptg_date,
                      max(pdua.created_date) AS max_created_date
                    FROM
                      `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.prebill_denial_detail AS pdua
                    WHERE (pdua.pat_acct_num, pdua.coid, pdua.created_date) IN(
                      SELECT AS STRUCT
                          prebill_denial_detail.pat_acct_num,
                          prebill_denial_detail.coid,
                          max(prebill_denial_detail.created_date)
                        FROM
                          `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.prebill_denial_detail
                        GROUP BY 1, 2
                    )
                     AND pdua.rptg_date = (
                      SELECT
                          max(prebill_denial_detail.rptg_date)
                        FROM
                          `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.prebill_denial_detail
                    )
                    GROUP BY 1, 2, 3
                ) AS p
          )
      ) AS pdu ON CAST(bqutil.fn.cw_td_normalize_number(a11.unit_num) as INT64) = CAST(bqutil.fn.cw_td_normalize_number(pdu.unit_num) as INT64)
       AND CAST(bqutil.fn.cw_td_normalize_number(a11.account_no) as FLOAT64) = pdu.pat_acct_num
       AND (rtrim(pdu.pilot_acct_ind) = 'N'
       OR pdu.pilot_acct_ind IS NULL)
      LEFT OUTER JOIN (
        SELECT
            cm_denial_indicator.patient_dw_id
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_denial_indicator
          WHERE rtrim(cm_denial_indicator.revised_full_doc_ind) = 'Y'
           AND rtrim(cm_denial_indicator.avoidable_denied_days_ind) = 'N'
          GROUP BY 1
      ) AS cm_full_doc_ind ON fp.patient_dw_id = cm_full_doc_ind.patient_dw_id
      LEFT OUTER JOIN (
        SELECT
            t.patient_dw_id,
            t.iplan_id,
            sum(t.ar_transaction_amt) AS payment_amt
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwpf_views.ar_transaction AS t
            INNER JOIN (
              SELECT
                  v_edw_daily_denial_inventory.coid,
                  max(v_edw_daily_denial_inventory.account_no) AS account_no,
                  v_edw_daily_denial_inventory.iplan_id,
                  v_edw_daily_denial_inventory.first_denial_date
                FROM
                  {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_denial_inventory
                GROUP BY 1, upper(v_edw_daily_denial_inventory.account_no), 3, 4
            ) AS dd ON ROUND(CAST(bqutil.fn.cw_td_normalize_number(dd.account_no) as NUMERIC), 0, 'ROUND_HALF_EVEN') = t.pat_acct_num
             AND rtrim(dd.coid) = rtrim(t.coid)
             AND dd.iplan_id = t.iplan_id
             AND t.ar_transaction_enter_date <= dd.first_denial_date
          WHERE rtrim(t.transaction_type_code) = '1'
          GROUP BY 1, 2
      ) AS fpd ON fp.patient_dw_id = fpd.patient_dw_id
       AND a11.iplan_id = fpd.iplan_id
       AND fpd.payment_amt > 0
      LEFT OUTER JOIN (
        SELECT DISTINCT
            patient_condition_code.patient_dw_id
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwpf_views.patient_condition_code
          WHERE patient_condition_code.condition_code IN(
            'XF', 'XG'
          )
      ) AS xfg ON xfg.patient_dw_id = fp.patient_dw_id
      LEFT OUTER JOIN (
        SELECT
            trim(substr(a.appeal_code_alias_name, 1, 2)) AS denial_code,
            a.appeal_code_alias_name AS appeal_code_alias_namea,
            b.appeal_code_alias_name AS appeal_code_alias_nameb,
            c.appeal_code_alias_name AS denial_code_category
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.dim_appeal_code AS a
            INNER JOIN (
              SELECT
                  dim_appeal_code.*
                FROM
                  `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.dim_appeal_code
                WHERE dim_appeal_code.sort_key_num BETWEEN 300 AND 400
            ) AS b ON upper(rtrim(a.appeal_code_parent)) = upper(rtrim(b.appeal_code_child))
            INNER JOIN (
              SELECT
                  dim_appeal_code.*
                FROM
                  `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.dim_appeal_code
                WHERE dim_appeal_code.sort_key_num BETWEEN 200 AND 300
            ) AS c ON upper(rtrim(b.appeal_code_parent)) = upper(rtrim(c.appeal_code_child))
          WHERE a.sort_key_num >= 400
      ) AS dcg ON upper(rtrim(dcg.denial_code)) = upper(rtrim(left(a11.external_appeal_code, 2)))
      LEFT OUTER JOIN (
        SELECT DISTINCT
            init_adm_pat_type.patient_dw_id,
            CASE
              WHEN clinical_ed_event_date_time.greet_date <> DATE '1900-01-01' THEN date_diff(fact_patient.discharge_date, clinical_ed_event_date_time.greet_date, DAY)
              ELSE date_diff(fact_patient.discharge_date, CASE
                WHEN init_adm_pat_type.eff_from_date > fact_patient.admission_date THEN fact_patient.admission_date
                ELSE init_adm_pat_type.eff_from_date
              END, DAY)
            END AS two_mid_inhouse_days
          FROM
            (
              SELECT
                  admission_patient_type.patient_dw_id,
                  admission_patient_type.eff_from_date,
                  admission_patient_type.admission_patient_type_code,
                  row_number() OVER (PARTITION BY admission_patient_type.patient_dw_id ORDER BY admission_patient_type.patient_dw_id, admission_patient_type.eff_from_date) AS rec_num
                FROM
                  `{{ params.param_parallon_cur_project_id }}`.edwcm_views.admission_patient_type
                WHERE admission_patient_type.admission_patient_type_code NOT IN(
                  'EP ', 'OP ', 'SP ', 'IP ', 'ER ', 'OR ', 'SR ', 'ERV', 'ORV', 'SRV', 'N  '
                )
            ) AS init_adm_pat_type
            RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.fact_patient ON init_adm_pat_type.patient_dw_id = fact_patient.patient_dw_id
             AND init_adm_pat_type.rec_num = 1
            LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.clinical_ed_event_date_time ON clinical_ed_event_date_time.patient_dw_id = fact_patient.patient_dw_id
      ) AS tmn ON tmn.patient_dw_id = fp.patient_dw_id
      LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.dim_pbs_drg AS drg ON CAST(cw_const as FLOAT64) = drg.ms_drg_sid
      LEFT OUTER JOIN (
        SELECT
            rh_837_claim.bill_type_code,
            rh_837_claim.patient_dw_id,
            rh_837_claim.iplan_id,
            rh_837_claim.iplan_insurance_order_num
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwpf_views.rh_837_claim
          WHERE rtrim(rh_837_claim.bill_type_code) = '131'
          GROUP BY 1, 2, 3, 4
      ) AS btc ON btc.patient_dw_id = fp.patient_dw_id
       AND btc.iplan_id = a11.iplan_id
       AND btc.iplan_insurance_order_num = a11.payer_rank
      LEFT OUTER JOIN (
        SELECT
            rh_837_claim.bill_type_code,
            rh_837_claim.patient_dw_id,
            rh_837_claim.iplan_id,
            rh_837_claim.iplan_insurance_order_num
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edwpf_views.rh_837_claim
          WHERE rtrim(rh_837_claim.bill_type_code) = '137'
          GROUP BY 1, 2, 3, 4
      ) AS btc1 ON btc1.patient_dw_id = fp.patient_dw_id
       AND btc1.iplan_id = a11.iplan_id
       AND btc1.iplan_insurance_order_num = a11.payer_rank
      LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpf_views.registration_iplan AS ri ON ri.patient_dw_id = fp.patient_dw_id
       AND ri.iplan_id = a11.iplan_id
       AND ri.eff_to_date = DATE '9999-12-31'
), a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc AS (
  SELECT
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.rn,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.schema_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.mon_account_payer_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pass_type,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.account_no,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ssc_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.rate_schedule_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ssc_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.unit_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.rate_schedule_eff_begin_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.rate_schedule_eff_end_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dob,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.insurance_provider_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payer_group_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.billing_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.billing_contact_person,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.authorization_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payer_patient_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payer_rank,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_financial_class,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_financial_class,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.accounting_period,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.major_payer_grp,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.reason_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.billing_status,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_service_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_account_status,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.cc_patient_type,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_discharge_status,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_patient_type,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.cancel_bill_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.admit_source,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.admit_type,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_drg,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.remit_drg_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.attending_physician_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.attending_physician_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.service_date_begin,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.discharge_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.comparison_method,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.project_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.work_queue_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.status_category_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.status_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.status_phase_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.calc_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_charges,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_actual_los,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_billed_charges,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.covered_charges,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_expected_payment,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_expected_adjustment,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_pt_responsibility_actual,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_variance_adjustment,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_payments,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_denial_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_due_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_total_account_bal,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ar_amount,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.otd_amt_net,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.writeoff_amt_net,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.cash_adj_amt_net,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.otd_to_date_amount_mtd,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.writeoff_amt_mtd,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.cash_adj_amt_mtd,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.max_aplno,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.max_seqno,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_orig_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.current_appealed_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.current_appeal_balance,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_date_created,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sequence_date_created,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.close_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.max_seq_deadline_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sequence_creator,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_owner,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_modifier,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.disp_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.disp_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.web_disp_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.web_disposition_type,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.root_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.root_cause_description,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.root_cause_dtl,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.external_appeal_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.apl_appeal_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.apl_appeal_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.first_denial_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.denial_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pa_denial_update_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.first_activity_create_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_activity_completion_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_activity_completion_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_user_activity_create_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_reason_change_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_status_change_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_project_change_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_owner_change_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.activity_due_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.activity_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.activity_subject,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.activity_owner,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_activity_ownr,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_initiation_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_activity_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_activity_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_status_change_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.activity_due_date_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.latest_seq_creation_date_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_deadline_days_remaining,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_activity_subj,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_activity_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.extract_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payer_category,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.source_system_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.seq_no,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dw_last_update_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.row_count,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.expected_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.new_appeal_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.disposition_code_modified_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.disposition_code_modified_by,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.vendor_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sub_unit_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.latest_iplan_change_date_pa,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.artiva_activity_due_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appsent_excrpt_disp_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.status_kpi,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.followup_kpi,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appealsent_kpi,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.deadline_kpi,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appealsent_deadline_kpi,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.post_sent_kpi,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.status_kpi_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.followup_kpi_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appealsent_kpi_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.deadline_kpi_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appealsent_deadline_kpi_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.post_sent_kpi_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.statusstratification,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.followup_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.followup_strat_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appealsentstratification,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.deadlinestratification,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.deadlinestratificationint,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.post_sent_category,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.post_sent_category_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.kpi_category,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.kpi_category_int,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appealsentflag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.followupdispflag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.denial_orig_age_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.denial_orig_age_stra,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.adjusted_current_appeal_balance,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.negative_curr_app_bal_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dollarstratint,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dollarstrat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dollarstratfp,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dollarstratfpint,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.unworked_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.open_close_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.project_type,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_due_amt_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.discharge_age_to_denial,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.discharge_age_to_denial_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.discharge_age_to_denial_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_activity_age_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_activity_age_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_age_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_age_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.first_denial_age_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.first_denial_age_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.discharge_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.latest_seq_creation_date_age_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.latest_seq_creation_date_age_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_reason_aging_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_reason_aging_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.disposition_mod_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.disposition_mod_aging_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.disposition_mod_aging_strat,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.artiva_activity_due_date_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_level,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.prior_appeal_response_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_sent_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.company_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid AS coid_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.unit_num AS unit_num_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sub_unit_num AS sub_unit_num_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.admission_patient_type_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pat_acct_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_type_code_pos1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.admission_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.discharge_date AS discharge_date_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.final_bill_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.financial_class_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.account_status_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.admission_source_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.admission_type_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.drg_code_payor,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.drg_code_hcfa,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.drg_hcfa_icd10_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.drg_desc_hcfa,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.drg_payment_weight_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.drg_code_classic,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.rdrg_code_pos4,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.social_security_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_person_dw_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_empi_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_discharge_month_age,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_address_dw_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_zip_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.resp_party_zip_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.employer_dw_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_physician_num_attend,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_physician_num_admit,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_physician_num_pcp,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_dw_id_ins1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id_ins1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ins_contract_id_ins1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.major_payor_id_ins1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.masterfacts_schema_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ins_product_code_ins1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ins_product_name_ins1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sub_payor_group_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_dw_id_ins2,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id_ins2,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_dw_id_ins3,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id_ins3,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.calculated_los,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_account_balance_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_billed_charges AS total_billed_charges_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_anc_charges,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_rb_charges,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_payments AS total_payments_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_adjustment_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_contract_allow_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_write_off_bad_debt_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_write_off_other_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_write_off_other_adj_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_uninsured_discount_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_write_off_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_charity_write_off_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_charity_write_off_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_patient_liability_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_patient_payment_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_patient_payment_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.initial_statement_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.last_statement_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.exempt_indicator,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.casemix_exempt_indicator,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.expected_pmt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.primary_icd_version_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.diag_code_admit,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.diag_code_final,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_01,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_02,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_03,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_04,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_05,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_06,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_07,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_08,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_09,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_10,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_11,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_12,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_13,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_14,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.proc_code_15,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_diag_admit_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_diag_final_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_01,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_02,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_03,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_04,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_05,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_06,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_07,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_08,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_09,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_10,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_11,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_12,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_13,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_14,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icd10_proc_code_15,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dss_op_cpt_hier,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.aps_op_cpt_reimb_hier_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.total_midnights_in_house_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.stop_loss_lesser_of_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.source_system_code AS source_system_code_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dw_last_update_date_time,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.unit_num AS unit_num_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.company_code AS company_code_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid AS coid_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.c_level,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.corporate_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.s_level,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sector_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.b_level,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.group_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.r_level,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.division_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.d_level,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.market_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.f_level,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.cons_facility_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.lob_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.lob_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sub_lob_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sub_lob_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.state_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pas_id_current,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pas_current_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pas_id_future,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pas_future_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_7_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_8_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_phys_svc_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_asd_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_imaging_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_oncology_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_cath_lab_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_intl_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summary_other_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pas_coid,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pas_status,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.company_code_operations,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.osg_pas_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.abs_facility_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.abl_facility_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.intl_pmis_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.hsc_member_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.cons_pas_coid,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.cons_pas_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.company_code AS company_code_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid AS coid_1_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.customer_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.customer_short_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.customer_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ssc_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.unit_num AS unit_num_1_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_mnemonic,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.group_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.division_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.market_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.f_level AS f_level_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.partnership_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.go_live_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.eff_from_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.eff_to_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ssc_alias_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.division_alias_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ssc_name AS ssc_name_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ssc_alias_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.consolidated_ssc_alias_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.corporate_name AS corporate_name_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.group_name AS group_name_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.market_name AS market_name_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.division_name AS division_name_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.group_alias_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.market_alias_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.division_alias_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ssc_coid,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid_name AS coid_name_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.consolidated_ssc_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_name AS facility_name_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_state_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.medicare_expansion_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.medicaid_conversion_vendor_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.facility_close_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.his_vendor_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.rcps_migration_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.discrepancy_threshold_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sma_high_dollar_threshold_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.hsc_member_ind AS hsc_member_ind_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.clear_contract_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.client_outbound_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.him_conversion_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.summ_days_release_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_financial_class_alias,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_financial_class_gen02,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_financial_class_gen03,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_financial_class_gen04,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_financial_class_member,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_financial_class_sid,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_fin_class_gen02_alias,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_fin_class_gen04_alias,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_fin_class_gen03_alias,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pat_acct_num AS pat_acct_num_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid AS coid_1_2,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id AS iplan_id_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_order_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.maxupymtdate,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.extract_date AS extract_date_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.account_no AS account_no_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid AS coid_1_3,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id AS iplan_id_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payer_rank AS payer_rank_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.maxdiscrdate,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.min_date_of_denial,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pat_acct_num AS pat_acct_num_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.unit_num AS unit_num_1_2,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_ins1_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pilot_acct_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pdu_accountid,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pdu_discharge_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pdu_work_group_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pdu_determination_reason_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pdu_initial_auth_status_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pdu_clinical_submitted_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pdu_interqual_completed_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id AS iplan_id_1_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payment_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1_2,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.denial_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_code_alias_namea,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.appeal_code_alias_nameb,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.denial_code_category,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1_3,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.two_mid_inhouse_days,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_drg_sid,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.aso_bso_storage_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_drg_name_parent,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_drg_name_child,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_drg_child_alias_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.alias_table_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.sort_key_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.consolidation_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.storage_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.two_pass_calc_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.formula_text,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_drg_med_surg_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_drg_med_surg_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_chois_prod_line_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_chois_prod_line_desc,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.member_solve_order_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ms_drg_hier_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.bill_type_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1_4,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id AS iplan_id_1_2,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_insurance_order_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.bill_type_code AS bill_type_code_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1_5,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id AS iplan_id_1_3,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_insurance_order_num AS iplan_insurance_order_num_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id AS patient_dw_id_1_6,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_dw_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_insurance_order_num AS iplan_insurance_order_num_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.payor_mail_to_name,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coid AS coid_1_4,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.company_code AS company_code_1_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.iplan_id AS iplan_id_1_4,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.eff_from_date AS eff_from_date_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.eff_to_date AS eff_to_date_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pat_acct_num AS pat_acct_num_1_1,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.person_role_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.pat_relationship_to_ins_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.policy_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.group_name AS group_name_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.group_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.hic_claim_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.signed_pat_rel_on_file_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.signed_assn_benf_on_file_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.treatment_authorization_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.verification_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.precertification_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.recertification_day_count,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.edit_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dependent_maximum_age_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.student_max_age_num,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.deductible_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coinsurance_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.source_system_code AS source_system_code_1_0,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dw_load_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.dw_change_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.health_plan_patient_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.health_plan_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.auto_post_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.gross_reimbursement_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.gross_reimbursement_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.expected_contractual_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.expected_contractual_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.coins_amt_source_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.calc_coins_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.calc_coins_amt_source_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.calc_coins_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.gross_reimbursement_var_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.gross_reimbursement_var_date,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.partb_professional_fee_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.blood_deductible_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.outpatient_pps_flag_tricare,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.outpatient_pps_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.irf_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.snf_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.psych_flag,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.log_format_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.non_covered_charge_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.copay_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.auto_post_amt,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.major_payor_group_id,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.ub04_pat_relation_to_ins_code,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.medicare_inpatient_outlier_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.precertification_ind,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icn,
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.icn50,
      enc.midas_encounter_id,
      enc.eff_from_date AS eff_from_date_1_0,
      enc.patient_dw_id AS patient_dw_id_1_7,
      enc.company_code AS company_code_1_2,
      enc.coid AS coid_1_5,
      enc.midas_facility_code,
      enc.eff_to_date AS eff_to_date_1_0,
      enc.location_id,
      enc.admitting_service_id,
      enc.iq_adm_initial_review_empl_id,
      enc.pat_acct_num AS pat_acct_num_1_2,
      enc.total_review_cnt,
      enc.completed_review_cnt,
      enc.midas_acct_num,
      enc.initial_record_ind,
      enc.admit_weekend_ind,
      enc.pdu_ind,
      enc.iq_adm_rev_type_ip_ind,
      enc.iq_adm_rev_type_obs_ind,
      enc.iq_adm_rev_type_ip_ptd_ind,
      enc.iq_adm_rev_type_obs_ptd_ind,
      enc.iq_adm_rev_type_ip_mn_met_ind,
      enc.iq_adm_rev_type_obs_mn_met_ind,
      enc.iq_adm_initial_review_hour,
      enc.iq_adm_initial_rev_date_time,
      enc.iq_adm_rev_criteria_status,
      enc.iq_adm_rev_location_id,
      enc.midas_encounter_last_updt_date,
      enc.external_pa_referral_cm_ind,
      enc.external_pa_referral_apel_ind,
      enc.external_pa_referral_other_ind,
      enc.external_pa_referral_cm_cnt,
      enc.external_pa_referral_apel_cnt,
      enc.external_pa_referral_other_cnt,
      enc.external_pa_referral_disp_id,
      enc.external_pa_referral_date_time,
      enc.external_pa_referral_id,
      enc.midas_last_ip_encounter_id,
      enc.denial_onbase_unique_id,
      enc.document_date,
      enc.bpci_episode_group_id,
      enc.bpci_data_science_percentage_num,
      enc.bpci_data_science_date,
      enc.concurrent_denial_code,
      enc.inpatient_admit_review_cnt,
      enc.first_inpatient_admit_review_date,
      enc.last_inpatient_admit_review_date,
      enc.inpatient_admit_review_delay_day_cnt,
      enc.inpatient_admit_review_1_day_delay_ind,
      enc.obs_prog_rev_after_disch_ind,
      enc.go_live_date_ind,
      enc.source_system_code AS source_system_code_1_1,
      enc.active_dw_ind,
      enc.dw_last_update_date_time AS dw_last_update_date_time_1
    FROM
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri
      INNER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_encounter AS enc ON a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.patient_dw_id = enc.patient_dw_id
       AND rtrim(enc.active_dw_ind) = 'Y'
    WHERE (enc.patient_dw_id, enc.midas_encounter_last_updt_date) IN(
      SELECT AS STRUCT
          cm_encounter.patient_dw_id,
          max(cm_encounter.midas_encounter_last_updt_date)
        FROM
          `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_encounter
        WHERE rtrim(cm_encounter.active_dw_ind) = 'Y'
        GROUP BY 1
    )
), enc AS (
  SELECT
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.*,
      CAST(NULL as INT64) AS null_0,
      CAST(NULL as DATE) AS null_1,
      CAST(NULL as NUMERIC) AS null_2,
      CAST(NULL as STRING) AS null_3,
      CAST(NULL as STRING) AS null_4,
      CAST(NULL as STRING) AS null_5,
      CAST(NULL as DATE) AS null_6,
      CAST(NULL as INT64) AS null_7,
      CAST(NULL as INT64) AS null_8,
      CAST(NULL as INT64) AS null_9,
      CAST(NULL as NUMERIC) AS null_10,
      CAST(NULL as INT64) AS null_11,
      CAST(NULL as INT64) AS null_12,
      CAST(NULL as STRING) AS null_13,
      CAST(NULL as STRING) AS null_14,
      CAST(NULL as STRING) AS null_15,
      CAST(NULL as STRING) AS null_16,
      CAST(NULL as STRING) AS null_17,
      CAST(NULL as STRING) AS null_18,
      CAST(NULL as STRING) AS null_19,
      CAST(NULL as STRING) AS null_20,
      CAST(NULL as STRING) AS null_21,
      CAST(NULL as STRING) AS null_22,
      CAST(NULL as INT64) AS null_23,
      CAST(NULL as DATETIME) AS null_24,
      CAST(NULL as STRING) AS null_25,
      CAST(NULL as INT64) AS null_26,
      CAST(NULL as DATE) AS null_27,
      CAST(NULL as STRING) AS null_28,
      CAST(NULL as STRING) AS null_29,
      CAST(NULL as STRING) AS null_30,
      CAST(NULL as INT64) AS null_31,
      CAST(NULL as INT64) AS null_32,
      CAST(NULL as INT64) AS null_33,
      CAST(NULL as INT64) AS null_34,
      CAST(NULL as DATETIME) AS null_35,
      CAST(NULL as INT64) AS null_36,
      CAST(NULL as INT64) AS null_37,
      CAST(NULL as INT64) AS null_38,
      CAST(NULL as DATE) AS null_39,
      CAST(NULL as INT64) AS null_40,
      CAST(NULL as NUMERIC) AS null_41,
      CAST(NULL as DATE) AS null_42,
      CAST(NULL as STRING) AS null_43,
      CAST(NULL as INT64) AS null_44,
      CAST(NULL as DATE) AS null_45,
      CAST(NULL as DATE) AS null_46,
      CAST(NULL as INT64) AS null_47,
      CAST(NULL as STRING) AS null_48,
      CAST(NULL as STRING) AS null_49,
      CAST(NULL as STRING) AS null_50,
      CAST(NULL as STRING) AS null_51,
      CAST(NULL as STRING) AS null_52,
      CAST(NULL as DATETIME) AS null_53
    FROM
      a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri
    WHERE a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri.rn NOT IN(
      SELECT
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.rn AS rn
        FROM
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc
    )
)
SELECT
    a.account_no,
    a.unit_num,
    a.iplan_id,
    a.payer_rank,
    opt_tact_root_cause AS opt_tact_root_cause,
    CASE
       upper(rtrim(opt_tact_root_cause))
      WHEN 'IDAPI' THEN 1
      WHEN 'MNCDC' THEN 2
      WHEN 'MNCDI' THEN 3
      WHEN 'IAREI' THEN 4
      WHEN 'ICPAP' THEN 5
      WHEN 'ICEIR' THEN 6
      ELSE 7
    END AS op_tact_root_cause_sid
  FROM
    (
      SELECT
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.schema_id,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.mon_account_payer_id,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pass_type,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.account_no,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.coid,
          upper(left(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.ssc_name, 3)) AS ssc_id,
          upper(left(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.ssc_name, 5)) AS ssc_name,
          CASE
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.company_code_1) = 'H' THEN 'HCA'
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.company_code_1) = 'A' THEN 'LIF'
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_coid) = '26600' THEN 'MER'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.coid_1 IN(
              '13100', '13110', '13130', '13140', '13190', '13200', '13230'
            ) THEN 'CAP'
            ELSE 'OTH'
          END AS cmp_id,
          cmp_name AS cmp_name,
          CASE
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.company_code_1) = 'H' THEN 'H'
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.company_code_1) = 'A' THEN 'A'
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_coid) = '26600' THEN 'M'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.coid_1 IN(
              '13100', '13110', '13130', '13140', '13190', '13200', '13230'
            ) THEN 'C'
            ELSE 'N'
          END AS company_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.b_level AS grp_id,
          left(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.group_name, 5) AS grp_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.r_level AS div_id,
          left(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.division_name, 5) AS div_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.d_level AS mkt_id,
          upper(left(trim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.market_name), 5)) AS mkt_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.rate_schedule_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.facility_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.unit_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.rate_schedule_eff_begin_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.rate_schedule_eff_end_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dob,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iplan_id,
          coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.insurance_provider_name, ' ') AS insurance_provider_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_group_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.billing_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.billing_contact_person,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.authorization_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_patient_id,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_rank,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_financial_class,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.accounting_period,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.major_payer_grp,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.reason_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.billing_status,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_service_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_account_status,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.cc_patient_type,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_discharge_status,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_patient_type,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.cancel_bill_ind,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.admit_source,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.admit_type,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_drg,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.attending_physician_id,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.attending_physician_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.service_date_begin,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.discharge_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.comparison_method,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.project_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.work_queue_name,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.status_category_desc,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.status_desc,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.status_phase_desc,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calc_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_charges,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_actual_los,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_billed_charges,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.covered_charges,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_expected_payment,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_expected_adjustment,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_pt_responsibility_actual,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_variance_adjustment,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_payments,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.total_denial_amt,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_due_amt,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_total_account_bal,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.ar_amount,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.otd_to_date_amount_mtd,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.max_aplno,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.max_seqno,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_orig_amt,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.current_appealed_amt,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.adjusted_current_appeal_balance,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.negative_curr_app_bal_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_date_created,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.sequence_date_created,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.close_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.open_close_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.max_seq_deadline_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.sequence_creator,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_owner,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_modifier,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disp_code,
          coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disp_desc, ' ') AS disp_desc,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.web_disp_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.root_code,
          coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.root_cause_description, ' ') AS root_cause_description,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.root_cause_dtl,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.external_appeal_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.apl_appeal_code,
          coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.apl_appeal_desc, ' ') AS apl_appeal_desc,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.first_denial_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.denial_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pa_denial_update_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.first_activity_create_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_activity_completion_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_activity_completion_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_user_activity_create_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_reason_change_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_status_change_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_project_change_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_owner_change_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.activity_due_date,
          substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.activity_desc, 1, 100) AS activity_desc,
          coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.activity_subject, ' ') AS activity_subject,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.activity_owner,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_ownr,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_subj,
          substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_desc, 1, 100) AS appeal_sent_activity_desc,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.statusstratification,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.status_kpi_int,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.status_kpi,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.followup_kpi_int,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.followup_kpi,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appealsent_kpi_int,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appealsent_kpi,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.deadline_kpi_int,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.deadline_kpi,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.extract_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_category,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.dw_last_update_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.project_type,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.dollarstratint,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.dollarstrat,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_due_amt_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.cash_adj_amt_net,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.writeoff_amt_net,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.deadlinestratificationint,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.deadlinestratification,
          CASE
            WHEN (CAST(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class as INT64) = 1
             OR CAST(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class as INT64) = 2)
             AND (upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'NAS'
             AND upper(rtrim(cmp_name)) = 'HCA'
             OR upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'HOU'
             OR upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'ATL'
             OR upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'DAL'
             OR upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'SAN'
             OR upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'TAM'
             OR upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'ORA'
             AND upper(rtrim(cmp_name)) = 'HCA'
             OR upper(rtrim(left(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_current_name), 3))) = 'RIC'
             AND upper(rtrim(cmp_name)) = 'HCA') THEN 'MSC'
            ELSE 'SSC'
          END AS ssc_msc_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.customer_short_name,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class = 99 THEN format('%4d', 99)
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class_alias IS NULL THEN 'Unknown'
            ELSE a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class_alias
          END AS payor_fc_alias,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.otd_amt_net,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.writeoff_amt_mtd,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.cash_adj_amt_mtd,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_status_change_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.activity_due_date_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.latest_seq_creation_date_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_deadline_days_remaining,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.source_system_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.seq_no,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.expected_amt,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.new_appeal_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.dollarstratfpint,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.dollarstratfp,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appealsentstratification,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appealsentflag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.followupdispflag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.denial_orig_age_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.denial_orig_age_stra,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.followup_strat_int,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.followup_strat,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.current_appeal_balance,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.unworked_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.web_disposition_type,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_initiation_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.discharge_age_to_denial,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.discharge_age_to_denial_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.discharge_age_to_denial_strat,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_age_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_age_strat,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.first_denial_age_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.first_denial_age_strat,
          upymt_to_denial_age AS upymt_to_denial_age,
          CASE
            WHEN upymt_to_denial_age <= 30 THEN 1
            WHEN upymt_to_denial_age BETWEEN 31 AND 60 THEN 2
            WHEN upymt_to_denial_age BETWEEN 61 AND 90 THEN 3
            WHEN upymt_to_denial_age BETWEEN 91 AND 120 THEN 4
            WHEN upymt_to_denial_age BETWEEN 121 AND 150 THEN 5
            WHEN upymt_to_denial_age BETWEEN 151 AND 180 THEN 6
            WHEN upymt_to_denial_age BETWEEN 181 AND 210 THEN 7
            WHEN upymt_to_denial_age BETWEEN 211 AND 240 THEN 8
            WHEN upymt_to_denial_age BETWEEN 241 AND 270 THEN 9
            WHEN upymt_to_denial_age BETWEEN 271 AND 300 THEN 10
            WHEN upymt_to_denial_age BETWEEN 301 AND 330 THEN 11
            WHEN upymt_to_denial_age BETWEEN 331 AND 360 THEN 12
            WHEN upymt_to_denial_age BETWEEN 361 AND 720 THEN 13
            WHEN upymt_to_denial_age > 720 THEN 14
            ELSE 15
          END AS upymt_to_denial_num,
          CASE
            WHEN upymt_to_denial_age <= 30 THEN '<= 30'
            WHEN upymt_to_denial_age BETWEEN 31 AND 60 THEN '31-60'
            WHEN upymt_to_denial_age BETWEEN 61 AND 90 THEN '61-90'
            WHEN upymt_to_denial_age BETWEEN 91 AND 120 THEN '91-120'
            WHEN upymt_to_denial_age BETWEEN 121 AND 150 THEN '121-150'
            WHEN upymt_to_denial_age BETWEEN 151 AND 180 THEN '151-180'
            WHEN upymt_to_denial_age BETWEEN 181 AND 210 THEN '181-210'
            WHEN upymt_to_denial_age BETWEEN 211 AND 240 THEN '211-240'
            WHEN upymt_to_denial_age BETWEEN 241 AND 270 THEN '241-270'
            WHEN upymt_to_denial_age BETWEEN 271 AND 300 THEN '271-300'
            WHEN upymt_to_denial_age BETWEEN 301 AND 330 THEN '301-330'
            WHEN upymt_to_denial_age BETWEEN 331 AND 360 THEN '331-360'
            WHEN upymt_to_denial_age BETWEEN 361 AND 720 THEN '361-720'
            WHEN upymt_to_denial_age > 720 THEN '>720'
            ELSE 'Null Age'
          END AS upymt_to_denial_strat,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.account_no_1 IS NULL THEN 'Den Only'
            ELSE 'Den & Discr'
          END AS concurrentdendiscrflag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.discharge_age,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.min_date_of_denial = DATE '1900-01-01' THEN 'Denial in Midas (No DNL Date)'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.min_date_of_denial > a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.discharge_date THEN 'Denial in Midas (Post Disch)'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.min_date_of_denial <= a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.discharge_date THEN 'Concurrent Denial (Pre Disch)'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.min_date_of_denial IS NULL THEN 'Not in Midas'
            ELSE 'Other'
          END AS denial_in_midas,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pat_acct_num_1 IS NOT NULL THEN 'PDU Denial'
            ELSE 'Other'
          END AS pdu_flag,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id_1 IS NULL THEN 'N'
            ELSE 'Y'
          END AS all_days_approved,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.latest_seq_creation_date_age_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.latest_seq_creation_date_age_strat,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_reason_aging_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_reason_aging_strat,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id_1 IS NULL THEN 'Full Denial'
            ELSE 'Partial Denial'
          END AS partial_denial_ind,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id_1 IS NULL THEN 'N'
            ELSE 'Y'
          END AS xf_xg_condition_code,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 0 AND 1 THEN 1
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 2 THEN 2
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 3 THEN 3
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 4 THEN 4
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 5 THEN 5
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 6 AND 10 THEN 6
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 11 AND 20 THEN 7
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 21 AND 90 THEN 8
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 61 AND 365 THEN 9
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los > 365 THEN 10
            ELSE 11
          END AS los_stratificationid,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 0 AND 1 THEN '0-1 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 2 THEN '2 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 3 THEN '3 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 4 THEN '4 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los = 5 THEN '5 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 6 AND 10 THEN '6-10 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 11 AND 20 THEN '11-20 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 21 AND 90 THEN '21-90 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los BETWEEN 61 AND 365 THEN '91-365 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.calculated_los > 365 THEN '>365 days'
            ELSE 'Other'
          END AS los_stratification,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days AS date_of_presentation_los,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 0 AND 1 THEN 1
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 2 THEN 2
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 3 THEN 3
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 4 THEN 4
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 5 THEN 5
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 6 AND 10 THEN 6
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 11 AND 20 THEN 7
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 21 AND 90 THEN 8
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 61 AND 365 THEN 9
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days > 365 THEN 10
            ELSE 11
          END AS date_of_pres_los_stratificationid,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 0 AND 1 THEN '0-1 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 2 THEN '2 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 3 THEN '3 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 4 THEN '4 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days = 5 THEN '5 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 6 AND 10 THEN '6-10 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 11 AND 20 THEN '11-20 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 21 AND 90 THEN '21-90 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days BETWEEN 61 AND 365 THEN '91-365 days'
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.two_mid_inhouse_days > 365 THEN '>365 days'
            ELSE 'Other'
          END AS date_of_pres_los_stratification,
          drg_code_int AS drg_code_int,
          CASE
            WHEN drg_code_int IS NULL THEN 'DRG Not Assigned'
            ELSE left(coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.ms_drg_child_alias_name, 'DRG Description Not Found'), 25)
          END AS drg_code_desc,
          CASE
            WHEN drg_code_int IS NULL THEN 'DRG Not Assigned'
            ELSE coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.ms_chois_prod_line_desc, 'DRG Description Not Found')
          END AS drg_prod_line,
          CASE
             rtrim(substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.ms_drg_med_surg_name, 4, 2))
            WHEN 'Me' THEN 'Medical'
            WHEN 'Su' THEN 'Surgical'
            ELSE 'Other'
          END AS medsurg,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id_1 IS NULL THEN 'N'
            ELSE 'Y'
          END AS claim_type_131_flag,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id_1 IS NULL THEN 'N'
            ELSE 'Y'
          END AS claim_type_137_flag,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num IS NULL THEN 'No Auth #'
            WHEN upper(trim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num)) = '' THEN 'No Auth #'
            WHEN strpos(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, '/') = 0 THEN 'No "/" in Auth #'
            WHEN upper(rtrim(substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, strpos(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, '/'), 2))) = '/I' THEN 'Inpatient (/I)'
            WHEN upper(rtrim(substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, strpos(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, '/'), 2))) = '/V' THEN 'Observation (/V)'
            WHEN upper(rtrim(substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, strpos(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, '/'), 2))) = '/S' THEN 'IP Skilled Nursing or Swing Bed (/S)'
            WHEN upper(rtrim(substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, strpos(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, '/'), 2))) = '/B' THEN 'IP Behavioral Health (/B)'
            WHEN upper(rtrim(substr(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, strpos(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num, '/'), 4))) = '/CPT' THEN 'Outpatient (/CPT)'
            ELSE 'Other'
          END AS pa_auth_type,
          CASE
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_ip_ind) = 'Y'
             AND rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_obs_ind) = 'N' THEN 'Inpatient Met'
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_ip_ind) = 'N'
             AND rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_obs_ind) = 'N' THEN 'Did not meet'
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_ip_ind) = 'N'
             AND rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_obs_ind) = 'Y' THEN 'Observation Met'
            WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_ip_ind) = 'Y'
             AND rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iq_adm_rev_type_obs_ind) = 'Y' THEN 'Met both IP and OBS'
            ELSE ' '
          END AS med_necessity_desc,
          coalesce(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pdu_determination_reason_desc, ' ') AS pdu_determinationreason,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.treatment_authorization_num AS pa_auth_num,
          concurden9.cm_p2p_not_overturned,
          concurden10.cm_p2p_not_performed,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.denial_code_category AS denial_code_grouping,
          CASE
            WHEN upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.denial_code_category) IN(
              'MEDICAL NECESSITY', 'NO AUTH/NOTIFICATION'
            )
             AND (upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disp_desc) NOT LIKE '%EXPAPP%'
             OR a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disp_desc IS NULL)
             AND (upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disp_desc) NOT LIKE '%IN WRITE OFF REVIEW%'
             OR a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disp_desc IS NULL)
             AND (a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.adjusted_current_appeal_balance >= 250
             AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.adjusted_current_appeal_balance < 1500)
             AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.denial_age > 7
             AND (a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.seq_no IS NULL
             OR a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.seq_no <= 1)
             AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class NOT IN(
              1, 2, 3, 6, 15
            ) THEN 'Expedited 1'
            WHEN upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.denial_code_category) IN(
              'MEDICAL NECESSITY', 'NO AUTH/NOTIFICATION'
            )
             AND (a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.adjusted_current_appeal_balance >= 1500
             AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.adjusted_current_appeal_balance < 5000)
             AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appeal_sent_activity_date IS NULL
             AND date_diff(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.extract_date, a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.last_reason_change_date, DAY) > 7
             AND upper(upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.status_desc)) LIKE '%VALIDATED_DEN%'
             AND (a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.seq_no IS NULL
             OR a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.seq_no <= 1)
             AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payor_financial_class NOT IN(
              1, 2, 3, 6, 15
            ) THEN 'Expedited 2'
            ELSE 'Other'
          END AS optact_expapp_flag,
          CASE
            WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_rank = 1
             AND upper(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.external_appeal_code) LIKE 'W%' THEN 'Y'
            ELSE 'N'
          END AS excrpt_primarywcode_flag,
          CASE
            WHEN ipch.patient_dw_id IS NULL THEN 'N'
            ELSE 'Y'
          END AS iplan_change_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disposition_code_modified_date,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.appsent_excrpt_disp_flag,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disposition_mod_age,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disposition_mod_aging_num,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.disposition_mod_aging_strat,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.dw_last_update_date AS updatetimemetric,
          CASE
            WHEN ipbtc.patient_dw_id IS NULL THEN 'N'
            ELSE 'Y'
          END AS claim_type_11x_flag,
          CASE
            WHEN opbtc.patient_dw_id IS NULL THEN 'N'
            ELSE 'Y'
          END AS claim_type_13x_flag,
          CASE
            WHEN mrbtc.bill_type_code LIKE '13%' THEN '13X'
            WHEN mrbtc.bill_type_code LIKE '11%' THEN '11X'
            ELSE 'Other'
          END AS claim_type_mostrecent_flag,
          CASE
            WHEN clip.initial_fc IN(
              15, 99
            )
             AND clip.claim_fc IN(
              3, 9
            ) THEN 'Y'
            ELSE 'N'
          END AS optact_retroelg_ipch_flag,
          CASE
            WHEN clip.initial_fc IN(
              15, 99
            )
             AND (clip.claim_fc NOT IN(
              3, 9, 15, 99
            )
             OR clip.claim_fc IS NULL) THEN 'Y'
            ELSE 'N'
          END AS optact_iplanunsinpat_ipch_flag,
          CASE
            WHEN clip.initial_fc = 1
             AND clip.claim_fc = 12 THEN 'Y'
            ELSE 'N'
          END AS optact_iplanelgincint_ipch_flag,
          CAST(clip.initial_fc as INT64) AS opttact_initial_fc,
          CAST(clip.claim_fc as INT64) AS opttact_claim_fc,
          a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.vendor_code
        FROM
          (
            SELECT
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.schema_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.mon_account_payer_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pass_type,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.account_no,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ssc_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.rate_schedule_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ssc_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.unit_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.rate_schedule_eff_begin_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.rate_schedule_eff_end_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dob,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.insurance_provider_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payer_group_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.billing_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.billing_contact_person,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.authorization_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payer_patient_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payer_rank,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_financial_class,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_financial_class,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.accounting_period,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.major_payer_grp,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.reason_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.billing_status,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_service_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_account_status,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.cc_patient_type,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_discharge_status,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_patient_type,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.cancel_bill_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admit_source,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admit_type,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_drg,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.remit_drg_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.attending_physician_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.attending_physician_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.service_date_begin,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.discharge_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.comparison_method,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.project_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.work_queue_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.status_category_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.status_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.status_phase_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.calc_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_charges,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_actual_los,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_billed_charges,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.covered_charges,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_expected_payment,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_expected_adjustment,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_pt_responsibility_actual,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_variance_adjustment,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_payments,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_denial_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_due_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_total_account_bal,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ar_amount,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.otd_amt_net,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.writeoff_amt_net,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.cash_adj_amt_net,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.otd_to_date_amount_mtd,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.writeoff_amt_mtd,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.cash_adj_amt_mtd,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.max_aplno,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.max_seqno,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_orig_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.current_appealed_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.current_appeal_balance,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_date_created,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sequence_date_created,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.close_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.max_seq_deadline_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sequence_creator,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_owner,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_modifier,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.disp_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.disp_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.web_disp_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.web_disposition_type,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.root_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.root_cause_description,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.root_cause_dtl,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_appeal_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.apl_appeal_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.apl_appeal_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.first_denial_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.denial_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pa_denial_update_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.first_activity_create_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_activity_completion_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_activity_completion_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_user_activity_create_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_reason_change_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_status_change_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_project_change_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_owner_change_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.activity_due_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.activity_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.activity_subject,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.activity_owner,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_activity_ownr,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_initiation_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_activity_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_activity_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_status_change_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.activity_due_date_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.latest_seq_creation_date_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_deadline_days_remaining,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_activity_subj,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_activity_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.extract_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payer_category,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.source_system_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.seq_no,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dw_last_update_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.row_count,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.expected_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.new_appeal_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.disposition_code_modified_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.disposition_code_modified_by,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.vendor_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sub_unit_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.latest_iplan_change_date_pa,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.artiva_activity_due_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appsent_excrpt_disp_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.status_kpi,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.followup_kpi,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appealsent_kpi,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.deadline_kpi,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appealsent_deadline_kpi,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.post_sent_kpi,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.status_kpi_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.followup_kpi_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appealsent_kpi_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.deadline_kpi_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appealsent_deadline_kpi_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.post_sent_kpi_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.statusstratification,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.followup_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.followup_strat_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appealsentstratification,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.deadlinestratification,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.deadlinestratificationint,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.post_sent_category,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.post_sent_category_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.kpi_category,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.kpi_category_int,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appealsentflag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.followupdispflag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.denial_orig_age_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.denial_orig_age_stra,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.adjusted_current_appeal_balance,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.negative_curr_app_bal_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dollarstratint,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dollarstrat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dollarstratfp,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dollarstratfpint,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.unworked_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.open_close_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.project_type,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_due_amt_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.discharge_age_to_denial,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.discharge_age_to_denial_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.discharge_age_to_denial_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_activity_age_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_activity_age_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_age_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_age_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.first_denial_age_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.first_denial_age_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.discharge_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.latest_seq_creation_date_age_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.latest_seq_creation_date_age_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_reason_aging_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_reason_aging_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.disposition_mod_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.disposition_mod_aging_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.disposition_mod_aging_strat,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.artiva_activity_due_date_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_level,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.prior_appeal_response_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_sent_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.company_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.unit_num_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sub_unit_num_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admission_patient_type_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pat_acct_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_type_code_pos1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admission_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.discharge_date_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.final_bill_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.financial_class_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.account_status_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admission_source_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admission_type_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.drg_code_payor,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.drg_code_hcfa,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.drg_hcfa_icd10_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.drg_desc_hcfa,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.drg_payment_weight_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.drg_code_classic,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.rdrg_code_pos4,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.social_security_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_person_dw_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_empi_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_discharge_month_age,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_address_dw_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_zip_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.resp_party_zip_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.employer_dw_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_physician_num_attend,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_physician_num_admit,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_physician_num_pcp,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_dw_id_ins1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_ins1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ins_contract_id_ins1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.major_payor_id_ins1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.masterfacts_schema_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ins_product_code_ins1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ins_product_name_ins1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sub_payor_group_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_dw_id_ins2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_ins2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_dw_id_ins3,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_ins3,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.calculated_los,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_account_balance_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_billed_charges_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_anc_charges,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_rb_charges,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_payments_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_adjustment_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_contract_allow_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_write_off_bad_debt_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_write_off_other_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_write_off_other_adj_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_uninsured_discount_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_write_off_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_charity_write_off_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_charity_write_off_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_patient_liability_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_patient_payment_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_patient_payment_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.initial_statement_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_statement_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.exempt_indicator,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.casemix_exempt_indicator,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.expected_pmt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.primary_icd_version_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.diag_code_admit,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.diag_code_final,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_01,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_02,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_03,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_04,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_05,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_06,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_07,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_08,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_09,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_10,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_11,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_12,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_13,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_14,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.proc_code_15,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_diag_admit_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_diag_final_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_01,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_02,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_03,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_04,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_05,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_06,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_07,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_08,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_09,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_10,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_11,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_12,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_13,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_14,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icd10_proc_code_15,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dss_op_cpt_hier,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.aps_op_cpt_reimb_hier_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_midnights_in_house_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.stop_loss_lesser_of_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.source_system_code_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dw_last_update_date_time,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.unit_num_1 AS unit_num_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.company_code_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_1 AS coid_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.c_level,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.corporate_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.s_level,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sector_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.b_level,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.group_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.r_level,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.division_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.d_level,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.market_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.f_level,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.cons_facility_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.lob_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.lob_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sub_lob_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sub_lob_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.state_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pas_id_current,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pas_current_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pas_id_future,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pas_future_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_7_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_8_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_phys_svc_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_asd_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_imaging_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_oncology_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_cath_lab_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_intl_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summary_other_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pas_coid,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pas_status,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.company_code_operations,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.osg_pas_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.abs_facility_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.abl_facility_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.intl_pmis_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.hsc_member_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.cons_pas_coid,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.cons_pas_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.company_code_1 AS company_code_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_1 AS coid_1_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.customer_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.customer_short_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.customer_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ssc_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.unit_num_1 AS unit_num_1_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_mnemonic,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.group_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.division_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.market_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.f_level_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.partnership_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.go_live_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.eff_from_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.eff_to_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ssc_alias_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.division_alias_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ssc_name_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ssc_alias_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.consolidated_ssc_alias_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.corporate_name_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.group_name_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.market_name_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.division_name_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.group_alias_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.market_alias_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.division_alias_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ssc_coid,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_name_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.consolidated_ssc_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_name_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_state_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.medicare_expansion_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.medicaid_conversion_vendor_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.facility_close_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.his_vendor_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.rcps_migration_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.discrepancy_threshold_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sma_high_dollar_threshold_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.hsc_member_ind_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.clear_contract_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.client_outbound_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.him_conversion_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.summ_days_release_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_financial_class_alias,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_financial_class_gen02,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_financial_class_gen03,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_financial_class_gen04,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_financial_class_member,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_financial_class_sid,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_fin_class_gen02_alias,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_fin_class_gen04_alias,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_fin_class_gen03_alias,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pat_acct_num_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_1 AS coid_1_2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_order_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.maxupymtdate,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.extract_date_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.account_no_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_1 AS coid_1_3,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_1 AS iplan_id_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payer_rank_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.maxdiscrdate,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.min_date_of_denial,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pat_acct_num_1 AS pat_acct_num_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.unit_num_1 AS unit_num_1_2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_ins1_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pilot_acct_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_accountid,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_discharge_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_work_group_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_determination_reason_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_initial_auth_status_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_clinical_submitted_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_interqual_completed_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_1 AS iplan_id_1_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payment_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.denial_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_code_alias_namea,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.appeal_code_alias_nameb,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.denial_code_category,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_3,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.two_mid_inhouse_days,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_drg_sid,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.aso_bso_storage_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_drg_name_parent,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_drg_name_child,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_drg_child_alias_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.alias_table_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.sort_key_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.consolidation_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.storage_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.two_pass_calc_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.formula_text,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_drg_med_surg_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_drg_med_surg_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_chois_prod_line_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_chois_prod_line_desc,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.member_solve_order_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ms_drg_hier_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.bill_type_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_4,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_1 AS iplan_id_1_2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_insurance_order_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.bill_type_code_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_5,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_1 AS iplan_id_1_3,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_insurance_order_num_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_6,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_dw_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_insurance_order_num_1 AS iplan_insurance_order_num_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.payor_mail_to_name,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_1 AS coid_1_4,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.company_code_1 AS company_code_1_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iplan_id_1 AS iplan_id_1_4,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.eff_from_date_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.eff_to_date_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pat_acct_num_1 AS pat_acct_num_1_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.person_role_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pat_relationship_to_ins_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.policy_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.group_name_1 AS group_name_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.group_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.hic_claim_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.signed_pat_rel_on_file_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.signed_assn_benf_on_file_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.treatment_authorization_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.verification_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.precertification_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.recertification_day_count,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.edit_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dependent_maximum_age_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.student_max_age_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.deductible_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coinsurance_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.source_system_code_1 AS source_system_code_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dw_load_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dw_change_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.health_plan_patient_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.health_plan_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.auto_post_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.gross_reimbursement_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.gross_reimbursement_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.expected_contractual_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.expected_contractual_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coins_amt_source_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.calc_coins_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.calc_coins_amt_source_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.calc_coins_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.gross_reimbursement_var_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.gross_reimbursement_var_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.partb_professional_fee_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.blood_deductible_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.outpatient_pps_flag_tricare,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.outpatient_pps_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.irf_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.snf_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.psych_flag,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.log_format_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.non_covered_charge_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.copay_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.auto_post_amt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.major_payor_group_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.ub04_pat_relation_to_ins_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.medicare_inpatient_outlier_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.precertification_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icn,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.icn50,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.midas_encounter_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.eff_from_date_1 AS eff_from_date_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.patient_dw_id_1 AS patient_dw_id_1_7,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.company_code_1 AS company_code_1_2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.coid_1 AS coid_1_5,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.midas_facility_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.eff_to_date_1 AS eff_to_date_1_0,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.location_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admitting_service_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_initial_review_empl_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pat_acct_num_1 AS pat_acct_num_1_2,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.total_review_cnt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.completed_review_cnt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.midas_acct_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.initial_record_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.admit_weekend_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.pdu_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_type_ip_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_type_obs_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_type_ip_ptd_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_type_obs_ptd_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_type_ip_mn_met_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_type_obs_mn_met_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_initial_review_hour,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_initial_rev_date_time,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_criteria_status,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.iq_adm_rev_location_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.midas_encounter_last_updt_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_cm_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_apel_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_other_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_cm_cnt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_apel_cnt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_other_cnt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_disp_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_date_time,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.external_pa_referral_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.midas_last_ip_encounter_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.denial_onbase_unique_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.document_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.bpci_episode_group_id,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.bpci_data_science_percentage_num,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.bpci_data_science_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.concurrent_denial_code,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.inpatient_admit_review_cnt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.first_inpatient_admit_review_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.last_inpatient_admit_review_date,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.inpatient_admit_review_delay_day_cnt,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.inpatient_admit_review_1_day_delay_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.obs_prog_rev_after_disch_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.go_live_date_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.source_system_code_1 AS source_system_code_1_1,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.active_dw_ind,
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc.dw_last_update_date_time_1
              FROM
                a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc
            UNION ALL
            SELECT
                enc.schema_id,
                enc.mon_account_payer_id,
                enc.pass_type,
                enc.coid,
                enc.account_no,
                enc.ssc_id,
                enc.rate_schedule_name,
                enc.ssc_name,
                enc.facility_name,
                enc.unit_num,
                enc.rate_schedule_eff_begin_date,
                enc.rate_schedule_eff_end_date,
                enc.patient_name,
                enc.patient_dob,
                enc.iplan_id,
                enc.insurance_provider_name,
                enc.payer_group_name,
                enc.billing_name,
                enc.billing_contact_person,
                enc.authorization_code,
                enc.payer_patient_id,
                enc.payer_rank,
                enc.pa_financial_class,
                enc.payor_financial_class,
                enc.accounting_period,
                enc.major_payer_grp,
                enc.reason_code,
                enc.billing_status,
                enc.pa_service_code,
                enc.pa_account_status,
                enc.cc_patient_type,
                enc.pa_discharge_status,
                enc.pa_patient_type,
                enc.cancel_bill_ind,
                enc.admit_source,
                enc.admit_type,
                enc.pa_drg,
                enc.remit_drg_code,
                enc.attending_physician_id,
                enc.attending_physician_name,
                enc.service_date_begin,
                enc.discharge_date,
                enc.comparison_method,
                enc.project_name,
                enc.work_queue_name,
                enc.status_category_desc,
                enc.status_desc,
                enc.status_phase_desc,
                enc.calc_date,
                enc.total_charges,
                enc.pa_actual_los,
                enc.total_billed_charges,
                enc.covered_charges,
                enc.total_expected_payment,
                enc.total_expected_adjustment,
                enc.total_pt_responsibility_actual,
                enc.total_variance_adjustment,
                enc.total_payments,
                enc.total_denial_amt,
                enc.payor_due_amt,
                enc.pa_total_account_bal,
                enc.ar_amount,
                enc.otd_amt_net,
                enc.writeoff_amt_net,
                enc.cash_adj_amt_net,
                enc.otd_to_date_amount_mtd,
                enc.writeoff_amt_mtd,
                enc.cash_adj_amt_mtd,
                enc.max_aplno,
                enc.max_seqno,
                enc.appeal_orig_amt,
                enc.current_appealed_amt,
                enc.current_appeal_balance,
                enc.appeal_date_created,
                enc.sequence_date_created,
                enc.close_date,
                enc.max_seq_deadline_date,
                enc.sequence_creator,
                enc.appeal_owner,
                enc.appeal_modifier,
                enc.disp_code,
                enc.disp_desc,
                enc.web_disp_code,
                enc.web_disposition_type,
                enc.root_code,
                enc.root_cause_description,
                enc.root_cause_dtl,
                enc.external_appeal_code,
                enc.apl_appeal_code,
                enc.apl_appeal_desc,
                enc.first_denial_date,
                enc.denial_age,
                enc.pa_denial_update_date,
                enc.first_activity_create_date,
                enc.last_activity_completion_date,
                enc.last_activity_completion_age,
                enc.last_user_activity_create_age,
                enc.last_reason_change_date,
                enc.last_status_change_date,
                enc.last_project_change_date,
                enc.last_owner_change_date,
                enc.activity_due_date,
                enc.activity_desc,
                enc.activity_subject,
                enc.activity_owner,
                enc.appeal_sent_activity_ownr,
                enc.appeal_initiation_date,
                enc.appeal_sent_activity_date,
                enc.appeal_sent_activity_age,
                enc.last_status_change_age,
                enc.activity_due_date_age,
                enc.latest_seq_creation_date_age,
                enc.appeal_deadline_days_remaining,
                enc.appeal_sent_activity_subj,
                enc.appeal_sent_activity_desc,
                enc.extract_date,
                enc.payer_category,
                enc.source_system_code,
                enc.seq_no,
                enc.dw_last_update_date,
                enc.row_count,
                enc.expected_amt,
                enc.new_appeal_flag,
                enc.disposition_code_modified_date,
                enc.disposition_code_modified_by,
                enc.vendor_code,
                enc.sub_unit_num,
                enc.latest_iplan_change_date_pa,
                enc.artiva_activity_due_date,
                enc.appsent_excrpt_disp_flag,
                enc.status_kpi,
                enc.followup_kpi,
                enc.appealsent_kpi,
                enc.deadline_kpi,
                enc.appealsent_deadline_kpi,
                enc.post_sent_kpi,
                enc.status_kpi_int,
                enc.followup_kpi_int,
                enc.appealsent_kpi_int,
                enc.deadline_kpi_int,
                enc.appealsent_deadline_kpi_int,
                enc.post_sent_kpi_int,
                enc.statusstratification,
                enc.followup_strat,
                enc.followup_strat_int,
                enc.appealsentstratification,
                enc.deadlinestratification,
                enc.deadlinestratificationint,
                enc.post_sent_category,
                enc.post_sent_category_int,
                enc.kpi_category,
                enc.kpi_category_int,
                enc.appealsentflag,
                enc.followupdispflag,
                enc.denial_orig_age_num,
                enc.denial_orig_age_stra,
                enc.adjusted_current_appeal_balance,
                enc.negative_curr_app_bal_flag,
                enc.dollarstratint,
                enc.dollarstrat,
                enc.dollarstratfp,
                enc.dollarstratfpint,
                enc.unworked_flag,
                enc.open_close_flag,
                enc.project_type,
                enc.payor_due_amt_flag,
                enc.discharge_age_to_denial,
                enc.discharge_age_to_denial_num,
                enc.discharge_age_to_denial_strat,
                enc.appeal_sent_activity_age_num,
                enc.appeal_sent_activity_age_strat,
                enc.appeal_sent_age_num,
                enc.appeal_sent_age_strat,
                enc.first_denial_age_num,
                enc.first_denial_age_strat,
                enc.discharge_age,
                enc.latest_seq_creation_date_age_num,
                enc.latest_seq_creation_date_age_strat,
                enc.last_reason_aging_num,
                enc.last_reason_aging_strat,
                enc.disposition_mod_age,
                enc.disposition_mod_aging_num,
                enc.disposition_mod_aging_strat,
                enc.artiva_activity_due_date_age,
                enc.appeal_level,
                enc.appeal_sent_date,
                enc.prior_appeal_response_date,
                enc.appeal_sent_age,
                enc.patient_dw_id,
                enc.company_code,
                enc.coid AS coid_1,
                enc.unit_num AS unit_num_1,
                enc.sub_unit_num AS sub_unit_num_1,
                enc.admission_patient_type_code,
                enc.pat_acct_num,
                enc.patient_type_code_pos1,
                enc.admission_date,
                enc.discharge_date AS discharge_date_1,
                enc.final_bill_date,
                enc.financial_class_code,
                enc.account_status_code,
                enc.admission_source_code,
                enc.admission_type_code,
                enc.drg_code_payor,
                enc.drg_code_hcfa,
                enc.drg_hcfa_icd10_code,
                enc.drg_desc_hcfa,
                enc.drg_payment_weight_amt,
                enc.drg_code_classic,
                enc.rdrg_code_pos4,
                enc.social_security_num,
                enc.patient_person_dw_id,
                enc.patient_empi_num,
                enc.patient_discharge_month_age,
                enc.patient_address_dw_id,
                enc.patient_zip_code,
                enc.resp_party_zip_code,
                enc.employer_dw_id,
                enc.facility_physician_num_attend,
                enc.facility_physician_num_admit,
                enc.facility_physician_num_pcp,
                enc.payor_dw_id_ins1,
                enc.iplan_id_ins1,
                enc.ins_contract_id_ins1,
                enc.major_payor_id_ins1,
                enc.masterfacts_schema_id,
                enc.ins_product_code_ins1,
                enc.ins_product_name_ins1,
                enc.sub_payor_group_code,
                enc.payor_dw_id_ins2,
                enc.iplan_id_ins2,
                enc.payor_dw_id_ins3,
                enc.iplan_id_ins3,
                enc.calculated_los,
                enc.total_account_balance_amt,
                enc.total_billed_charges AS total_billed_charges_1,
                enc.total_anc_charges,
                enc.total_rb_charges,
                enc.total_payments AS total_payments_1,
                enc.total_adjustment_amt,
                enc.total_contract_allow_amt,
                enc.total_write_off_bad_debt_amt,
                enc.total_write_off_other_amt,
                enc.total_write_off_other_adj_amt,
                enc.total_uninsured_discount_amt,
                enc.last_write_off_date,
                enc.total_charity_write_off_amt,
                enc.last_charity_write_off_date,
                enc.total_patient_liability_amt,
                enc.total_patient_payment_amt,
                enc.last_patient_payment_date,
                enc.initial_statement_date,
                enc.last_statement_date,
                enc.exempt_indicator,
                enc.casemix_exempt_indicator,
                enc.expected_pmt,
                enc.primary_icd_version_code,
                enc.diag_code_admit,
                enc.diag_code_final,
                enc.proc_code_01,
                enc.proc_code_02,
                enc.proc_code_03,
                enc.proc_code_04,
                enc.proc_code_05,
                enc.proc_code_06,
                enc.proc_code_07,
                enc.proc_code_08,
                enc.proc_code_09,
                enc.proc_code_10,
                enc.proc_code_11,
                enc.proc_code_12,
                enc.proc_code_13,
                enc.proc_code_14,
                enc.proc_code_15,
                enc.icd10_diag_admit_code,
                enc.icd10_diag_final_code,
                enc.icd10_proc_code_01,
                enc.icd10_proc_code_02,
                enc.icd10_proc_code_03,
                enc.icd10_proc_code_04,
                enc.icd10_proc_code_05,
                enc.icd10_proc_code_06,
                enc.icd10_proc_code_07,
                enc.icd10_proc_code_08,
                enc.icd10_proc_code_09,
                enc.icd10_proc_code_10,
                enc.icd10_proc_code_11,
                enc.icd10_proc_code_12,
                enc.icd10_proc_code_13,
                enc.icd10_proc_code_14,
                enc.icd10_proc_code_15,
                enc.dss_op_cpt_hier,
                enc.aps_op_cpt_reimb_hier_id,
                enc.total_midnights_in_house_amt,
                enc.stop_loss_lesser_of_code,
                enc.source_system_code AS source_system_code_1,
                enc.dw_last_update_date_time,
                enc.unit_num AS unit_num_1_0,
                enc.company_code AS company_code_1,
                enc.coid AS coid_1_0,
                enc.coid_name,
                enc.c_level,
                enc.corporate_name,
                enc.s_level,
                enc.sector_name,
                enc.b_level,
                enc.group_name,
                enc.r_level,
                enc.division_name,
                enc.d_level,
                enc.market_name,
                enc.f_level,
                enc.cons_facility_name,
                enc.lob_code,
                enc.lob_name,
                enc.sub_lob_code,
                enc.sub_lob_name,
                enc.state_code,
                enc.pas_id_current,
                enc.pas_current_name,
                enc.pas_id_future,
                enc.pas_future_name,
                enc.summary_7_member_ind,
                enc.summary_8_member_ind,
                enc.summary_phys_svc_member_ind,
                enc.summary_asd_member_ind,
                enc.summary_imaging_member_ind,
                enc.summary_oncology_member_ind,
                enc.summary_cath_lab_member_ind,
                enc.summary_intl_member_ind,
                enc.summary_other_member_ind,
                enc.pas_coid,
                enc.pas_status,
                enc.company_code_operations,
                enc.osg_pas_ind,
                enc.abs_facility_member_ind,
                enc.abl_facility_member_ind,
                enc.intl_pmis_member_ind,
                enc.hsc_member_ind,
                enc.cons_pas_coid,
                enc.cons_pas_name,
                enc.company_code AS company_code_1_0,
                enc.coid AS coid_1_1,
                enc.customer_code,
                enc.customer_short_name,
                enc.customer_name,
                enc.ssc_code,
                enc.unit_num AS unit_num_1_1,
                enc.facility_mnemonic,
                enc.group_code,
                enc.division_code,
                enc.market_code,
                enc.f_level AS f_level_1,
                enc.partnership_ind,
                enc.go_live_date,
                enc.eff_from_date,
                enc.eff_to_date,
                enc.ssc_alias_code,
                enc.division_alias_code,
                enc.ssc_name AS ssc_name_1,
                enc.ssc_alias_name,
                enc.consolidated_ssc_alias_name,
                enc.corporate_name AS corporate_name_1,
                enc.group_name AS group_name_1,
                enc.market_name AS market_name_1,
                enc.division_name AS division_name_1,
                enc.group_alias_name,
                enc.market_alias_name,
                enc.division_alias_name,
                enc.ssc_coid,
                enc.coid_name AS coid_name_1,
                enc.consolidated_ssc_num,
                enc.facility_name AS facility_name_1,
                enc.facility_state_code,
                enc.medicare_expansion_ind,
                enc.medicaid_conversion_vendor_name,
                enc.facility_close_date,
                enc.his_vendor_name,
                enc.rcps_migration_date,
                enc.discrepancy_threshold_amt,
                enc.sma_high_dollar_threshold_amt,
                enc.hsc_member_ind AS hsc_member_ind_1,
                enc.clear_contract_ind,
                enc.client_outbound_ind,
                enc.him_conversion_date,
                enc.summ_days_release_date,
                enc.payor_financial_class_alias,
                enc.payor_financial_class_gen02,
                enc.payor_financial_class_gen03,
                enc.payor_financial_class_gen04,
                enc.payor_financial_class_member,
                enc.payor_financial_class_sid,
                enc.payor_fin_class_gen02_alias,
                enc.payor_fin_class_gen04_alias,
                enc.payor_fin_class_gen03_alias,
                enc.pat_acct_num AS pat_acct_num_1,
                enc.coid AS coid_1_2,
                enc.iplan_id AS iplan_id_1,
                enc.iplan_order_num,
                enc.maxupymtdate,
                enc.extract_date AS extract_date_1,
                enc.account_no AS account_no_1,
                enc.coid AS coid_1_3,
                enc.iplan_id AS iplan_id_1_0,
                enc.payer_rank AS payer_rank_1,
                enc.maxdiscrdate,
                enc.patient_dw_id AS patient_dw_id_1,
                enc.min_date_of_denial,
                enc.pat_acct_num AS pat_acct_num_1_0,
                enc.unit_num AS unit_num_1_2,
                enc.iplan_ins1_id,
                enc.pilot_acct_ind,
                enc.pdu_accountid,
                enc.pdu_discharge_date,
                enc.pdu_work_group_name,
                enc.pdu_determination_reason_desc,
                enc.pdu_initial_auth_status_code,
                enc.pdu_clinical_submitted_date,
                enc.pdu_interqual_completed_ind,
                enc.patient_dw_id AS patient_dw_id_1_0,
                enc.patient_dw_id AS patient_dw_id_1_1,
                enc.iplan_id AS iplan_id_1_1,
                enc.payment_amt,
                enc.patient_dw_id AS patient_dw_id_1_2,
                enc.denial_code,
                enc.appeal_code_alias_namea,
                enc.appeal_code_alias_nameb,
                enc.denial_code_category,
                enc.patient_dw_id AS patient_dw_id_1_3,
                enc.two_mid_inhouse_days,
                enc.ms_drg_sid,
                enc.aso_bso_storage_code,
                enc.ms_drg_name_parent,
                enc.ms_drg_name_child,
                enc.ms_drg_child_alias_name,
                enc.alias_table_name,
                enc.sort_key_num,
                enc.consolidation_code,
                enc.storage_code,
                enc.two_pass_calc_code,
                enc.formula_text,
                enc.ms_drg_med_surg_code,
                enc.ms_drg_med_surg_name,
                enc.ms_chois_prod_line_code,
                enc.ms_chois_prod_line_desc,
                enc.member_solve_order_num,
                enc.ms_drg_hier_name,
                enc.bill_type_code,
                enc.patient_dw_id AS patient_dw_id_1_4,
                enc.iplan_id AS iplan_id_1_2,
                enc.iplan_insurance_order_num,
                enc.bill_type_code AS bill_type_code_1,
                enc.patient_dw_id AS patient_dw_id_1_5,
                enc.iplan_id AS iplan_id_1_3,
                enc.iplan_insurance_order_num AS iplan_insurance_order_num_1,
                enc.patient_dw_id AS patient_dw_id_1_6,
                enc.payor_dw_id,
                enc.iplan_insurance_order_num AS iplan_insurance_order_num_1_0,
                enc.payor_name,
                enc.payor_mail_to_name,
                enc.coid AS coid_1_4,
                enc.company_code AS company_code_1_1,
                enc.iplan_id AS iplan_id_1_4,
                enc.eff_from_date AS eff_from_date_1,
                enc.eff_to_date AS eff_to_date_1,
                enc.pat_acct_num AS pat_acct_num_1_1,
                enc.person_role_code,
                enc.pat_relationship_to_ins_code,
                enc.policy_num,
                enc.group_name AS group_name_1_0,
                enc.group_num,
                enc.hic_claim_num,
                enc.signed_pat_rel_on_file_ind,
                enc.signed_assn_benf_on_file_ind,
                enc.treatment_authorization_num,
                enc.verification_date,
                enc.precertification_date,
                enc.recertification_day_count,
                enc.edit_code,
                enc.dependent_maximum_age_num,
                enc.student_max_age_num,
                enc.deductible_amt,
                enc.coinsurance_amt,
                enc.source_system_code AS source_system_code_1_0,
                enc.dw_load_date,
                enc.dw_change_date,
                enc.health_plan_patient_id,
                enc.health_plan_id,
                enc.auto_post_ind,
                enc.gross_reimbursement_amt,
                enc.gross_reimbursement_date,
                enc.expected_contractual_amt,
                enc.expected_contractual_date,
                enc.coins_amt_source_ind,
                enc.calc_coins_amt,
                enc.calc_coins_amt_source_ind,
                enc.calc_coins_date,
                enc.gross_reimbursement_var_amt,
                enc.gross_reimbursement_var_date,
                enc.partb_professional_fee_amt,
                enc.blood_deductible_amt,
                enc.outpatient_pps_flag_tricare,
                enc.outpatient_pps_flag,
                enc.irf_flag,
                enc.snf_flag,
                enc.psych_flag,
                enc.log_format_ind,
                enc.non_covered_charge_amt,
                enc.copay_amt,
                enc.auto_post_amt,
                enc.major_payor_group_id,
                enc.ub04_pat_relation_to_ins_code,
                enc.medicare_inpatient_outlier_ind,
                enc.precertification_ind,
                enc.icn,
                enc.icn50,
                enc.null_0 AS midas_encounter_id,
                enc.null_1 AS eff_from_date_1_0,
                enc.null_2 AS patient_dw_id_1_7,
                enc.null_3 AS company_code_1_2,
                enc.null_4 AS coid_1_5,
                enc.null_5 AS midas_facility_code,
                enc.null_6 AS eff_to_date_1_0,
                enc.null_7 AS location_id,
                enc.null_8 AS admitting_service_id,
                enc.null_9 AS iq_adm_initial_review_empl_id,
                enc.null_10 AS pat_acct_num_1_2,
                enc.null_11 AS total_review_cnt,
                enc.null_12 AS completed_review_cnt,
                enc.null_13 AS midas_acct_num,
                enc.null_14 AS initial_record_ind,
                enc.null_15 AS admit_weekend_ind,
                enc.null_16 AS pdu_ind,
                enc.null_17 AS iq_adm_rev_type_ip_ind,
                enc.null_18 AS iq_adm_rev_type_obs_ind,
                enc.null_19 AS iq_adm_rev_type_ip_ptd_ind,
                enc.null_20 AS iq_adm_rev_type_obs_ptd_ind,
                enc.null_21 AS iq_adm_rev_type_ip_mn_met_ind,
                enc.null_22 AS iq_adm_rev_type_obs_mn_met_ind,
                enc.null_23 AS iq_adm_initial_review_hour,
                enc.null_24 AS iq_adm_initial_rev_date_time,
                enc.null_25 AS iq_adm_rev_criteria_status,
                enc.null_26 AS iq_adm_rev_location_id,
                enc.null_27 AS midas_encounter_last_updt_date,
                enc.null_28 AS external_pa_referral_cm_ind,
                enc.null_29 AS external_pa_referral_apel_ind,
                enc.null_30 AS external_pa_referral_other_ind,
                enc.null_31 AS external_pa_referral_cm_cnt,
                enc.null_32 AS external_pa_referral_apel_cnt,
                enc.null_33 AS external_pa_referral_other_cnt,
                enc.null_34 AS external_pa_referral_disp_id,
                enc.null_35 AS external_pa_referral_date_time,
                enc.null_36 AS external_pa_referral_id,
                enc.null_37 AS midas_last_ip_encounter_id,
                enc.null_38 AS denial_onbase_unique_id,
                enc.null_39 AS document_date,
                enc.null_40 AS bpci_episode_group_id,
                enc.null_41 AS bpci_data_science_percentage_num,
                enc.null_42 AS bpci_data_science_date,
                enc.null_43 AS concurrent_denial_code,
                enc.null_44 AS inpatient_admit_review_cnt,
                enc.null_45 AS first_inpatient_admit_review_date,
                enc.null_46 AS last_inpatient_admit_review_date,
                enc.null_47 AS inpatient_admit_review_delay_day_cnt,
                enc.null_48 AS inpatient_admit_review_1_day_delay_ind,
                enc.null_49 AS obs_prog_rev_after_disch_ind,
                enc.null_50 AS go_live_date_ind,
                enc.null_51 AS source_system_code_1_1,
                enc.null_52 AS active_dw_ind,
                enc.null_53 AS dw_last_update_date_time_1
              FROM
                enc
          ) AS a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc
          LEFT OUTER JOIN (
            SELECT
                cm_encounter.patient_dw_id AS cm_patient_dw_id,
                max(CASE
                  WHEN hcm_avoid_denied_appeal.hcm_appeal_status_id = 29
                   OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 31
                   OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 34
                   OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 38
                   OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 39 THEN 'Y'
                  ELSE 'N'
                END) AS cm_p2p_not_overturned
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwcm_views.ref_hcm_days_type AS hcm_avoid_denied_days_type
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_day_info ON hcm_avoid_denied_day_info.type_of_day_id = hcm_avoid_denied_days_type.hcm_days_type_id
                 AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
                 AND rtrim(hcm_avoid_denied_days_type.active_dw_ind) = 'Y'
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_day ON hcm_avoid_denied_day.hcm_avoid_denied_day_id = hcm_avoid_denied_day_info.hcm_avoid_denied_day_id
                 AND rtrim(hcm_avoid_denied_day.active_dw_ind) = 'Y'
                 AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_encounter ON hcm_avoid_denied_day.midas_encounter_id = cm_encounter.midas_encounter_id
                 AND rtrim(hcm_avoid_denied_day.active_dw_ind) = 'Y'
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.fact_patient ON fact_patient.patient_dw_id = cm_encounter.patient_dw_id
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.fact_facility ON rtrim(fact_facility.coid) = rtrim(fact_patient.coid)
                 AND rtrim(fact_facility.company_code) = rtrim(fact_patient.company_code)
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_facility ON rtrim(cm_facility.company_code) = rtrim(cm_encounter.company_code)
                 AND rtrim(cm_facility.coid) = rtrim(cm_encounter.coid)
                 AND rtrim(cm_facility.midas_facility_code) = rtrim(cm_encounter.midas_facility_code)
                 AND rtrim(cm_facility.active_dw_ind) = 'Y'
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.major_payor ON fact_patient.major_payor_id_ins1 = major_payor.major_payor_id
                 AND fact_patient.masterfacts_schema_id = major_payor.schema_id
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.denials_onbase ON denials_onbase.patient_dw_id = fact_patient.patient_dw_id
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.chois_product_line ON rtrim(CASE
                  WHEN rtrim(fact_patient.primary_icd_version_code) = '9'
                   AND fact_patient.drg_code_hcfa IS NOT NULL THEN fact_patient.drg_code_hcfa
                  WHEN rtrim(fact_patient.primary_icd_version_code) = '9'
                   AND fact_patient.drg_code_hcfa IS NULL THEN fact_patient.drg_hcfa_icd10_code
                  WHEN rtrim(fact_patient.primary_icd_version_code) = '0' THEN fact_patient.drg_hcfa_icd10_code
                END) = rtrim(chois_product_line.drg_code)
                 AND rtrim(chois_product_line.drg_code_group_ind) = 'M'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.clinical_ed_event_date_time ON clinical_ed_event_date_time.patient_dw_id = fact_patient.patient_dw_id
                LEFT OUTER JOIN (
                  SELECT
                      admission_patient_type.patient_dw_id,
                      admission_patient_type.eff_from_date,
                      admission_patient_type.admission_patient_type_code,
                      row_number() OVER (PARTITION BY admission_patient_type.patient_dw_id ORDER BY admission_patient_type.patient_dw_id, admission_patient_type.eff_from_date) AS rec_num
                    FROM
                      `{{ params.param_parallon_cur_project_id }}`.edwcm_views.admission_patient_type
                    WHERE admission_patient_type.admission_patient_type_code NOT IN(
                      'EP ', 'OP ', 'SP ', 'IP ', 'ER ', 'OR ', 'SR ', 'ERV', 'ORV', 'SRV', 'N  '
                    )
                ) AS init_adm_pat_type ON init_adm_pat_type.patient_dw_id = fact_patient.patient_dw_id
                 AND init_adm_pat_type.rec_num = 1
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.auth_base_views.cm_billing_condition_code ON cm_encounter.midas_encounter_id = cm_billing_code_comment.midas_encounter_id
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                 AND rtrim(cm_billing_code_comment.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_appeal ON hcm_avoid_denied_day_info.hcm_avoid_denied_day_info_id = hcm_avoid_denied_appeal.hcm_avoid_denied_day_info_id
                 AND hcm_avoid_denied_day_info.hcm_avoid_denied_day_id = hcm_avoid_denied_appeal.hcm_avoid_denied_day_id
                 AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
                 AND rtrim(hcm_avoid_denied_appeal.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.ref_hcm_appeal_status ON hcm_avoid_denied_appeal.hcm_appeal_status_id = ref_hcm_appeal_status.hcm_appeal_status_id
                 AND rtrim(hcm_avoid_denied_appeal.active_dw_ind) = 'Y'
                 AND rtrim(ref_hcm_appeal_status.active_dw_ind) = 'Y'
              WHERE rtrim(fact_facility.company_code) = 'H'
               AND rtrim(fact_facility.coid_status_code) = 'F'
               AND fact_patient.admission_patient_type_code IN(
                'I', 'IB'
              )
               AND upper(hcm_avoid_denied_days_type.hcm_days_type_name) LIKE '%DEN %'
               AND fact_patient.discharge_date BETWEEN date_add(date_add(current_date('US/Central'), interval 1 DAY), interval -25 MONTH) AND current_date('US/Central')
               AND trim(cm_facility.midas_facility_code) IS NOT NULL
               AND upper(rtrim(CASE
                WHEN hcm_avoid_denied_appeal.hcm_appeal_status_id = 29
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 31
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 34
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 38
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 39 THEN 'Y'
                ELSE 'N'
              END)) = 'Y'
              GROUP BY 1, upper(CASE
                WHEN hcm_avoid_denied_appeal.hcm_appeal_status_id = 29
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 31
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 34
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 38
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 39 THEN 'Y'
                ELSE 'N'
              END)
          ) AS concurden9 ON concurden9.cm_patient_dw_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id
           AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_rank = 1
          LEFT OUTER JOIN (
            SELECT
                cm_encounter.patient_dw_id AS cm_patient_dw_id,
                max(CASE
                  WHEN hcm_avoid_denied_appeal.hcm_appeal_status_id = 30
                   OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 36
                   OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 37 THEN 'Y'
                  ELSE 'N'
                END) AS cm_p2p_not_performed
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwcm_views.ref_hcm_days_type AS hcm_avoid_denied_days_type
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_day_info ON hcm_avoid_denied_day_info.type_of_day_id = hcm_avoid_denied_days_type.hcm_days_type_id
                 AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
                 AND rtrim(hcm_avoid_denied_days_type.active_dw_ind) = 'Y'
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_day ON hcm_avoid_denied_day.hcm_avoid_denied_day_id = hcm_avoid_denied_day_info.hcm_avoid_denied_day_id
                 AND rtrim(hcm_avoid_denied_day.active_dw_ind) = 'Y'
                 AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_encounter ON hcm_avoid_denied_day.midas_encounter_id = cm_encounter.midas_encounter_id
                 AND rtrim(hcm_avoid_denied_day.active_dw_ind) = 'Y'
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                RIGHT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.fact_patient ON fact_patient.patient_dw_id = cm_encounter.patient_dw_id
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.fact_facility ON rtrim(fact_facility.coid) = rtrim(fact_patient.coid)
                 AND rtrim(fact_facility.company_code) = rtrim(fact_patient.company_code)
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_facility ON rtrim(cm_facility.company_code) = rtrim(cm_encounter.company_code)
                 AND rtrim(cm_facility.coid) = rtrim(cm_encounter.coid)
                 AND rtrim(cm_facility.midas_facility_code) = rtrim(cm_encounter.midas_facility_code)
                 AND rtrim(cm_facility.active_dw_ind) = 'Y'
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.major_payor ON fact_patient.major_payor_id_ins1 = major_payor.major_payor_id
                 AND fact_patient.masterfacts_schema_id = major_payor.schema_id
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.denials_onbase ON denials_onbase.patient_dw_id = fact_patient.patient_dw_id
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.chois_product_line ON rtrim(CASE
                  WHEN rtrim(fact_patient.primary_icd_version_code) = '9'
                   AND fact_patient.drg_code_hcfa IS NOT NULL THEN fact_patient.drg_code_hcfa
                  WHEN rtrim(fact_patient.primary_icd_version_code) = '9'
                   AND fact_patient.drg_code_hcfa IS NULL THEN fact_patient.drg_hcfa_icd10_code
                  WHEN rtrim(fact_patient.primary_icd_version_code) = '0' THEN fact_patient.drg_hcfa_icd10_code
                END) = rtrim(chois_product_line.drg_code)
                 AND rtrim(chois_product_line.drg_code_group_ind) = 'M'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.clinical_ed_event_date_time ON clinical_ed_event_date_time.patient_dw_id = fact_patient.patient_dw_id
                LEFT OUTER JOIN (
                  SELECT
                      admission_patient_type.patient_dw_id,
                      admission_patient_type.eff_from_date,
                      admission_patient_type.admission_patient_type_code,
                      row_number() OVER (PARTITION BY admission_patient_type.patient_dw_id ORDER BY admission_patient_type.patient_dw_id, admission_patient_type.eff_from_date) AS rec_num
                    FROM
                      `{{ params.param_parallon_cur_project_id }}`.edwcm_views.admission_patient_type
                    WHERE admission_patient_type.admission_patient_type_code NOT IN(
                      'EP ', 'OP ', 'SP ', 'IP ', 'ER ', 'OR ', 'SR ', 'ERV', 'ORV', 'SRV', 'N  '
                    )
                ) AS init_adm_pat_type ON init_adm_pat_type.patient_dw_id = fact_patient.patient_dw_id
                 AND init_adm_pat_type.rec_num = 1
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.cm_billing_code_comment ON cm_encounter.midas_encounter_id = cm_billing_code_comment.midas_encounter_id
                 AND rtrim(cm_encounter.active_dw_ind) = 'Y'
                 AND rtrim(cm_billing_code_comment.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.hcm_avoid_denied_appeal ON hcm_avoid_denied_day_info.hcm_avoid_denied_day_info_id = hcm_avoid_denied_appeal.hcm_avoid_denied_day_info_id
                 AND hcm_avoid_denied_day_info.hcm_avoid_denied_day_id = hcm_avoid_denied_appeal.hcm_avoid_denied_day_id
                 AND rtrim(hcm_avoid_denied_day_info.active_dw_ind) = 'Y'
                 AND rtrim(hcm_avoid_denied_appeal.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwcm_views.ref_hcm_appeal_status ON hcm_avoid_denied_appeal.hcm_appeal_status_id = ref_hcm_appeal_status.hcm_appeal_status_id
                 AND rtrim(hcm_avoid_denied_appeal.active_dw_ind) = 'Y'
                 AND rtrim(ref_hcm_appeal_status.active_dw_ind) = 'Y'
              WHERE rtrim(fact_facility.company_code) = 'H'
               AND rtrim(fact_facility.coid_status_code) = 'F'
               AND fact_patient.admission_patient_type_code IN(
                'I', 'IB'
              )
               AND upper(hcm_avoid_denied_days_type.hcm_days_type_name) LIKE '%DEN %'
               AND fact_patient.discharge_date BETWEEN date_add(date_add(current_date('US/Central'), interval 1 DAY), interval -25 MONTH) AND current_date('US/Central')
               AND trim(cm_facility.midas_facility_code) IS NOT NULL
               AND upper(rtrim(CASE
                WHEN hcm_avoid_denied_appeal.hcm_appeal_status_id = 30
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 36
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 37 THEN 'Y'
                ELSE 'N'
              END)) = 'Y'
              GROUP BY 1, upper(CASE
                WHEN hcm_avoid_denied_appeal.hcm_appeal_status_id = 30
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 36
                 OR hcm_avoid_denied_appeal.hcm_appeal_status_id = 37 THEN 'Y'
                ELSE 'N'
              END)
          ) AS concurden10 ON concurden10.cm_patient_dw_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id
           AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_rank = 1
          LEFT OUTER JOIN (
            SELECT
                fp.company_code,
                fp.patient_dw_id,
                fp.unit_num,
                fp.coid,
                fp.pat_acct_num,
                fp.iplan_id_ins1,
                eom.iplan_id_ins1 AS eom_iplan,
                fp.major_payor_id_ins1,
                eom.major_payor_group_id AS eom_mpg
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwpf_views.fact_patient AS fp
                INNER JOIN (
                  SELECT
                      eom_0.patient_dw_id,
                      eom_0.iplan_id_ins1,
                      fi.major_payor_group_id
                    FROM
                      `{{ params.param_parallon_cur_project_id }}`.edwpf_views.bobj_pass_eom AS eom_0
                      LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpf_views.facility_iplan AS fi ON rtrim(eom_0.coid) = rtrim(fi.coid)
                       AND eom_0.iplan_id_ins1 = fi.iplan_id
                       AND fi.major_payor_group_id IS NOT NULL
                       AND trim(fi.major_payor_group_id) <> ''
                    WHERE (eom_0.patient_dw_id, eom_0.rptg_period) IN(
                      SELECT AS STRUCT
                          bobj_pass_eom.patient_dw_id,
                          min(bobj_pass_eom.rptg_period) AS min_rptg_period
                        FROM
                          `{{ params.param_parallon_cur_project_id }}`.edwpf_views.bobj_pass_eom
                        WHERE bobj_pass_eom.final_bill_date IS NOT NULL
                        GROUP BY 1
                    )
                     AND (CAST(eom_0.pat_acct_num as FLOAT64), eom_0.coid) IN(
                      SELECT DISTINCT AS STRUCT
                          CAST(bqutil.fn.cw_td_normalize_number(edw_daily_denial_inventory.account_no) as FLOAT64) AS account_no,
                          edw_daily_denial_inventory.coid
                        FROM
                          {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory
                    )
                ) AS eom ON fp.patient_dw_id = eom.patient_dw_id
                 AND fp.iplan_id_ins1 <> eom.iplan_id_ins1
          ) AS ipch ON ipch.patient_dw_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id
          LEFT OUTER JOIN (
            SELECT
                rh_837_claim.patient_dw_id,
                rh_837_claim.iplan_id,
                rh_837_claim.iplan_insurance_order_num
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwpf_views.rh_837_claim
              WHERE rh_837_claim.bill_type_code LIKE '11%'
              GROUP BY 1, 2, 3
          ) AS ipbtc ON ipbtc.patient_dw_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id
           AND ipbtc.iplan_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iplan_id
           AND ipbtc.iplan_insurance_order_num = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_rank
          LEFT OUTER JOIN (
            SELECT
                rh_837_claim.patient_dw_id,
                rh_837_claim.iplan_id,
                rh_837_claim.iplan_insurance_order_num
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwpf_views.rh_837_claim
              WHERE rh_837_claim.bill_type_code LIKE '13%'
              GROUP BY 1, 2, 3
          ) AS opbtc ON opbtc.patient_dw_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id
           AND opbtc.iplan_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iplan_id
           AND opbtc.iplan_insurance_order_num = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_rank
          LEFT OUTER JOIN (
            SELECT
                rh_837_claim.bill_type_code,
                rh_837_claim.patient_dw_id,
                rh_837_claim.iplan_id,
                rh_837_claim.iplan_insurance_order_num
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwpf_views.rh_837_claim
              WHERE CAST(rh_837_claim.pat_acct_num as FLOAT64) IN(
                SELECT
                    CAST(bqutil.fn.cw_td_normalize_number(edw_daily_denial_inventory.account_no) as FLOAT64) AS account_no
                  FROM
                    {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory
              )
              QUALIFY row_number() OVER (PARTITION BY rh_837_claim.patient_dw_id, rh_837_claim.iplan_id, rh_837_claim.iplan_insurance_order_num ORDER BY rh_837_claim.bill_date DESC) = 1
          ) AS mrbtc ON mrbtc.patient_dw_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.patient_dw_id
           AND mrbtc.iplan_id = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.iplan_id
           AND mrbtc.iplan_insurance_order_num = a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.payer_rank
          LEFT OUTER JOIN (
            SELECT
                ri.pat_acct_num,
                ri.coid,
                ri.iplan_id AS initial_iplan,
                fi.iplan_financial_class_code AS initial_fc,
                rh.iplan_id AS claim_iplan,
                fi1.iplan_financial_class_code AS claim_fc,
                left(org.ssc_name, 3) AS ssc_name
              FROM
                `{{ params.param_parallon_cur_project_id }}`.edwpf_views.registration_iplan AS ri
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpf_views.fact_patient AS fp ON fp.patient_dw_id = ri.patient_dw_id
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpbs_views.dim_rcm_organization AS org ON rtrim(ri.coid) = rtrim(org.coid)
                INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_denial_inventory AS d ON CAST(bqutil.fn.cw_td_normalize_number(d.account_no) as FLOAT64) = ri.pat_acct_num
                 AND rtrim(d.coid) = rtrim(ri.coid)
                 AND upper(rtrim(d.source_system_code)) = 'N'
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpf_views.facility_iplan AS fi ON fi.payor_dw_id = ri.payor_dw_id
                LEFT OUTER JOIN (
                  SELECT
                      rh_837_claim.patient_dw_id,
                      rh_837_claim.iplan_id,
                      rh_837_claim.coid
                    FROM
                      `{{ params.param_parallon_cur_project_id }}`.edwpf_views.rh_837_claim
                    WHERE rh_837_claim.iplan_insurance_order_num = 1
                     AND (rh_837_claim.patient_dw_id, rh_837_claim.bill_date) IN(
                      SELECT AS STRUCT
                          rh_837_claim_0.patient_dw_id,
                          min(rh_837_claim_0.bill_date) AS min_bill_date
                        FROM
                          `{{ params.param_parallon_cur_project_id }}`.edwpf_views.rh_837_claim AS rh_837_claim_0
                        GROUP BY 1
                    )
                ) AS rh ON rh.patient_dw_id = ri.patient_dw_id
                LEFT OUTER JOIN `{{ params.param_parallon_cur_project_id }}`.edwpf_views.facility_iplan AS fi1 ON fi1.iplan_id = rh.iplan_id
                 AND rtrim(fi1.coid) = rtrim(rh.coid)
              WHERE ri.iplan_insurance_order_num = 1
              QUALIFY row_number() OVER (PARTITION BY ri.patient_dw_id ORDER BY ri.eff_from_date) = 1
          ) AS clip ON clip.pat_acct_num = CAST(bqutil.fn.cw_td_normalize_number(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.account_no) as FLOAT64)
           AND rtrim(clip.coid) = rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.coid)
           AND clip.initial_iplan <> clip.claim_iplan
          CROSS JOIN UNNEST(ARRAY[
            CASE
              WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_hcfa_icd10_code IS NULL
               OR trim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_hcfa_icd10_code) = '' THEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_code_hcfa
              ELSE a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.drg_hcfa_icd10_code
            END
          ]) AS drg_code_int
          CROSS JOIN UNNEST(ARRAY[
            date_diff(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.first_denial_date, a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.maxupymtdate, DAY)
          ]) AS upymt_to_denial_age
          CROSS JOIN UNNEST(ARRAY[
            CASE
              WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_coid) = '26600' THEN 'Mercy Health'
              WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.company_code_1) = 'H' THEN 'HCA'
              WHEN rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.company_code_1) = 'A' THEN 'Life Point'
              WHEN a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.coid_1 IN(
                '13100', '13110', '13130', '13140', '13190', '13200', '13230'
              ) THEN 'Capella'
              ELSE 'Other Clients'
            END
          ]) AS cmp_name
        WHERE upper(rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.source_system_code)) = 'N'
         AND rtrim(a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.summary_7_member_ind) = 'Y'
         AND a11_fp_a12_org_fc_und_discr_concdenatdisch_pdu_cm_full_doc_ind_fpd_xfg_dcg_tmn_drg_btc_btc1_ri_enc_enc.pas_coid IN(
          '08591', '08648', '08942', '08945', '08947', '08948', '08949', '08950'
        )
    ) AS a
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN upper(rtrim(a.pa_patient_type)) = 'I'
         AND upper(rtrim(a.all_days_approved)) = 'Y'
         AND upper(a.denial_code_grouping) IN(
          'MEDICAL NECESSITY', 'NO AUTH/NOTIFICATION'
        )
         AND CASE
          WHEN (a.seq_no IS NULL
           OR a.seq_no = 0)
           AND a.current_appeal_balance = 0 THEN a.payor_due_amt
          ELSE a.current_appeal_balance
        END >= 250
         AND (upper(rtrim(a.disp_desc)) <> 'INACTIVE - NOT TRUE DENIAL'
         OR a.disp_desc IS NULL)
         AND upper(rtrim(a.iplan_change_flag)) = 'N'
         AND upper(rtrim(a.claim_type_mostrecent_flag)) = '11X'
         AND (a.payor_financial_class NOT IN(
          1, 2, 15, 20, 99
        )
         OR a.payor_financial_class IS NULL)
         AND upper(rtrim(a.drg_prod_line)) <> 'MS 13 - BEHAVIORAL HEALTH'
         AND (upper(a.pa_auth_type) LIKE '%/I%'
         OR upper(a.pa_auth_type) LIKE '%/B%') THEN 'IDAPI'
        WHEN upper(rtrim(a.pa_patient_type)) = 'I'
         AND upper(rtrim(a.all_days_approved)) = 'N'
         AND upper(a.denial_code_grouping) IN(
          'MEDICAL NECESSITY', 'NO AUTH/NOTIFICATION'
        )
         AND CASE
          WHEN (a.seq_no IS NULL
           OR a.seq_no = 0)
           AND a.current_appeal_balance = 0 THEN a.payor_due_amt
          ELSE a.current_appeal_balance
        END >= 250
         AND (upper(rtrim(a.disp_desc)) <> 'INACTIVE - NOT TRUE DENIAL'
         OR a.disp_desc IS NULL)
         AND upper(rtrim(a.iplan_change_flag)) = 'N'
         AND (a.payor_financial_class NOT IN(
          1, 2, 15, 20, 99
        )
         OR a.payor_financial_class IS NULL)
         AND upper(rtrim(a.cm_p2p_not_performed)) = 'Y'
         AND upper(rtrim(a.denial_in_midas)) <> 'NOT IN MIDAS' THEN 'MNCDC'
        WHEN upper(rtrim(a.pa_patient_type)) = 'I'
         AND upper(rtrim(a.all_days_approved)) = 'N'
         AND upper(a.denial_code_grouping) IN(
          'MEDICAL NECESSITY', 'NO AUTH/NOTIFICATION'
        )
         AND CASE
          WHEN (a.seq_no IS NULL
           OR a.seq_no = 0)
           AND a.current_appeal_balance = 0 THEN a.payor_due_amt
          ELSE a.current_appeal_balance
        END >= 250
         AND (upper(rtrim(a.disp_desc)) <> 'INACTIVE - NOT TRUE DENIAL'
         OR a.disp_desc IS NULL)
         AND upper(rtrim(a.iplan_change_flag)) = 'N'
         AND (a.payor_financial_class NOT IN(
          1, 2, 15, 20, 99
        )
         OR a.payor_financial_class IS NULL)
         AND upper(rtrim(a.cm_p2p_not_overturned)) = 'Y'
         AND upper(rtrim(a.claim_type_mostrecent_flag)) = '11X'
         AND upper(rtrim(a.denial_in_midas)) <> 'NOT IN MIDAS' THEN 'MNCDI'
        WHEN upper(rtrim(a.pa_patient_type)) = 'I'
         AND upper(rtrim(a.denial_code_grouping)) = 'NO AUTH/NOTIFICATION'
         AND upper(rtrim(a.optact_retroelg_ipch_flag)) = 'Y'
         AND CASE
          WHEN (a.seq_no IS NULL
           OR a.seq_no = 0)
           AND a.current_appeal_balance = 0 THEN a.payor_due_amt
          ELSE a.current_appeal_balance
        END >= 250
         AND (upper(rtrim(a.disp_desc)) <> 'INACTIVE - NOT TRUE DENIAL'
         OR a.disp_desc IS NULL)
         AND upper(rtrim(a.denial_in_midas)) = 'NOT IN MIDAS' THEN 'IAREI'
        WHEN upper(rtrim(a.pa_patient_type)) = 'I'
         AND upper(rtrim(a.denial_code_grouping)) = 'NO AUTH/NOTIFICATION'
         AND upper(rtrim(a.optact_iplanunsinpat_ipch_flag)) = 'Y'
         AND CASE
          WHEN (a.seq_no IS NULL
           OR a.seq_no = 0)
           AND a.current_appeal_balance = 0 THEN a.payor_due_amt
          ELSE a.current_appeal_balance
        END >= 250
         AND (upper(rtrim(a.disp_desc)) <> 'INACTIVE - NOT TRUE DENIAL'
         OR a.disp_desc IS NULL)
         AND upper(rtrim(a.denial_in_midas)) = 'NOT IN MIDAS' THEN 'ICPAP'
        WHEN upper(rtrim(a.pa_patient_type)) = 'I'
         AND upper(rtrim(a.denial_code_grouping)) = 'NO AUTH/NOTIFICATION'
         AND upper(rtrim(a.optact_iplanelgincint_ipch_flag)) = 'Y'
         AND CASE
          WHEN (a.seq_no IS NULL
           OR a.seq_no = 0)
           AND a.current_appeal_balance = 0 THEN a.payor_due_amt
          ELSE a.current_appeal_balance
        END >= 250
         AND (upper(rtrim(a.disp_desc)) <> 'INACTIVE - NOT TRUE DENIAL'
         OR a.disp_desc IS NULL)
         AND upper(rtrim(a.denial_in_midas)) = 'NOT IN MIDAS' THEN 'ICEIR'
        ELSE CAST(NULL as STRING)
      END
    ]) AS opt_tact_root_cause
  WHERE a.root_code IS NULL
   AND opt_tact_root_cause IS NOT NULL
   AND a.seq_no IS NULL
;
