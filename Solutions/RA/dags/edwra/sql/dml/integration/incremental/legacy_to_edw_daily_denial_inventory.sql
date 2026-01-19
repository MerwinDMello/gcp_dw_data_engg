DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/legacy_to_edw_daily_denial_inventory.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/Legacy_To_Edw_Daily_Denial_Inventory.out;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory
WHERE edw_daily_denial_inventory.schema_id = 0.0;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory AS mt USING
  (SELECT DISTINCT -- WRITEOFF_AMT_NET,
 -- WRITEOFF_AMT_MTD,
 t.schema_id,
 ROUND(t.mon_account_payer_id, 0, 'ROUND_HALF_EVEN') AS mon_account_payer_id,
 t.pass_type AS pass_type,
 t.coid AS coid,
 t.account_no,
 t.ssc_id,
 t.rate_schedule_name,
 substr(t.ssc_name, 1, 200) AS ssc_name,
 t.facility_name,
 t.unit_num,
 t.rate_schedule_eff_begin_date,
 t.rate_schedule_eff_end_date,
 t.patient_name,
 t.patient_dob,
 t.iplan_id,
 t.insurance_provider_name,
 substr(t.payer_group_name, 1, 213) AS payer_group_name,
 t.billing_name,
 t.billing_contact_person,
 t.authorization_code,
 t.payer_patient_id,
 t.payer_rank,
 substr(t.pa_financial_class, 1, 10) AS pa_financial_class,
 ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(t.payor_financial_class) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS payor_financial_class,
 t.accounting_period,
 substr(t.major_payer_grp, 1, 20) AS major_payer_grp,
 t.reason_code,
 t.billing_status AS billing_status,
 t.pa_service_code,
 t.pa_account_status,
 t.cc_patient_type AS cc_patient_type,
 t.pa_discharge_status AS pa_discharge_status,
 t.pa_patient_type,
 t.cancel_bill_ind,
 substr(t.admit_source, 1, 1) AS admit_source,
 substr(t.admit_type, 1, 1) AS admit_type,
 t.pa_drg AS pa_drg,
 t.attending_physician_id,
 t.attending_physician_name,
 t.service_date_begin,
 t.discharge_date,
 t.comparison_method,
 t.project_name,
 t.work_queue_name,
 t.status_category_desc AS status_category_desc,
 t.status_desc,
 t.status_phase_desc,
 t.calc_date,
 t.total_charges,
 t.pa_actual_los,
 t.total_billed_charges,
 t.covered_charges,
 t.total_expected_payment,
 t.total_expected_adjustment,
 t.total_pt_responsibility_actual,
 t.total_variance_adjustment,
 t.total_payments,
 t.total_denial_amt,
 t.payor_due_amt,
 t.pa_total_account_bal,
 t.ar_amount,
 t.otd_amt_net,
 t.cash_adj_amt_net,
 t.otd_to_date_amount_mtd,
 t.cash_adj_amt_mtd,
 t.max_aplno,
 t.max_seqno,
 t.appeal_orig_amt,
 t.current_appealed_amt,
 t.current_appeal_balance,
 t.appeal_date_created,
 t.sequence_date_created,
 t.close_date,
 t.max_seq_deadline_date,
 t.sequence_creator,
 t.appeal_owner,
 t.appeal_modifier,
 substr(t.disp_code, 1, 10) AS disp_code,
 t.disp_desc,
 t.web_disp_code,
 t.web_disposition_type AS web_disposition_type,
 t.root_code AS root_code,
 t.root_cause_description,
 t.root_cause_dtl,
 t.external_appeal_code,
 t.apl_appeal_code AS apl_appeal_code,
 substr(t.apl_appeal_desc, 1, 300) AS apl_appeal_desc,
 t.first_denial_date,
 t.denial_age,
 t.pa_denial_update_date,
 t.first_activity_create_date,
 t.last_activity_completion_date,
 t.last_activity_completion_age,
 t.last_user_activity_create_age,
 t.last_reason_change_date,
 t.last_status_change_date,
 t.last_project_change_date,
 t.last_owner_change_date,
 t.activity_due_date,
 t.activity_desc,
 t.activity_subject,
 t.activity_owner,
 t.appeal_sent_activity_ownr,
 t.appeal_initiation_date,
 t.appeal_sent_activity_date,
 t.appeal_sent_activity_age,
 t.last_status_change_age,
 t.activity_due_date_age,
 t.latest_seq_creation_date_age,
 t.latest_appeal_creation_date_age,
 t.appeal_deadline_days_remaining,
 t.appeal_sent_activity_subj,
 t.appeal_sent_activity_desc,
 t.extract_date,
 t.payer_category,
 substr(t.source_system_code, 1, 55) AS source_system_code,
 t.seq_no,
 t.dw_last_update_date,
 t.row_count,
 CAST(ROUND(t.expected_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS expected_amt,
 t.new_appeal_flag AS new_appeal_flag
   FROM
     (SELECT ddi.schema_id,
             ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ddi.mon_account_payer_id) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS mon_account_payer_id,
             ddi.pass_type,
             ddi.coid AS coid,
             ddi.account_no AS account_no,
             ddi.ssc_id,
             ddi.rate_schedule_name,
             dro.ssc_name AS ssc_name, -- SSC_NAME,
 ddi.facility_name AS facility_name,
 ddi.unit_num AS unit_num,
 ddi.rate_schedule_eff_begin_date AS rate_schedule_eff_begin_date,
 ddi.rate_schedule_eff_end_date AS rate_schedule_eff_end_date,
 ddi.patient_name,
 ddi.patient_dob AS patient_dob,
 ddi.iplan_id AS iplan_id,
 ddi.insurance_provider_name,
 rmpg.major_payor_group_desc AS payer_group_name,
 ddi.billing_name,
 ddi.billing_contact_person,
 regip.treatment_authorization_num AS authorization_code,
 ddi.payer_patient_id,
 ddi.payer_rank AS payer_rank,
 trim(ddi.pa_financial_class) AS pa_financial_class,
 trim(format('%#20.0f', ddi.payor_financial_class)) AS payor_financial_class,
 ddi.accounting_period,
 fip.major_payor_group_id AS major_payer_grp,
 ddi.reason_code,
 ddi.billing_status,
 ddi.pa_service_code,
 ddi.pa_account_status,
 ddi.cc_patient_type,
 ddi.pa_discharge_status,
 ddi.pa_patient_type,
 ddi.cancel_bill_ind,
 fp.admission_source_code AS admit_source,
 fp.admission_type_code AS admit_type,
 ddi.pa_drg,
 ddi.attending_physician_id,
 ddi.attending_physician_name,
 ddi.service_date_begin AS service_date_begin,
 ddi.discharge_date AS discharge_date,
 ddi.comparison_method,
 ddi.project_name,
 ddi.work_queue_name,
 ddi.status_category_desc,
 ddi.status_desc,
 ddi.status_phase_desc,
 eord.calc_date AS calc_date,
 coalesce(ddi.total_charges, CAST(0 AS NUMERIC)) AS total_charges,
 ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(coalesce(ddi.pa_actual_los, format('%4d', 0))) AS NUMERIC), 3, 'ROUND_HALF_EVEN') AS pa_actual_los,
 coalesce(fp.total_billed_charges, CAST(0 AS NUMERIC)) AS total_billed_charges,
 ROUND(coalesce(ddi.total_billed_charges, CAST(0 AS NUMERIC)) - coalesce(regip.non_covered_charge_amt, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS covered_charges,
 coalesce(ddi.total_expected_payment, CAST(0 AS NUMERIC)) AS total_expected_payment,
 coalesce(ddi.total_expected_adjustment, CAST(0 AS NUMERIC)) AS total_expected_adjustment,
 coalesce(ddi.total_pt_responsibility_actual, CAST(0 AS NUMERIC)) AS total_pt_responsibility_actual,
 coalesce(ddi.total_variance_adjustment, CAST(0 AS NUMERIC)) AS total_variance_adjustment,
 coalesce(ddi.total_payments, CAST(0 AS NUMERIC)) AS total_payments,
 coalesce(ddi.total_denial_amt, CAST(0 AS NUMERIC)) AS total_denial_amt,
 coalesce(ddi.payor_due_amt, CAST(0 AS NUMERIC)) AS payor_due_amt,
 coalesce(ddi.pa_total_account_bal, CAST(0 AS NUMERIC)) AS pa_total_account_bal,
 coalesce(ddi.ar_amount, CAST(0 AS NUMERIC)) AS ar_amount,
 coalesce(ddi.otd_amt, CAST(0 AS NUMERIC)) AS otd_amt_net, -- CAST(COALESCE(WRITEOFF_AMT,0) as Decimal(18,3) )as WRITEOFF_AMT_NET,
 coalesce(ddi.cash_adj_amt, CAST(0 AS NUMERIC)) AS cash_adj_amt_net,
 coalesce(ddi.otd_to_date_amt_mtd, CAST(0 AS NUMERIC)) AS otd_to_date_amount_mtd, -- CAST(COALESCE(WRITEOFF_AMT_MTD,0) as Decimal(18,3)) as WRITEOFF_AMT_MTD,
 coalesce(ddi.cash_adj_amt_mtd, CAST(0 AS NUMERIC)) AS cash_adj_amt_mtd,
 ddi.max_aplno,
 ddi.max_seqno,
 coalesce(ddi.appeal_orig_amt, CAST(0 AS NUMERIC)) AS appeal_orig_amt,
 coalesce(ddi.current_appealed_amt, CAST(0 AS NUMERIC)) AS current_appealed_amt,
 coalesce(ddi.current_appeal_balance, CAST(0 AS NUMERIC)) AS current_appeal_balance,
 ddi.appeal_date_created AS appeal_date_created,
 ddi.sequence_date_created AS sequence_date_created,
 ddi.close_date AS close_date,
 ddi.max_seq_deadline_date AS max_seq_deadline_date,
 ddi.sequence_creator,
 ddi.appeal_owner,
 ddi.appeal_modifier, -- NULL as DISP_CODE,
 concat(trim(ddi.web_disposition_type), trim(ddi.disp_code)) AS disp_code,
 ddi.disp_desc,
 ddi.web_disp_code,
 ddi.web_disposition_type,
 ddi.root_code,
 ddi.root_cause_description,
 ddi.root_cause_dtl,
 ddi.external_appeal_code,
 ddi.apl_appeal_code,
 ddi.apl_appeal_desc,
 ddi.first_denial_date AS first_denial_date,
 ddi.denial_age,
 ddi.pa_denial_update_date AS pa_denial_update_date,
 ddi.first_activity_create_date AS first_activity_create_date,
 ddi.last_activity_completion_date AS last_activity_completion_date,
 ddi.last_activity_completion_age,
 ddi.last_user_activity_create_age,
 ddi.last_reason_change_date AS last_reason_change_date,
 ddi.last_status_change_date AS last_status_change_date,
 ddi.last_project_change_date AS last_project_change_date,
 ddi.last_owner_change_date AS last_owner_change_date,
 ddi.activity_due_date AS activity_due_date,
 ddi.activity_desc,
 ddi.activity_subject,
 ddi.activity_owner,
 ddi.appeal_sent_activity_ownr,
 ddi.appeal_initiation_date,
 ddi.appeal_sent_activity_date AS appeal_sent_activity_date,
 ddi.appeal_sent_activity_age,
 ddi.last_status_change_age,
 ddi.activity_due_date_age,
 ddi.latest_seq_creation_date_age AS latest_seq_creation_date_age,
 ddi.latest_seq_creation_date_age AS latest_appeal_creation_date_age,
 ddi.appeal_deadline_days_remaining,
 ddi.appeal_sent_activity_subj,
 ddi.appeal_sent_activity_desc,
 DATE(ddi.extract_date) AS extract_date,
 ddi.payer_category,
 ddi.source_system_code,
 ddi.seq_no AS seq_no,
 current_datetime('US/Central') AS dw_last_update_date,
 ddi.row_count AS ROW_COUNT, -- Cast(EXPETED_AMT as decimal(18,3)) as EXPECTED_AMT,
 sum(coalesce(ddi.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(ddi.total_pt_responsibility_actual, CAST(0 AS NUMERIC)) - coalesce(ddi.total_variance_adjustment, CAST(0 AS NUMERIC)) - coalesce(ddi.total_payments, CAST(0 AS NUMERIC)) - coalesce(ddi.total_denial_amt, CAST(0 AS NUMERIC))) OVER () AS expected_amt,
                                                                                                                                                                                                                                                                                                             ddi.new_appeal_flag
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials AS ddi
      LEFT OUTER JOIN
        (SELECT max(ldd.coid) AS coid,
                max(ldd.account_no) AS account_no,
                ldd.iplan_id,
                ldd.payer_rank,
                eor.pat_acct_num,
                max(eor.eor_log_date) AS calc_date
         FROM  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials AS ldd
         LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS eor ON upper(rtrim(ldd.coid)) = upper(rtrim(eor.coid))
         AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ldd.account_no) AS FLOAT64) = eor.pat_acct_num
         AND ldd.iplan_id = eor.iplan_id
         AND ldd.payer_rank = eor.iplan_insurance_order_num
         AND upper(rtrim(eor.eor_reversal_code)) = ''
         AND upper(rtrim(eor.source_system_code)) = 'P'
         GROUP BY upper(ldd.coid),
                  upper(ldd.account_no),
                  3,
                  4,
                  5) AS eord ON upper(rtrim(ddi.coid)) = upper(rtrim(eord.coid))
      AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ddi.account_no) AS FLOAT64) = eord.pat_acct_num
      AND ddi.iplan_id = eord.iplan_id
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fp ON upper(rtrim(ddi.coid)) = upper(rtrim(fp.coid))
      AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ddi.account_no) AS FLOAT64) = fp.pat_acct_num
      LEFT OUTER JOIN
        (SELECT ext.coid,
                ext.account_no,
                ext.iplan_id,
                ext.payer_rank,
                rip.pat_acct_num,
                rip.eff_from_date,
                rip.non_covered_charge_amt,
                rip.treatment_authorization_num
         FROM
           (SELECT max(ldd.coid) AS coid,
                   max(ldd.account_no) AS account_no,
                   ldd.iplan_id,
                   ldd.payer_rank,
                   max(eor.eff_from_date) AS eff_date
            FROM  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials AS ldd
            LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.registration_iplan AS eor ON upper(rtrim(ldd.coid)) = upper(rtrim(eor.coid))
            AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ldd.account_no) AS FLOAT64) = eor.pat_acct_num
            AND ldd.iplan_id = eor.iplan_id
            AND ldd.payer_rank = eor.iplan_insurance_order_num
            GROUP BY upper(ldd.coid),
                     upper(ldd.account_no),
                     3,
                     4) AS ext
         LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.registration_iplan AS rip ON upper(rtrim(ext.coid)) = upper(rtrim(rip.coid))
         AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ext.account_no) AS FLOAT64) = rip.pat_acct_num
         AND ext.iplan_id = rip.iplan_id
         AND ext.payer_rank = rip.iplan_insurance_order_num
         AND ext.eff_date = rip.eff_from_date) AS regip ON upper(rtrim(ddi.coid)) = upper(rtrim(regip.coid))
      AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ddi.account_no) AS FLOAT64) = regip.pat_acct_num
      AND ddi.iplan_id = regip.iplan_id
      AND ddi.payer_rank = regip.payer_rank
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS fip ON upper(rtrim(ddi.coid)) = upper(rtrim(fip.coid))
      AND ddi.iplan_id = fip.iplan_id
      LEFT OUTER JOIN
        (SELECT DISTINCT ref_major_payor_group.major_payor_group_code,
                         ref_major_payor_group.major_payor_group_desc
         FROM {{ params.param_auth_base_views_dataset_name }}.ref_major_payor_group) AS rmpg ON upper(rtrim(fip.major_payor_group_id)) = upper(rtrim(rmpg.major_payor_group_code))
      INNER JOIN {{ params.param_parallon_pbs_base_views_dataset_name }}.dim_rcm_organization AS dro ON upper(rtrim(ddi.coid)) = upper(rtrim(dro.coid))) AS t QUALIFY row_number() OVER (PARTITION BY t.schema_id,
                                                                                                                                                                                           upper(coid),
                                                                                                                                                                                           upper(t.account_no),
                                                                                                                                                                                           t.iplan_id,
                                                                                                                                                                                           t.payer_rank,
                                                                                                                                                                                           t.max_aplno,
                                                                                                                                                                                           t.max_seqno,
                                                                                                                                                                                           upper(t.source_system_code)
                                                                                                                                                                              ORDER BY t.seq_no DESC) = 1) AS ms ON coalesce(mt.schema_id, 0) = coalesce(ms.schema_id, 0)
AND coalesce(mt.schema_id, 1) = coalesce(ms.schema_id, 1)
AND (coalesce(mt.mon_account_payer_id, NUMERIC '0') = coalesce(ms.mon_account_payer_id, NUMERIC '0')
     AND coalesce(mt.mon_account_payer_id, NUMERIC '1') = coalesce(ms.mon_account_payer_id, NUMERIC '1'))
AND (upper(coalesce(mt.pass_type, '0')) = upper(coalesce(ms.pass_type, '0'))
     AND upper(coalesce(mt.pass_type, '1')) = upper(coalesce(ms.pass_type, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.account_no, '0')) = upper(coalesce(ms.account_no, '0'))
     AND upper(coalesce(mt.account_no, '1')) = upper(coalesce(ms.account_no, '1')))
AND (coalesce(mt.ssc_id, NUMERIC '0') = coalesce(ms.ssc_id, NUMERIC '0')
     AND coalesce(mt.ssc_id, NUMERIC '1') = coalesce(ms.ssc_id, NUMERIC '1'))
AND (upper(coalesce(mt.rate_schedule_name, '0')) = upper(coalesce(ms.rate_schedule_name, '0'))
     AND upper(coalesce(mt.rate_schedule_name, '1')) = upper(coalesce(ms.rate_schedule_name, '1')))
AND (upper(coalesce(mt.ssc_name, '0')) = upper(coalesce(ms.ssc_name, '0'))
     AND upper(coalesce(mt.ssc_name, '1')) = upper(coalesce(ms.ssc_name, '1')))
AND (upper(coalesce(mt.facility_name, '0')) = upper(coalesce(ms.facility_name, '0'))
     AND upper(coalesce(mt.facility_name, '1')) = upper(coalesce(ms.facility_name, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.rate_schedule_eff_begin_date, DATE '1970-01-01') = coalesce(ms.rate_schedule_eff_begin_date, DATE '1970-01-01')
     AND coalesce(mt.rate_schedule_eff_begin_date, DATE '1970-01-02') = coalesce(ms.rate_schedule_eff_begin_date, DATE '1970-01-02'))
AND (coalesce(mt.rate_schedule_eff_end_date, DATE '1970-01-01') = coalesce(ms.rate_schedule_eff_end_date, DATE '1970-01-01')
     AND coalesce(mt.rate_schedule_eff_end_date, DATE '1970-01-02') = coalesce(ms.rate_schedule_eff_end_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.patient_name, '0')) = upper(coalesce(ms.patient_name, '0'))
     AND upper(coalesce(mt.patient_name, '1')) = upper(coalesce(ms.patient_name, '1')))
AND (coalesce(mt.patient_dob, DATE '1970-01-01') = coalesce(ms.patient_dob, DATE '1970-01-01')
     AND coalesce(mt.patient_dob, DATE '1970-01-02') = coalesce(ms.patient_dob, DATE '1970-01-02'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (upper(coalesce(mt.insurance_provider_name, '0')) = upper(coalesce(ms.insurance_provider_name, '0'))
     AND upper(coalesce(mt.insurance_provider_name, '1')) = upper(coalesce(ms.insurance_provider_name, '1')))
AND (upper(coalesce(mt.payer_group_name, '0')) = upper(coalesce(ms.payer_group_name, '0'))
     AND upper(coalesce(mt.payer_group_name, '1')) = upper(coalesce(ms.payer_group_name, '1')))
AND (upper(coalesce(mt.billing_name, '0')) = upper(coalesce(ms.billing_name, '0'))
     AND upper(coalesce(mt.billing_name, '1')) = upper(coalesce(ms.billing_name, '1')))
AND (upper(coalesce(mt.billing_contact_person, '0')) = upper(coalesce(ms.billing_contact_person, '0'))
     AND upper(coalesce(mt.billing_contact_person, '1')) = upper(coalesce(ms.billing_contact_person, '1')))
AND (upper(coalesce(mt.authorization_code, '0')) = upper(coalesce(ms.authorization_code, '0'))
     AND upper(coalesce(mt.authorization_code, '1')) = upper(coalesce(ms.authorization_code, '1')))
AND (upper(coalesce(mt.payer_patient_id, '0')) = upper(coalesce(ms.payer_patient_id, '0'))
     AND upper(coalesce(mt.payer_patient_id, '1')) = upper(coalesce(ms.payer_patient_id, '1')))
AND (coalesce(mt.payer_rank, 0) = coalesce(ms.payer_rank, 0)
     AND coalesce(mt.payer_rank, 1) = coalesce(ms.payer_rank, 1))
AND (upper(coalesce(mt.pa_financial_class, '0')) = upper(coalesce(ms.pa_financial_class, '0'))
     AND upper(coalesce(mt.pa_financial_class, '1')) = upper(coalesce(ms.pa_financial_class, '1')))
AND (coalesce(mt.payor_financial_class, NUMERIC '0') = coalesce(ms.payor_financial_class, NUMERIC '0')
     AND coalesce(mt.payor_financial_class, NUMERIC '1') = coalesce(ms.payor_financial_class, NUMERIC '1'))
AND (upper(coalesce(mt.accounting_period, '0')) = upper(coalesce(ms.accounting_period, '0'))
     AND upper(coalesce(mt.accounting_period, '1')) = upper(coalesce(ms.accounting_period, '1')))
AND (upper(coalesce(mt.major_payer_grp, '0')) = upper(coalesce(ms.major_payer_grp, '0'))
     AND upper(coalesce(mt.major_payer_grp, '1')) = upper(coalesce(ms.major_payer_grp, '1')))
AND (upper(coalesce(mt.reason_code, '0')) = upper(coalesce(ms.reason_code, '0'))
     AND upper(coalesce(mt.reason_code, '1')) = upper(coalesce(ms.reason_code, '1')))
AND (upper(coalesce(mt.billing_status, '0')) = upper(coalesce(ms.billing_status, '0'))
     AND upper(coalesce(mt.billing_status, '1')) = upper(coalesce(ms.billing_status, '1')))
AND (upper(coalesce(mt.pa_service_code, '0')) = upper(coalesce(ms.pa_service_code, '0'))
     AND upper(coalesce(mt.pa_service_code, '1')) = upper(coalesce(ms.pa_service_code, '1')))
AND (upper(coalesce(mt.pa_account_status, '0')) = upper(coalesce(ms.pa_account_status, '0'))
     AND upper(coalesce(mt.pa_account_status, '1')) = upper(coalesce(ms.pa_account_status, '1')))
AND (upper(coalesce(mt.cc_patient_type, '0')) = upper(coalesce(ms.cc_patient_type, '0'))
     AND upper(coalesce(mt.cc_patient_type, '1')) = upper(coalesce(ms.cc_patient_type, '1')))
AND (upper(coalesce(mt.pa_discharge_status, '0')) = upper(coalesce(ms.pa_discharge_status, '0'))
     AND upper(coalesce(mt.pa_discharge_status, '1')) = upper(coalesce(ms.pa_discharge_status, '1')))
AND (upper(coalesce(mt.pa_patient_type, '0')) = upper(coalesce(ms.pa_patient_type, '0'))
     AND upper(coalesce(mt.pa_patient_type, '1')) = upper(coalesce(ms.pa_patient_type, '1')))
AND (upper(coalesce(mt.cancel_bill_ind, '0')) = upper(coalesce(ms.cancel_bill_ind, '0'))
     AND upper(coalesce(mt.cancel_bill_ind, '1')) = upper(coalesce(ms.cancel_bill_ind, '1')))
AND (upper(coalesce(mt.admit_source, '0')) = upper(coalesce(ms.admit_source, '0'))
     AND upper(coalesce(mt.admit_source, '1')) = upper(coalesce(ms.admit_source, '1')))
AND (upper(coalesce(mt.admit_type, '0')) = upper(coalesce(ms.admit_type, '0'))
     AND upper(coalesce(mt.admit_type, '1')) = upper(coalesce(ms.admit_type, '1')))
AND (upper(coalesce(mt.pa_drg, '0')) = upper(coalesce(ms.pa_drg, '0'))
     AND upper(coalesce(mt.pa_drg, '1')) = upper(coalesce(ms.pa_drg, '1')))
AND (upper(coalesce(mt.attending_physician_id, '0')) = upper(coalesce(ms.attending_physician_id, '0'))
     AND upper(coalesce(mt.attending_physician_id, '1')) = upper(coalesce(ms.attending_physician_id, '1')))
AND (upper(coalesce(mt.attending_physician_name, '0')) = upper(coalesce(ms.attending_physician_name, '0'))
     AND upper(coalesce(mt.attending_physician_name, '1')) = upper(coalesce(ms.attending_physician_name, '1')))
AND (coalesce(mt.service_date_begin, DATE '1970-01-01') = coalesce(ms.service_date_begin, DATE '1970-01-01')
     AND coalesce(mt.service_date_begin, DATE '1970-01-02') = coalesce(ms.service_date_begin, DATE '1970-01-02'))
AND (coalesce(mt.discharge_date, DATE '1970-01-01') = coalesce(ms.discharge_date, DATE '1970-01-01')
     AND coalesce(mt.discharge_date, DATE '1970-01-02') = coalesce(ms.discharge_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.comparison_method, '0')) = upper(coalesce(ms.comparison_method, '0'))
     AND upper(coalesce(mt.comparison_method, '1')) = upper(coalesce(ms.comparison_method, '1')))
AND (upper(coalesce(mt.project_name, '0')) = upper(coalesce(ms.project_name, '0'))
     AND upper(coalesce(mt.project_name, '1')) = upper(coalesce(ms.project_name, '1')))
AND (upper(coalesce(mt.work_queue_name, '0')) = upper(coalesce(ms.work_queue_name, '0'))
     AND upper(coalesce(mt.work_queue_name, '1')) = upper(coalesce(ms.work_queue_name, '1')))
AND (upper(coalesce(mt.status_category_desc, '0')) = upper(coalesce(ms.status_category_desc, '0'))
     AND upper(coalesce(mt.status_category_desc, '1')) = upper(coalesce(ms.status_category_desc, '1')))
AND (upper(coalesce(mt.status_desc, '0')) = upper(coalesce(ms.status_desc, '0'))
     AND upper(coalesce(mt.status_desc, '1')) = upper(coalesce(ms.status_desc, '1')))
AND (upper(coalesce(mt.status_phase_desc, '0')) = upper(coalesce(ms.status_phase_desc, '0'))
     AND upper(coalesce(mt.status_phase_desc, '1')) = upper(coalesce(ms.status_phase_desc, '1')))
AND (coalesce(mt.calc_date, DATE '1970-01-01') = coalesce(ms.calc_date, DATE '1970-01-01')
     AND coalesce(mt.calc_date, DATE '1970-01-02') = coalesce(ms.calc_date, DATE '1970-01-02'))
AND (coalesce(mt.total_charges, NUMERIC '0') = coalesce(ms.total_charges, NUMERIC '0')
     AND coalesce(mt.total_charges, NUMERIC '1') = coalesce(ms.total_charges, NUMERIC '1'))
AND (coalesce(mt.pa_actual_los, NUMERIC '0') = coalesce(ms.pa_actual_los, NUMERIC '0')
     AND coalesce(mt.pa_actual_los, NUMERIC '1') = coalesce(ms.pa_actual_los, NUMERIC '1'))
AND (coalesce(mt.total_billed_charges, NUMERIC '0') = coalesce(ms.total_billed_charges, NUMERIC '0')
     AND coalesce(mt.total_billed_charges, NUMERIC '1') = coalesce(ms.total_billed_charges, NUMERIC '1'))
AND (coalesce(mt.covered_charges, NUMERIC '0') = coalesce(ms.covered_charges, NUMERIC '0')
     AND coalesce(mt.covered_charges, NUMERIC '1') = coalesce(ms.covered_charges, NUMERIC '1'))
AND (coalesce(mt.total_expected_payment, NUMERIC '0') = coalesce(ms.total_expected_payment, NUMERIC '0')
     AND coalesce(mt.total_expected_payment, NUMERIC '1') = coalesce(ms.total_expected_payment, NUMERIC '1'))
AND (coalesce(mt.total_expected_adjustment, NUMERIC '0') = coalesce(ms.total_expected_adjustment, NUMERIC '0')
     AND coalesce(mt.total_expected_adjustment, NUMERIC '1') = coalesce(ms.total_expected_adjustment, NUMERIC '1'))
AND (coalesce(mt.total_pt_responsibility_actual, NUMERIC '0') = coalesce(ms.total_pt_responsibility_actual, NUMERIC '0')
     AND coalesce(mt.total_pt_responsibility_actual, NUMERIC '1') = coalesce(ms.total_pt_responsibility_actual, NUMERIC '1'))
AND (coalesce(mt.total_variance_adjustment, NUMERIC '0') = coalesce(ms.total_variance_adjustment, NUMERIC '0')
     AND coalesce(mt.total_variance_adjustment, NUMERIC '1') = coalesce(ms.total_variance_adjustment, NUMERIC '1'))
AND (coalesce(mt.total_payments, NUMERIC '0') = coalesce(ms.total_payments, NUMERIC '0')
     AND coalesce(mt.total_payments, NUMERIC '1') = coalesce(ms.total_payments, NUMERIC '1'))
AND (coalesce(mt.total_denial_amt, NUMERIC '0') = coalesce(ms.total_denial_amt, NUMERIC '0')
     AND coalesce(mt.total_denial_amt, NUMERIC '1') = coalesce(ms.total_denial_amt, NUMERIC '1'))
AND (coalesce(mt.payor_due_amt, NUMERIC '0') = coalesce(ms.payor_due_amt, NUMERIC '0')
     AND coalesce(mt.payor_due_amt, NUMERIC '1') = coalesce(ms.payor_due_amt, NUMERIC '1'))
AND (coalesce(mt.pa_total_account_bal, NUMERIC '0') = coalesce(ms.pa_total_account_bal, NUMERIC '0')
     AND coalesce(mt.pa_total_account_bal, NUMERIC '1') = coalesce(ms.pa_total_account_bal, NUMERIC '1'))
AND (coalesce(mt.ar_amount, NUMERIC '0') = coalesce(ms.ar_amount, NUMERIC '0')
     AND coalesce(mt.ar_amount, NUMERIC '1') = coalesce(ms.ar_amount, NUMERIC '1'))
AND (coalesce(mt.otd_amt_net, NUMERIC '0') = coalesce(ms.otd_amt_net, NUMERIC '0')
     AND coalesce(mt.otd_amt_net, NUMERIC '1') = coalesce(ms.otd_amt_net, NUMERIC '1'))
AND (coalesce(mt.cash_adj_amt_net, NUMERIC '0') = coalesce(ms.cash_adj_amt_net, NUMERIC '0')
     AND coalesce(mt.cash_adj_amt_net, NUMERIC '1') = coalesce(ms.cash_adj_amt_net, NUMERIC '1'))
AND (coalesce(mt.otd_to_date_amount_mtd, NUMERIC '0') = coalesce(ms.otd_to_date_amount_mtd, NUMERIC '0')
     AND coalesce(mt.otd_to_date_amount_mtd, NUMERIC '1') = coalesce(ms.otd_to_date_amount_mtd, NUMERIC '1'))
AND (coalesce(mt.cash_adj_amt_mtd, NUMERIC '0') = coalesce(ms.cash_adj_amt_mtd, NUMERIC '0')
     AND coalesce(mt.cash_adj_amt_mtd, NUMERIC '1') = coalesce(ms.cash_adj_amt_mtd, NUMERIC '1'))
AND (coalesce(mt.max_aplno, 0) = coalesce(ms.max_aplno, 0)
     AND coalesce(mt.max_aplno, 1) = coalesce(ms.max_aplno, 1))
AND (coalesce(mt.max_seqno, 0) = coalesce(ms.max_seqno, 0)
     AND coalesce(mt.max_seqno, 1) = coalesce(ms.max_seqno, 1))
AND (coalesce(mt.appeal_orig_amt, NUMERIC '0') = coalesce(ms.appeal_orig_amt, NUMERIC '0')
     AND coalesce(mt.appeal_orig_amt, NUMERIC '1') = coalesce(ms.appeal_orig_amt, NUMERIC '1'))
AND (coalesce(mt.current_appealed_amt, NUMERIC '0') = coalesce(ms.current_appealed_amt, NUMERIC '0')
     AND coalesce(mt.current_appealed_amt, NUMERIC '1') = coalesce(ms.current_appealed_amt, NUMERIC '1'))
AND (coalesce(mt.current_appeal_balance, NUMERIC '0') = coalesce(ms.current_appeal_balance, NUMERIC '0')
     AND coalesce(mt.current_appeal_balance, NUMERIC '1') = coalesce(ms.current_appeal_balance, NUMERIC '1'))
AND (coalesce(mt.appeal_date_created, DATE '1970-01-01') = coalesce(ms.appeal_date_created, DATE '1970-01-01')
     AND coalesce(mt.appeal_date_created, DATE '1970-01-02') = coalesce(ms.appeal_date_created, DATE '1970-01-02'))
AND (coalesce(mt.sequence_date_created, DATE '1970-01-01') = coalesce(ms.sequence_date_created, DATE '1970-01-01')
     AND coalesce(mt.sequence_date_created, DATE '1970-01-02') = coalesce(ms.sequence_date_created, DATE '1970-01-02'))
AND (coalesce(mt.close_date, DATE '1970-01-01') = coalesce(ms.close_date, DATE '1970-01-01')
     AND coalesce(mt.close_date, DATE '1970-01-02') = coalesce(ms.close_date, DATE '1970-01-02'))
AND (coalesce(mt.max_seq_deadline_date, DATE '1970-01-01') = coalesce(ms.max_seq_deadline_date, DATE '1970-01-01')
     AND coalesce(mt.max_seq_deadline_date, DATE '1970-01-02') = coalesce(ms.max_seq_deadline_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.sequence_creator, '0')) = upper(coalesce(ms.sequence_creator, '0'))
     AND upper(coalesce(mt.sequence_creator, '1')) = upper(coalesce(ms.sequence_creator, '1')))
AND (upper(coalesce(mt.appeal_owner, '0')) = upper(coalesce(ms.appeal_owner, '0'))
     AND upper(coalesce(mt.appeal_owner, '1')) = upper(coalesce(ms.appeal_owner, '1')))
AND (upper(coalesce(mt.appeal_modifier, '0')) = upper(coalesce(ms.appeal_modifier, '0'))
     AND upper(coalesce(mt.appeal_modifier, '1')) = upper(coalesce(ms.appeal_modifier, '1')))
AND (upper(coalesce(mt.disp_code, '0')) = upper(coalesce(ms.disp_code, '0'))
     AND upper(coalesce(mt.disp_code, '1')) = upper(coalesce(ms.disp_code, '1')))
AND (upper(coalesce(mt.disp_desc, '0')) = upper(coalesce(ms.disp_desc, '0'))
     AND upper(coalesce(mt.disp_desc, '1')) = upper(coalesce(ms.disp_desc, '1')))
AND (coalesce(mt.web_disp_code, 0) = coalesce(ms.web_disp_code, 0)
     AND coalesce(mt.web_disp_code, 1) = coalesce(ms.web_disp_code, 1))
AND (upper(coalesce(mt.web_disposition_type, '0')) = upper(coalesce(ms.web_disposition_type, '0'))
     AND upper(coalesce(mt.web_disposition_type, '1')) = upper(coalesce(ms.web_disposition_type, '1')))
AND (upper(coalesce(mt.root_code, '0')) = upper(coalesce(ms.root_code, '0'))
     AND upper(coalesce(mt.root_code, '1')) = upper(coalesce(ms.root_code, '1')))
AND (upper(coalesce(mt.root_cause_description, '0')) = upper(coalesce(ms.root_cause_description, '0'))
     AND upper(coalesce(mt.root_cause_description, '1')) = upper(coalesce(ms.root_cause_description, '1')))
AND (upper(coalesce(mt.root_cause_dtl, '0')) = upper(coalesce(ms.root_cause_dtl, '0'))
     AND upper(coalesce(mt.root_cause_dtl, '1')) = upper(coalesce(ms.root_cause_dtl, '1')))
AND (upper(coalesce(mt.external_appeal_code, '0')) = upper(coalesce(ms.external_appeal_code, '0'))
     AND upper(coalesce(mt.external_appeal_code, '1')) = upper(coalesce(ms.external_appeal_code, '1')))
AND (upper(coalesce(mt.apl_appeal_code, '0')) = upper(coalesce(ms.apl_appeal_code, '0'))
     AND upper(coalesce(mt.apl_appeal_code, '1')) = upper(coalesce(ms.apl_appeal_code, '1')))
AND (upper(coalesce(mt.apl_appeal_desc, '0')) = upper(coalesce(ms.apl_appeal_desc, '0'))
     AND upper(coalesce(mt.apl_appeal_desc, '1')) = upper(coalesce(ms.apl_appeal_desc, '1')))
AND (coalesce(mt.first_denial_date, DATE '1970-01-01') = coalesce(ms.first_denial_date, DATE '1970-01-01')
     AND coalesce(mt.first_denial_date, DATE '1970-01-02') = coalesce(ms.first_denial_date, DATE '1970-01-02'))
AND (coalesce(mt.denial_age, 0) = coalesce(ms.denial_age, 0)
     AND coalesce(mt.denial_age, 1) = coalesce(ms.denial_age, 1))
AND (coalesce(mt.pa_denial_update_date, DATE '1970-01-01') = coalesce(ms.pa_denial_update_date, DATE '1970-01-01')
     AND coalesce(mt.pa_denial_update_date, DATE '1970-01-02') = coalesce(ms.pa_denial_update_date, DATE '1970-01-02'))
AND (coalesce(mt.first_activity_create_date, DATE '1970-01-01') = coalesce(ms.first_activity_create_date, DATE '1970-01-01')
     AND coalesce(mt.first_activity_create_date, DATE '1970-01-02') = coalesce(ms.first_activity_create_date, DATE '1970-01-02'))
AND (coalesce(mt.last_activity_completion_date, DATE '1970-01-01') = coalesce(ms.last_activity_completion_date, DATE '1970-01-01')
     AND coalesce(mt.last_activity_completion_date, DATE '1970-01-02') = coalesce(ms.last_activity_completion_date, DATE '1970-01-02'))
AND (coalesce(mt.last_activity_completion_age, 0) = coalesce(ms.last_activity_completion_age, 0)
     AND coalesce(mt.last_activity_completion_age, 1) = coalesce(ms.last_activity_completion_age, 1))
AND (coalesce(mt.last_user_activity_create_age, 0) = coalesce(ms.last_user_activity_create_age, 0)
     AND coalesce(mt.last_user_activity_create_age, 1) = coalesce(ms.last_user_activity_create_age, 1))
AND (coalesce(mt.last_reason_change_date, DATE '1970-01-01') = coalesce(ms.last_reason_change_date, DATE '1970-01-01')
     AND coalesce(mt.last_reason_change_date, DATE '1970-01-02') = coalesce(ms.last_reason_change_date, DATE '1970-01-02'))
AND (coalesce(mt.last_status_change_date, DATE '1970-01-01') = coalesce(ms.last_status_change_date, DATE '1970-01-01')
     AND coalesce(mt.last_status_change_date, DATE '1970-01-02') = coalesce(ms.last_status_change_date, DATE '1970-01-02'))
AND (coalesce(mt.last_project_change_date, DATE '1970-01-01') = coalesce(ms.last_project_change_date, DATE '1970-01-01')
     AND coalesce(mt.last_project_change_date, DATE '1970-01-02') = coalesce(ms.last_project_change_date, DATE '1970-01-02'))
AND (coalesce(mt.last_owner_change_date, DATE '1970-01-01') = coalesce(ms.last_owner_change_date, DATE '1970-01-01')
     AND coalesce(mt.last_owner_change_date, DATE '1970-01-02') = coalesce(ms.last_owner_change_date, DATE '1970-01-02'))
AND (coalesce(mt.activity_due_date, DATE '1970-01-01') = coalesce(ms.activity_due_date, DATE '1970-01-01')
     AND coalesce(mt.activity_due_date, DATE '1970-01-02') = coalesce(ms.activity_due_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.activity_desc, '0')) = upper(coalesce(ms.activity_desc, '0'))
     AND upper(coalesce(mt.activity_desc, '1')) = upper(coalesce(ms.activity_desc, '1')))
AND (upper(coalesce(mt.activity_subject, '0')) = upper(coalesce(ms.activity_subject, '0'))
     AND upper(coalesce(mt.activity_subject, '1')) = upper(coalesce(ms.activity_subject, '1')))
AND (upper(coalesce(mt.activity_owner, '0')) = upper(coalesce(ms.activity_owner, '0'))
     AND upper(coalesce(mt.activity_owner, '1')) = upper(coalesce(ms.activity_owner, '1')))
AND (upper(coalesce(mt.appeal_sent_activity_ownr, '0')) = upper(coalesce(ms.appeal_sent_activity_ownr, '0'))
     AND upper(coalesce(mt.appeal_sent_activity_ownr, '1')) = upper(coalesce(ms.appeal_sent_activity_ownr, '1')))
AND (coalesce(mt.appeal_initiation_date, DATE '1970-01-01') = coalesce(ms.appeal_initiation_date, DATE '1970-01-01')
     AND coalesce(mt.appeal_initiation_date, DATE '1970-01-02') = coalesce(ms.appeal_initiation_date, DATE '1970-01-02'))
AND (coalesce(mt.appeal_sent_activity_date, DATE '1970-01-01') = coalesce(ms.appeal_sent_activity_date, DATE '1970-01-01')
     AND coalesce(mt.appeal_sent_activity_date, DATE '1970-01-02') = coalesce(ms.appeal_sent_activity_date, DATE '1970-01-02'))
AND (coalesce(mt.appeal_sent_activity_age, 0) = coalesce(ms.appeal_sent_activity_age, 0)
     AND coalesce(mt.appeal_sent_activity_age, 1) = coalesce(ms.appeal_sent_activity_age, 1))
AND (coalesce(mt.last_status_change_age, 0) = coalesce(ms.last_status_change_age, 0)
     AND coalesce(mt.last_status_change_age, 1) = coalesce(ms.last_status_change_age, 1))
AND (coalesce(mt.activity_due_date_age, 0) = coalesce(ms.activity_due_date_age, 0)
     AND coalesce(mt.activity_due_date_age, 1) = coalesce(ms.activity_due_date_age, 1))
AND (coalesce(mt.latest_seq_creation_date_age, 0) = coalesce(ms.latest_seq_creation_date_age, 0)
     AND coalesce(mt.latest_seq_creation_date_age, 1) = coalesce(ms.latest_seq_creation_date_age, 1))
AND (coalesce(mt.latest_appeal_creation_date_age, 0) = coalesce(ms.latest_appeal_creation_date_age, 0)
     AND coalesce(mt.latest_appeal_creation_date_age, 1) = coalesce(ms.latest_appeal_creation_date_age, 1))
AND (coalesce(mt.appeal_deadline_days_remaining, 0) = coalesce(ms.appeal_deadline_days_remaining, 0)
     AND coalesce(mt.appeal_deadline_days_remaining, 1) = coalesce(ms.appeal_deadline_days_remaining, 1))
AND (upper(coalesce(mt.appeal_sent_activity_subj, '0')) = upper(coalesce(ms.appeal_sent_activity_subj, '0'))
     AND upper(coalesce(mt.appeal_sent_activity_subj, '1')) = upper(coalesce(ms.appeal_sent_activity_subj, '1')))
AND (upper(coalesce(mt.appeal_sent_activity_desc, '0')) = upper(coalesce(ms.appeal_sent_activity_desc, '0'))
     AND upper(coalesce(mt.appeal_sent_activity_desc, '1')) = upper(coalesce(ms.appeal_sent_activity_desc, '1')))
AND (coalesce(mt.extract_date, DATE '1970-01-01') = coalesce(ms.extract_date, DATE '1970-01-01')
     AND coalesce(mt.extract_date, DATE '1970-01-02') = coalesce(ms.extract_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.payer_category, '0')) = upper(coalesce(ms.payer_category, '0'))
     AND upper(coalesce(mt.payer_category, '1')) = upper(coalesce(ms.payer_category, '1')))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (coalesce(mt.seq_no, 0) = coalesce(ms.seq_no, 0)
     AND coalesce(mt.seq_no, 1) = coalesce(ms.seq_no, 1))
AND (coalesce(mt.dw_last_update_date, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.row_count, 0) = coalesce(ms.row_count, 0)
     AND coalesce(mt.row_count, 1) = coalesce(ms.row_count, 1))
AND (coalesce(mt.expected_amt, NUMERIC '0') = coalesce(ms.expected_amt, NUMERIC '0')
     AND coalesce(mt.expected_amt, NUMERIC '1') = coalesce(ms.expected_amt, NUMERIC '1'))
AND (upper(coalesce(mt.new_appeal_flag, '0')) = upper(coalesce(ms.new_appeal_flag, '0'))
     AND upper(coalesce(mt.new_appeal_flag, '1')) = upper(coalesce(ms.new_appeal_flag, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (schema_id,
        mon_account_payer_id,
        pass_type,
        coid,
        account_no,
        ssc_id,
        rate_schedule_name,
        ssc_name,
        facility_name,
        unit_num,
        rate_schedule_eff_begin_date,
        rate_schedule_eff_end_date,
        patient_name,
        patient_dob,
        iplan_id,
        insurance_provider_name,
        payer_group_name,
        billing_name,
        billing_contact_person,
        authorization_code,
        payer_patient_id,
        payer_rank,
        pa_financial_class,
        payor_financial_class,
        accounting_period,
        major_payer_grp,
        reason_code,
        billing_status,
        pa_service_code,
        pa_account_status,
        cc_patient_type,
        pa_discharge_status,
        pa_patient_type,
        cancel_bill_ind,
        admit_source,
        admit_type,
        pa_drg,
        attending_physician_id,
        attending_physician_name,
        service_date_begin,
        discharge_date,
        comparison_method,
        project_name,
        work_queue_name,
        status_category_desc,
        status_desc,
        status_phase_desc,
        calc_date,
        total_charges,
        pa_actual_los,
        total_billed_charges,
        covered_charges,
        total_expected_payment,
        total_expected_adjustment,
        total_pt_responsibility_actual,
        total_variance_adjustment,
        total_payments,
        total_denial_amt,
        payor_due_amt,
        pa_total_account_bal,
        ar_amount,
        otd_amt_net,
        cash_adj_amt_net,
        otd_to_date_amount_mtd,
        cash_adj_amt_mtd,
        max_aplno,
        max_seqno,
        appeal_orig_amt,
        current_appealed_amt,
        current_appeal_balance,
        appeal_date_created,
        sequence_date_created,
        close_date,
        max_seq_deadline_date,
        sequence_creator,
        appeal_owner,
        appeal_modifier,
        disp_code,
        disp_desc,
        web_disp_code,
        web_disposition_type,
        root_code,
        root_cause_description,
        root_cause_dtl,
        external_appeal_code,
        apl_appeal_code,
        apl_appeal_desc,
        first_denial_date,
        denial_age,
        pa_denial_update_date,
        first_activity_create_date,
        last_activity_completion_date,
        last_activity_completion_age,
        last_user_activity_create_age,
        last_reason_change_date,
        last_status_change_date,
        last_project_change_date,
        last_owner_change_date,
        activity_due_date,
        activity_desc,
        activity_subject,
        activity_owner,
        appeal_sent_activity_ownr,
        appeal_initiation_date,
        appeal_sent_activity_date,
        appeal_sent_activity_age,
        last_status_change_age,
        activity_due_date_age,
        latest_seq_creation_date_age,
        latest_appeal_creation_date_age,
        appeal_deadline_days_remaining,
        appeal_sent_activity_subj,
        appeal_sent_activity_desc,
        extract_date,
        payer_category,
        source_system_code,
        seq_no,
        dw_last_update_date,
        ROW_COUNT,
        expected_amt,
        new_appeal_flag)
VALUES (ms.schema_id, ms.mon_account_payer_id, ms.pass_type, ms.coid, ms.account_no, ms.ssc_id, ms.rate_schedule_name, ms.ssc_name, ms.facility_name, ms.unit_num, ms.rate_schedule_eff_begin_date, ms.rate_schedule_eff_end_date, ms.patient_name, ms.patient_dob, ms.iplan_id, ms.insurance_provider_name, ms.payer_group_name, ms.billing_name, ms.billing_contact_person, ms.authorization_code, ms.payer_patient_id, ms.payer_rank, ms.pa_financial_class, ms.payor_financial_class, ms.accounting_period, ms.major_payer_grp, ms.reason_code, ms.billing_status, ms.pa_service_code, ms.pa_account_status, ms.cc_patient_type, ms.pa_discharge_status, ms.pa_patient_type, ms.cancel_bill_ind, ms.admit_source, ms.admit_type, ms.pa_drg, ms.attending_physician_id, ms.attending_physician_name, ms.service_date_begin, ms.discharge_date, ms.comparison_method, ms.project_name, ms.work_queue_name, ms.status_category_desc, ms.status_desc, ms.status_phase_desc, ms.calc_date, ms.total_charges, ms.pa_actual_los, ms.total_billed_charges, ms.covered_charges, ms.total_expected_payment, ms.total_expected_adjustment, ms.total_pt_responsibility_actual, ms.total_variance_adjustment, ms.total_payments, ms.total_denial_amt, ms.payor_due_amt, ms.pa_total_account_bal, ms.ar_amount, ms.otd_amt_net, ms.cash_adj_amt_net, ms.otd_to_date_amount_mtd, ms.cash_adj_amt_mtd, ms.max_aplno, ms.max_seqno, ms.appeal_orig_amt, ms.current_appealed_amt, ms.current_appeal_balance, ms.appeal_date_created, ms.sequence_date_created, ms.close_date, ms.max_seq_deadline_date, ms.sequence_creator, ms.appeal_owner, ms.appeal_modifier, ms.disp_code, ms.disp_desc, ms.web_disp_code, ms.web_disposition_type, ms.root_code, ms.root_cause_description, ms.root_cause_dtl, ms.external_appeal_code, ms.apl_appeal_code, ms.apl_appeal_desc, ms.first_denial_date, ms.denial_age, ms.pa_denial_update_date, ms.first_activity_create_date, ms.last_activity_completion_date, ms.last_activity_completion_age, ms.last_user_activity_create_age, ms.last_reason_change_date, ms.last_status_change_date, ms.last_project_change_date, ms.last_owner_change_date, ms.activity_due_date, ms.activity_desc, ms.activity_subject, ms.activity_owner, ms.appeal_sent_activity_ownr, ms.appeal_initiation_date, ms.appeal_sent_activity_date, ms.appeal_sent_activity_age, ms.last_status_change_age, ms.activity_due_date_age, ms.latest_seq_creation_date_age, ms.latest_appeal_creation_date_age, ms.appeal_deadline_days_remaining, ms.appeal_sent_activity_subj, ms.appeal_sent_activity_desc, ms.extract_date, ms.payer_category, ms.source_system_code, ms.seq_no, ms.dw_last_update_date, ms.row_count, ms.expected_amt, ms.new_appeal_flag);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT schema_id,
             coid,
             account_no,
             iplan_id,
             payer_rank,
             max_aplno,
             max_seqno,
             source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory
      GROUP BY schema_id,
               coid,
               account_no,
               iplan_id,
               payer_rank,
               max_aplno,
               max_seqno,
               source_system_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.edw_daily_denial_inventory');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;