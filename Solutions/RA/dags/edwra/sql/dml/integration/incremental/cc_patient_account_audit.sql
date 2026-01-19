DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_patient_account_audit.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: PT
      Name: CC_Patient_Account_Audit - BTEQ Script.
      Mod1: Creation of script on 10/27/2023.
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA611;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.cc_patient_account_audit_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_stage_dataset_name }}.cc_patient_account_audit_stg AS mt USING
  (SELECT DISTINCT a.patient_dw_id,
                   po.payor_dw_id,
                   rccos.company_code AS company_code,
                   rccos.coid AS coid,
                   rccos.unit_num AS unit_num,
                   paa.pat_acct_num,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(paa.ins_plan), 1, 3), substr(trim(paa.ins_plan), 5, 2))) AS INT64) AS iplan_id,
                   paa.pat_acct_aud_id,
                   paa.mon_acct_pyr_id AS acct_payor_id,
                   paa.clm_num AS claim_num,
                   paa.rec_id AS record_id,
                   substr(paa.rec_type, 1, 20) AS record_type_txt,
                   paa.corp_ctrl AS corp_control_txt,
                   paa.ssc_ctrl AS ssc_control_txt,
                   paa.corp_stts_dt AS corp_status_date,
                   paa.aud_elig AS audit_eligibility_txt,
                   paa.aud_stts AS audit_status_txt,
                   paa.pre_aud_type AS pre_audit_type_txt,
                   paa.post_aud_type AS post_audit_type_txt,
                   substr(paa.othr_aud_type, 1, 4000) AS other_audit_type_desc,
                   paa.aud_stts_dt AS audit_status_date,
                   paa.pre_aud_drg AS prev_audit_drg_nm,
                   paa.post_aud_drg AS post_audit_drg_nm,
                   paa.pyr_of_aud AS payer_of_audit_txt,
                   paa.aud_schd_date AS audit_schedule_date,
                   paa.expt_aud_fees AS expt_audit_fee_amt,
                   paa.aud_fee_amt_coll AS act_audit_fee_coll_amt,
                   paa.aud_loc AS audit_location_txt,
                   paa.finl_aud_otcm AS final_audit_outcome_txt,
                   paa.wrk_agn_dt AS work_again_date,
                   paa.svc_strt_dt AS service_start_date,
                   paa.svc_end_dt AS service_end_date,
                   paa.corp_stts AS corp_status_desc,
                   substr(paa.addl_proj_fld, 1, 4000) AS addl_project_desc,
                   paa.fndg_ltr_rslt AS finding_letter_result_txt,
                   paa.intn_dt AS initiation_date,
                   paa.ltr_dt AS letter_date,
                   paa.rspn_dt AS response_date,
                   substr(paa.mr_trk_id, 1, 30) AS mr_trk_id,
                   paa.mr_req AS mr_req_txt,
                   paa.mr_rls_req_to_roi AS mr_rls_req_to_roi_date,
                   paa.mr_rls_meth AS mr_rls_method_txt,
                   paa.mr_rls_dt AS mr_rls_date,
                   paa.mis_mr_notf_dt AS mr_miss_notf_date,
                   paa.mis_doc_req AS miss_doc_req_txt,
                   substr(paa.pyr_drg, 1, 5) AS payer_drg_code,
                   paa.pyr_ref_id AS payer_ref_id,
                   paa.dlr_at_risk AS risk_amt,
                   paa.dcrp_amt AS disc_amt,
                   paa.rfnd_req_amt AS refund_req_amt,
                   paa.rfnd_amt AS refund_amt,
                   paa.rfnd_req_rsn AS refund_req_reason_txt,
                   paa.rfnd_stts AS refund_status_txt,
                   paa.rfnd_stts_dt AS refund_status_date,
                   paa.finl_rfnd_otcm AS final_refund_outcome_txt,
                   paa.creation_dt AS creation_date,
                   paa.creation_user AS creation_user_id,
                   paa.modification_dt AS modification_date,
                   paa.modification_user AS modification_user_id,
                   substr(paa.map_to_rec_type, 1, 10) AS map_to_record_type_txt,
                   CAST(paa.map_to_rec_id AS INT64) AS map_to_record_id,
                   paa.rfnd_prcs_tool_stts AS refund_prcs_tool_status_txt,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.pat_acct_aud AS paa
   INNER JOIN   {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = paa.schema_id
   AND upper(rtrim(rccos.unit_num)) = upper(rtrim(paa.unit_num))
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = paa.pat_acct_num
   INNER JOIN   {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(po.company_code)) = upper(rtrim(rccos.company_code))
   AND po.iplan_id = CAST( bqutil_fns.cw_td_normalize_number(concat(substr(trim(paa.ins_plan), 1, 3), substr(trim(paa.ins_plan), 5, 2))) AS INT64)) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.pat_acct_audit_id, NUMERIC '0') = coalesce(ms.pat_acct_aud_id, NUMERIC '0')
     AND coalesce(mt.pat_acct_audit_id, NUMERIC '1') = coalesce(ms.pat_acct_aud_id, NUMERIC '1'))
AND (coalesce(mt.acct_payor_id, NUMERIC '0') = coalesce(ms.acct_payor_id, NUMERIC '0')
     AND coalesce(mt.acct_payor_id, NUMERIC '1') = coalesce(ms.acct_payor_id, NUMERIC '1'))
AND (upper(coalesce(mt.claim_num, '0')) = upper(coalesce(ms.claim_num, '0'))
     AND upper(coalesce(mt.claim_num, '1')) = upper(coalesce(ms.claim_num, '1')))
AND (coalesce(mt.record_id, 0) = coalesce(ms.record_id, 0)
     AND coalesce(mt.record_id, 1) = coalesce(ms.record_id, 1))
AND (upper(coalesce(mt.record_type_txt, '0')) = upper(coalesce(ms.record_type_txt, '0'))
     AND upper(coalesce(mt.record_type_txt, '1')) = upper(coalesce(ms.record_type_txt, '1')))
AND (upper(coalesce(mt.corp_control_txt, '0')) = upper(coalesce(ms.corp_control_txt, '0'))
     AND upper(coalesce(mt.corp_control_txt, '1')) = upper(coalesce(ms.corp_control_txt, '1')))
AND (upper(coalesce(mt.ssc_control_txt, '0')) = upper(coalesce(ms.ssc_control_txt, '0'))
     AND upper(coalesce(mt.ssc_control_txt, '1')) = upper(coalesce(ms.ssc_control_txt, '1')))
AND (coalesce(mt.corp_status_date, DATE '1970-01-01') = coalesce(ms.corp_status_date, DATE '1970-01-01')
     AND coalesce(mt.corp_status_date, DATE '1970-01-02') = coalesce(ms.corp_status_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.audit_eligibility_txt, '0')) = upper(coalesce(ms.audit_eligibility_txt, '0'))
     AND upper(coalesce(mt.audit_eligibility_txt, '1')) = upper(coalesce(ms.audit_eligibility_txt, '1')))
AND (upper(coalesce(mt.audit_status_txt, '0')) = upper(coalesce(ms.audit_status_txt, '0'))
     AND upper(coalesce(mt.audit_status_txt, '1')) = upper(coalesce(ms.audit_status_txt, '1')))
AND (upper(coalesce(mt.pre_audit_type_txt, '0')) = upper(coalesce(ms.pre_audit_type_txt, '0'))
     AND upper(coalesce(mt.pre_audit_type_txt, '1')) = upper(coalesce(ms.pre_audit_type_txt, '1')))
AND (upper(coalesce(mt.post_audit_type_txt, '0')) = upper(coalesce(ms.post_audit_type_txt, '0'))
     AND upper(coalesce(mt.post_audit_type_txt, '1')) = upper(coalesce(ms.post_audit_type_txt, '1')))
AND (upper(coalesce(mt.other_audit_type_desc, '0')) = upper(coalesce(ms.other_audit_type_desc, '0'))
     AND upper(coalesce(mt.other_audit_type_desc, '1')) = upper(coalesce(ms.other_audit_type_desc, '1')))
AND (coalesce(mt.audit_status_date, DATE '1970-01-01') = coalesce(ms.audit_status_date, DATE '1970-01-01')
     AND coalesce(mt.audit_status_date, DATE '1970-01-02') = coalesce(ms.audit_status_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.prev_audit_drg_nm, '0')) = upper(coalesce(ms.prev_audit_drg_nm, '0'))
     AND upper(coalesce(mt.prev_audit_drg_nm, '1')) = upper(coalesce(ms.prev_audit_drg_nm, '1')))
AND (upper(coalesce(mt.post_audit_drg_nm, '0')) = upper(coalesce(ms.post_audit_drg_nm, '0'))
     AND upper(coalesce(mt.post_audit_drg_nm, '1')) = upper(coalesce(ms.post_audit_drg_nm, '1')))
AND (upper(coalesce(mt.payer_of_audit_txt, '0')) = upper(coalesce(ms.payer_of_audit_txt, '0'))
     AND upper(coalesce(mt.payer_of_audit_txt, '1')) = upper(coalesce(ms.payer_of_audit_txt, '1')))
AND (coalesce(mt.audit_schedule_date, DATE '1970-01-01') = coalesce(ms.audit_schedule_date, DATE '1970-01-01')
     AND coalesce(mt.audit_schedule_date, DATE '1970-01-02') = coalesce(ms.audit_schedule_date, DATE '1970-01-02'))
AND (coalesce(mt.expt_audit_fee_amt, NUMERIC '0') = coalesce(ms.expt_audit_fee_amt, NUMERIC '0')
     AND coalesce(mt.expt_audit_fee_amt, NUMERIC '1') = coalesce(ms.expt_audit_fee_amt, NUMERIC '1'))
AND (coalesce(mt.act_audit_fee_coll_amt, NUMERIC '0') = coalesce(ms.act_audit_fee_coll_amt, NUMERIC '0')
     AND coalesce(mt.act_audit_fee_coll_amt, NUMERIC '1') = coalesce(ms.act_audit_fee_coll_amt, NUMERIC '1'))
AND (upper(coalesce(mt.audit_location_txt, '0')) = upper(coalesce(ms.audit_location_txt, '0'))
     AND upper(coalesce(mt.audit_location_txt, '1')) = upper(coalesce(ms.audit_location_txt, '1')))
AND (upper(coalesce(mt.final_audit_outcome_txt, '0')) = upper(coalesce(ms.final_audit_outcome_txt, '0'))
     AND upper(coalesce(mt.final_audit_outcome_txt, '1')) = upper(coalesce(ms.final_audit_outcome_txt, '1')))
AND (coalesce(mt.work_again_date, DATE '1970-01-01') = coalesce(ms.work_again_date, DATE '1970-01-01')
     AND coalesce(mt.work_again_date, DATE '1970-01-02') = coalesce(ms.work_again_date, DATE '1970-01-02'))
AND (coalesce(mt.service_start_date, DATE '1970-01-01') = coalesce(ms.service_start_date, DATE '1970-01-01')
     AND coalesce(mt.service_start_date, DATE '1970-01-02') = coalesce(ms.service_start_date, DATE '1970-01-02'))
AND (coalesce(mt.service_end_date, DATE '1970-01-01') = coalesce(ms.service_end_date, DATE '1970-01-01')
     AND coalesce(mt.service_end_date, DATE '1970-01-02') = coalesce(ms.service_end_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.corp_status_desc, '0')) = upper(coalesce(ms.corp_status_desc, '0'))
     AND upper(coalesce(mt.corp_status_desc, '1')) = upper(coalesce(ms.corp_status_desc, '1')))
AND (upper(coalesce(mt.addl_project_desc, '0')) = upper(coalesce(ms.addl_project_desc, '0'))
     AND upper(coalesce(mt.addl_project_desc, '1')) = upper(coalesce(ms.addl_project_desc, '1')))
AND (upper(coalesce(mt.finding_letter_result_txt, '0')) = upper(coalesce(ms.finding_letter_result_txt, '0'))
     AND upper(coalesce(mt.finding_letter_result_txt, '1')) = upper(coalesce(ms.finding_letter_result_txt, '1')))
AND (coalesce(mt.initiation_date, DATE '1970-01-01') = coalesce(ms.initiation_date, DATE '1970-01-01')
     AND coalesce(mt.initiation_date, DATE '1970-01-02') = coalesce(ms.initiation_date, DATE '1970-01-02'))
AND (coalesce(mt.letter_date, DATE '1970-01-01') = coalesce(ms.letter_date, DATE '1970-01-01')
     AND coalesce(mt.letter_date, DATE '1970-01-02') = coalesce(ms.letter_date, DATE '1970-01-02'))
AND (coalesce(mt.response_date, DATE '1970-01-01') = coalesce(ms.response_date, DATE '1970-01-01')
     AND coalesce(mt.response_date, DATE '1970-01-02') = coalesce(ms.response_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.mr_trk_id, '0')) = upper(coalesce(ms.mr_trk_id, '0'))
     AND upper(coalesce(mt.mr_trk_id, '1')) = upper(coalesce(ms.mr_trk_id, '1')))
AND (upper(coalesce(mt.mr_req_txt, '0')) = upper(coalesce(ms.mr_req_txt, '0'))
     AND upper(coalesce(mt.mr_req_txt, '1')) = upper(coalesce(ms.mr_req_txt, '1')))
AND (coalesce(mt.mr_rls_req_to_roi_date, DATE '1970-01-01') = coalesce(ms.mr_rls_req_to_roi_date, DATE '1970-01-01')
     AND coalesce(mt.mr_rls_req_to_roi_date, DATE '1970-01-02') = coalesce(ms.mr_rls_req_to_roi_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.mr_rls_method_txt, '0')) = upper(coalesce(ms.mr_rls_method_txt, '0'))
     AND upper(coalesce(mt.mr_rls_method_txt, '1')) = upper(coalesce(ms.mr_rls_method_txt, '1')))
AND (coalesce(mt.mr_rls_date, DATE '1970-01-01') = coalesce(ms.mr_rls_date, DATE '1970-01-01')
     AND coalesce(mt.mr_rls_date, DATE '1970-01-02') = coalesce(ms.mr_rls_date, DATE '1970-01-02'))
AND (coalesce(mt.mr_miss_notf_date, DATE '1970-01-01') = coalesce(ms.mr_miss_notf_date, DATE '1970-01-01')
     AND coalesce(mt.mr_miss_notf_date, DATE '1970-01-02') = coalesce(ms.mr_miss_notf_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.miss_doc_req_txt, '0')) = upper(coalesce(ms.miss_doc_req_txt, '0'))
     AND upper(coalesce(mt.miss_doc_req_txt, '1')) = upper(coalesce(ms.miss_doc_req_txt, '1')))
AND (upper(coalesce(mt.payer_drg_code, '0')) = upper(coalesce(ms.payer_drg_code, '0'))
     AND upper(coalesce(mt.payer_drg_code, '1')) = upper(coalesce(ms.payer_drg_code, '1')))
AND (upper(coalesce(mt.payer_ref_id, '0')) = upper(coalesce(ms.payer_ref_id, '0'))
     AND upper(coalesce(mt.payer_ref_id, '1')) = upper(coalesce(ms.payer_ref_id, '1')))
AND (coalesce(mt.risk_amt, NUMERIC '0') = coalesce(ms.risk_amt, NUMERIC '0')
     AND coalesce(mt.risk_amt, NUMERIC '1') = coalesce(ms.risk_amt, NUMERIC '1'))
AND (coalesce(mt.disc_amt, NUMERIC '0') = coalesce(ms.disc_amt, NUMERIC '0')
     AND coalesce(mt.disc_amt, NUMERIC '1') = coalesce(ms.disc_amt, NUMERIC '1'))
AND (coalesce(mt.refund_req_amt, NUMERIC '0') = coalesce(ms.refund_req_amt, NUMERIC '0')
     AND coalesce(mt.refund_req_amt, NUMERIC '1') = coalesce(ms.refund_req_amt, NUMERIC '1'))
AND (coalesce(mt.refund_amt, NUMERIC '0') = coalesce(ms.refund_amt, NUMERIC '0')
     AND coalesce(mt.refund_amt, NUMERIC '1') = coalesce(ms.refund_amt, NUMERIC '1'))
AND (upper(coalesce(mt.refund_req_reason_txt, '0')) = upper(coalesce(ms.refund_req_reason_txt, '0'))
     AND upper(coalesce(mt.refund_req_reason_txt, '1')) = upper(coalesce(ms.refund_req_reason_txt, '1')))
AND (upper(coalesce(mt.refund_status_txt, '0')) = upper(coalesce(ms.refund_status_txt, '0'))
     AND upper(coalesce(mt.refund_status_txt, '1')) = upper(coalesce(ms.refund_status_txt, '1')))
AND (coalesce(mt.refund_status_date, DATE '1970-01-01') = coalesce(ms.refund_status_date, DATE '1970-01-01')
     AND coalesce(mt.refund_status_date, DATE '1970-01-02') = coalesce(ms.refund_status_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.final_refund_outcome_txt, '0')) = upper(coalesce(ms.final_refund_outcome_txt, '0'))
     AND upper(coalesce(mt.final_refund_outcome_txt, '1')) = upper(coalesce(ms.final_refund_outcome_txt, '1')))
AND (coalesce(mt.creation_date, DATE '1970-01-01') = coalesce(ms.creation_date, DATE '1970-01-01')
     AND coalesce(mt.creation_date, DATE '1970-01-02') = coalesce(ms.creation_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.creation_user_id, '0')) = upper(coalesce(ms.creation_user_id, '0'))
     AND upper(coalesce(mt.creation_user_id, '1')) = upper(coalesce(ms.creation_user_id, '1')))
AND (coalesce(mt.modification_date, DATE '1970-01-01') = coalesce(ms.modification_date, DATE '1970-01-01')
     AND coalesce(mt.modification_date, DATE '1970-01-02') = coalesce(ms.modification_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.modification_user_id, '0')) = upper(coalesce(ms.modification_user_id, '0'))
     AND upper(coalesce(mt.modification_user_id, '1')) = upper(coalesce(ms.modification_user_id, '1')))
AND (upper(coalesce(mt.map_to_record_type_txt, '0')) = upper(coalesce(ms.map_to_record_type_txt, '0'))
     AND upper(coalesce(mt.map_to_record_type_txt, '1')) = upper(coalesce(ms.map_to_record_type_txt, '1')))
AND (coalesce(mt.map_to_record_id, 0) = coalesce(ms.map_to_record_id, 0)
     AND coalesce(mt.map_to_record_id, 1) = coalesce(ms.map_to_record_id, 1))
AND (upper(coalesce(mt.refund_prcs_tool_status_txt, '0')) = upper(coalesce(ms.refund_prcs_tool_status_txt, '0'))
     AND upper(coalesce(mt.refund_prcs_tool_status_txt, '1')) = upper(coalesce(ms.refund_prcs_tool_status_txt, '1')))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        payor_dw_id,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        iplan_id,
        pat_acct_audit_id,
        acct_payor_id,
        claim_num,
        record_id,
        record_type_txt,
        corp_control_txt,
        ssc_control_txt,
        corp_status_date,
        audit_eligibility_txt,
        audit_status_txt,
        pre_audit_type_txt,
        post_audit_type_txt,
        other_audit_type_desc,
        audit_status_date,
        prev_audit_drg_nm,
        post_audit_drg_nm,
        payer_of_audit_txt,
        audit_schedule_date,
        expt_audit_fee_amt,
        act_audit_fee_coll_amt,
        audit_location_txt,
        final_audit_outcome_txt,
        work_again_date,
        service_start_date,
        service_end_date,
        corp_status_desc,
        addl_project_desc,
        finding_letter_result_txt,
        initiation_date,
        letter_date,
        response_date,
        mr_trk_id,
        mr_req_txt,
        mr_rls_req_to_roi_date,
        mr_rls_method_txt,
        mr_rls_date,
        mr_miss_notf_date,
        miss_doc_req_txt,
        payer_drg_code,
        payer_ref_id,
        risk_amt,
        disc_amt,
        refund_req_amt,
        refund_amt,
        refund_req_reason_txt,
        refund_status_txt,
        refund_status_date,
        final_refund_outcome_txt,
        creation_date,
        creation_user_id,
        modification_date,
        modification_user_id,
        map_to_record_type_txt,
        map_to_record_id,
        refund_prcs_tool_status_txt,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_dw_id, ms.payor_dw_id, ms.company_code, ms.coid, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.pat_acct_aud_id, ms.acct_payor_id, ms.claim_num, ms.record_id, ms.record_type_txt, ms.corp_control_txt, ms.ssc_control_txt, ms.corp_status_date, ms.audit_eligibility_txt, ms.audit_status_txt, ms.pre_audit_type_txt, ms.post_audit_type_txt, ms.other_audit_type_desc, ms.audit_status_date, ms.prev_audit_drg_nm, ms.post_audit_drg_nm, ms.payer_of_audit_txt, ms.audit_schedule_date, ms.expt_audit_fee_amt, ms.act_audit_fee_coll_amt, ms.audit_location_txt, ms.final_audit_outcome_txt, ms.work_again_date, ms.service_start_date, ms.service_end_date, ms.corp_status_desc, ms.addl_project_desc, ms.finding_letter_result_txt, ms.initiation_date, ms.letter_date, ms.response_date, ms.mr_trk_id, ms.mr_req_txt, ms.mr_rls_req_to_roi_date, ms.mr_rls_method_txt, ms.mr_rls_date, ms.mr_miss_notf_date, ms.miss_doc_req_txt, ms.payer_drg_code, ms.payer_ref_id, ms.risk_amt, ms.disc_amt, ms.refund_req_amt, ms.refund_amt, ms.refund_req_reason_txt, ms.refund_status_txt, ms.refund_status_date, ms.final_refund_outcome_txt, ms.creation_date, ms.creation_user_id, ms.modification_date, ms.modification_user_id, ms.map_to_record_type_txt, ms.map_to_record_id, ms.refund_prcs_tool_status_txt, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             payor_dw_id,
             pat_acct_audit_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_patient_account_audit_stg
      GROUP BY patient_dw_id,
               payor_dw_id,
               pat_acct_audit_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.cc_patient_account_audit_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Patient_Account_Audit_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;
SET srctableid = Null;
SET srctablename = 'edwra_staging.pat_acct_aud';
SET tgttablename = 'edwra.cc_patient_account_audit';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) FROM  {{ params.param_parallon_ra_stage_dataset_name }}.pat_acct_aud AS paa
   INNER JOIN   {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = paa.schema_id
   AND upper(rtrim(rccos.unit_num)) = upper(rtrim(paa.unit_num))
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = paa.pat_acct_num
   INNER JOIN   {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(po.company_code)) = upper(rtrim(rccos.company_code))
   AND po.iplan_id = CAST( bqutil_fns.cw_td_normalize_number(concat(substr(trim(paa.ins_plan), 1, 3), substr(trim(paa.ins_plan), 5, 2))) AS INT64)
);

SET actual_value =
(
select count(*) as row_count
from  {{ params.param_parallon_ra_stage_dataset_name }}.cc_patient_account_audit_stg
);

SET difference = 
CASE WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64) 
     WHEN expected_value = 0 and actual_value = 0 Then 0 
	 ELSE actual_value
END;

SET audit_status = CASE WHEN difference <= 0 THEN "PASS" ELSE "FAIL" END;

INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
(uuid, table_id, src_sys_nm, src_tbl_nm, tgt_tbl_nm, audit_type, 
expected_value, actual_value, load_start_time, load_end_time, 
load_run_time, job_name, audit_time, audit_status)
VALUES
(GENERATE_UUID(), cast(srctableid as int64), 'ra',srctablename, tgttablename, audit_type,
expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
tableload_run_time, job_name, audit_time, audit_status
);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO   {{ params.param_parallon_ra_core_dataset_name }}.cc_patient_account_audit AS x USING
  (SELECT cc_patient_account_audit_stg.*
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_patient_account_audit_stg) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.pat_acct_audit_id = z.pat_acct_audit_id
AND upper(rtrim(x.unit_num)) = upper(rtrim(z.unit_num)) WHEN MATCHED THEN
UPDATE
SET pat_acct_num = z.pat_acct_num,
    iplan_id = z.iplan_id,
    acct_payor_id = z.acct_payor_id,
    claim_num = z.claim_num,
    record_id = z.record_id,
    record_type_txt = z.record_type_txt,
    corp_control_txt = z.corp_control_txt,
    ssc_control_txt = z.ssc_control_txt,
    corp_status_date = z.corp_status_date,
    audit_eligibility_txt = z.audit_eligibility_txt,
    audit_status_txt = z.audit_status_txt,
    pre_audit_type_txt = z.pre_audit_type_txt,
    post_audit_type_txt = z.post_audit_type_txt,
    other_audit_type_desc = z.other_audit_type_desc,
    audit_status_date = z.audit_status_date,
    prev_audit_drg_nm = z.prev_audit_drg_nm,
    post_audit_drg_nm = z.post_audit_drg_nm,
    payer_of_audit_txt = z.payer_of_audit_txt,
    audit_schedule_date = z.audit_schedule_date,
    expt_audit_fee_amt = z.expt_audit_fee_amt,
    act_audit_fee_coll_amt = z.act_audit_fee_coll_amt,
    audit_location_txt = z.audit_location_txt,
    final_audit_outcome_txt = z.final_audit_outcome_txt,
    work_again_date = z.work_again_date,
    service_start_date = z.service_start_date,
    service_end_date = z.service_end_date,
    corp_status_desc = z.corp_status_desc,
    addl_project_desc = z.addl_project_desc,
    finding_letter_result_txt = z.finding_letter_result_txt,
    initiation_date = z.initiation_date,
    letter_date = z.letter_date,
    response_date = z.response_date,
    mr_trk_id = z.mr_trk_id,
    mr_req_txt = z.mr_req_txt,
    mr_rls_req_to_roi_date = z.mr_rls_req_to_roi_date,
    mr_rls_method_txt = z.mr_rls_method_txt,
    mr_rls_date = z.mr_rls_date,
    mr_miss_notf_date = z.mr_miss_notf_date,
    miss_doc_req_txt = z.miss_doc_req_txt,
    payer_drg_code = z.payer_drg_code,
    payer_ref_id = z.payer_ref_id,
    risk_amt = z.risk_amt,
    disc_amt = z.disc_amt,
    refund_req_amt = z.refund_req_amt,
    refund_amt = z.refund_amt,
    refund_req_reason_txt = z.refund_req_reason_txt,
    refund_status_txt = z.refund_status_txt,
    refund_status_date = z.refund_status_date,
    final_refund_outcome_txt = z.final_refund_outcome_txt,
    creation_date = z.creation_date,
    creation_user_id = z.creation_user_id,
    modification_date = z.modification_date,
    modification_user_id = z.modification_user_id,
    map_to_record_type_txt = z.map_to_record_type_txt,
    map_to_record_id = z.map_to_record_id,
    refund_prcs_tool_status_txt = z.refund_prcs_tool_status_txt,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        payor_dw_id,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        iplan_id,
        pat_acct_audit_id,
        acct_payor_id,
        claim_num,
        record_id,
        record_type_txt,
        corp_control_txt,
        ssc_control_txt,
        corp_status_date,
        audit_eligibility_txt,
        audit_status_txt,
        pre_audit_type_txt,
        post_audit_type_txt,
        other_audit_type_desc,
        audit_status_date,
        prev_audit_drg_nm,
        post_audit_drg_nm,
        payer_of_audit_txt,
        audit_schedule_date,
        expt_audit_fee_amt,
        act_audit_fee_coll_amt,
        audit_location_txt,
        final_audit_outcome_txt,
        work_again_date,
        service_start_date,
        service_end_date,
        corp_status_desc,
        addl_project_desc,
        finding_letter_result_txt,
        initiation_date,
        letter_date,
        response_date,
        mr_trk_id,
        mr_req_txt,
        mr_rls_req_to_roi_date,
        mr_rls_method_txt,
        mr_rls_date,
        mr_miss_notf_date,
        miss_doc_req_txt,
        payer_drg_code,
        payer_ref_id,
        risk_amt,
        disc_amt,
        refund_req_amt,
        refund_amt,
        refund_req_reason_txt,
        refund_status_txt,
        refund_status_date,
        final_refund_outcome_txt,
        creation_date,
        creation_user_id,
        modification_date,
        modification_user_id,
        map_to_record_type_txt,
        map_to_record_id,
        refund_prcs_tool_status_txt,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.patient_dw_id, z.payor_dw_id, z.company_code, z.coid, z.unit_num, z.pat_acct_num, z.iplan_id, z.pat_acct_audit_id, z.acct_payor_id, z.claim_num, z.record_id, z.record_type_txt, z.corp_control_txt, z.ssc_control_txt, z.corp_status_date, z.audit_eligibility_txt, z.audit_status_txt, z.pre_audit_type_txt, z.post_audit_type_txt, z.other_audit_type_desc, z.audit_status_date, z.prev_audit_drg_nm, z.post_audit_drg_nm, z.payer_of_audit_txt, z.audit_schedule_date, z.expt_audit_fee_amt, z.act_audit_fee_coll_amt, z.audit_location_txt, z.final_audit_outcome_txt, z.work_again_date, z.service_start_date, z.service_end_date, z.corp_status_desc, z.addl_project_desc, z.finding_letter_result_txt, z.initiation_date, z.letter_date, z.response_date, z.mr_trk_id, z.mr_req_txt, z.mr_rls_req_to_roi_date, z.mr_rls_method_txt, z.mr_rls_date, z.mr_miss_notf_date, z.miss_doc_req_txt, z.payer_drg_code, z.payer_ref_id, z.risk_amt, z.disc_amt, z.refund_req_amt, z.refund_amt, z.refund_req_reason_txt, z.refund_status_txt, z.refund_status_date, z.final_refund_outcome_txt, z.creation_date, z.creation_user_id, z.modification_date, z.modification_user_id, z.map_to_record_type_txt, z.map_to_record_id, z.refund_prcs_tool_status_txt, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             payor_dw_id,
             pat_acct_audit_id
      FROM   {{ params.param_parallon_ra_core_dataset_name }}.cc_patient_account_audit
      GROUP BY patient_dw_id,
               payor_dw_id,
               pat_acct_audit_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_core_dataset_name }}`.cc_patient_account_audit');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM   {{ params.param_parallon_ra_core_dataset_name }}.cc_patient_account_audit
WHERE upper(TRIM(cc_patient_account_audit.coid)) IN
    (SELECT upper(r.coid) AS coid
     FROM   {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_patient_account_audit.dw_last_update_date_time <>
    (SELECT max(cc_patient_account_audit_0.dw_last_update_date_time)
     FROM   {{ params.param_parallon_ra_core_dataset_name }}.cc_patient_account_audit AS cc_patient_account_audit_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Patient_Account_Audit');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;