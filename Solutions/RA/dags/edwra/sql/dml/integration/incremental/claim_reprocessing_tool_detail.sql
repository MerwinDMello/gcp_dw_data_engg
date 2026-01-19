DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/claim_reprocessing_tool_detail.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Pritam Tawale
      Name: Claim_Reprocessing_Tool_Detail BTEQ Script.
      Mod1: Creation of script on 08/22/17. PT.
****************************************************************************/ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.claim_reprocessing_tool_detail AS x USING
  (SELECT keys.patient_dw_id,
          crtd.log_id,
          crtd.coid,
          keys.company_code,
          substr(CAST(crtd.unit_num AS STRING), 1, 5) AS unit_num,
          crtd.account_no,
          crtd.request_type_desc,
          crtd.request_date,
          crtd.fin_class,
          crtd.last_activity_date,
          crtd.status_desc,
          crtd.disc_date,
          crtd.disc_source_desc,
          crtd.reimb_impact,
          crtd.repr_reasons,
          crtd.queue_name,
          crtd.extract_date,
          crtd.accountprefix AS claim_category_type,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'C' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.crt_detail AS crtd
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS keys ON upper(rtrim(crtd.coid)) = upper(rtrim(keys.coid))
   AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(crtd.account_no) AS FLOAT64) = keys.pat_acct_num
   WHERE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(crtd.account_no) AS FLOAT64) <> 0 ) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.crt_log_id = z.log_id WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    coid = substr(z.coid, 1, 5),
    unit_num = z.unit_num,
    pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN'),
    request_type_desc = z.request_type_desc,
    request_date_time = z.request_date,
    financial_class_code = substr(z.fin_class, 1, 3),
    last_activity_date_time = z.last_activity_date,
    status_desc = z.status_desc,
    discrepancy_date_time = z.disc_date,
    discrepancy_source_desc = z.disc_source_desc,
    reimbursement_impact_desc = z.reimb_impact,
    reprocess_reason_text = z.repr_reasons,
    queue_name = z.queue_name,
    extract_date_time = z.extract_date,
    claim_category_type = substr(z.claim_category_type, 1, 1),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'C' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        crt_log_id,
        coid,
        company_code,
        unit_num,
        pat_acct_num,
        request_type_desc,
        request_date_time,
        financial_class_code,
        last_activity_date_time,
        status_desc,
        discrepancy_date_time,
        discrepancy_source_desc,
        reimbursement_impact_desc,
        reprocess_reason_text,
        queue_name,
        extract_date_time,
        claim_category_type,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.log_id, substr(z.coid, 1, 5), z.company_code, z.unit_num, ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN'), z.request_type_desc, z.request_date, substr(z.fin_class, 1, 3), z.last_activity_date, z.status_desc, z.disc_date, z.disc_source_desc, z.reimb_impact, z.repr_reasons, z.queue_name, z.extract_date, substr(z.claim_category_type, 1, 1), datetime_trunc(current_datetime('US/Central'), SECOND), 'C');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             crt_log_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.claim_reprocessing_tool_detail
      GROUP BY patient_dw_id,
               crt_log_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.claim_reprocessing_tool_detail');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;