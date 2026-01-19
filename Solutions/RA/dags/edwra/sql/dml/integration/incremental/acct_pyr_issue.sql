DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/acct_pyr_issue.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: PT
      Name: CC_Account_Payor_Issue - BTEQ Script.
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA514;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor_issue AS x USING
  (SELECT b.patient_dw_id,
          c.payor_dw_id,
          api.insurance_order_num,
          rccos.company_code,
          rccos.coid,
          rccos.unit_num,
          api.pat_acct_num AS pat_acct_num,
          api.iplan_id,
          api.acct_pyr_issue_id AS acct_payer_issue_id,
          api.acct_pyr_id AS acct_payer_id,
          api.issue_id AS issue_id,
          api.issue_rank AS issue_rank_num,
          api.creation_dt AS create_date,
          api.creation_user AS create_user_name,
          api.modification_dt AS update_date,
          api.modification_user AS update_user_name,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.acct_pyr_issue AS api
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.org_id = api.org_id
   AND rccos.schema_id = api.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS b ON upper(rtrim(rccos.coid)) = upper(rtrim(b.coid))
   AND api.pat_acct_num = b.pat_acct_num
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS c ON upper(rtrim(rccos.coid)) = upper(rtrim(c.coid))
   AND upper(rtrim(rccos.company_code)) = upper(rtrim(c.company_code))
   AND api.iplan_id = c.iplan_id) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.insurance_order_num = z.insurance_order_num
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.acct_payer_issue_id = z.acct_payer_issue_id WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_id = z.iplan_id,
    issue_rank_num = z.issue_rank_num,
    acct_payer_id = z.acct_payer_id,
    issue_id = z.issue_id,
    create_date = z.create_date,
    create_user_name = z.create_user_name,
    update_date = z.update_date,
    update_user_name = z.update_user_name,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        payor_dw_id,
        insurance_order_num,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        iplan_id,
        acct_payer_issue_id,
        acct_payer_id,
        issue_id,
        issue_rank_num,
        create_date,
        create_user_name,
        update_date,
        update_user_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.patient_dw_id, z.payor_dw_id, z.insurance_order_num, z.company_code, z.coid, z.unit_num, z.pat_acct_num, z.iplan_id, z.acct_payer_issue_id, z.acct_payer_id, z.issue_id, z.issue_rank_num, z.create_date, z.create_user_name, z.update_date, z.update_user_name, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             payor_dw_id,
             insurance_order_num,
             coid,
             acct_payer_issue_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor_issue
      GROUP BY patient_dw_id,
               payor_dw_id,
               insurance_order_num,
               coid,
               acct_payer_issue_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor_issue');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Account_Payor_Issue');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;