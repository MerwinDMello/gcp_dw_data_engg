DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_account_occur_span_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: CC_Account_Occur_Span_Code - BTEQ Script.
      Mod1: Creation of script on 8/11/2011. SW.
      Mod2: Script changed due to new DDL on 9/6/2011. SW.
      Mod3: Script changed due to new tables for 9.7 on 3/6/2012 SW
      Mod4: Added Diagnostics per Teradata for long running queries on 6/30/2014 FY.
      Mod5: Tuned SQL for performance. Removed diagnostics on 11/5/2014 SW.
      Mod6: Removed CAST on Patient Account Number on 1/13/2015. AS
      Mod7: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA226;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_core_dataset_name }}.cc_account_occur_span_code;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_account_occur_span_code AS x USING
  (SELECT a.patient_dw_id,
          CAST(mancc.payer_rank AS INT64) AS occur_span_code_seq,
          reforg.company_code AS company_cd,
          CASE
              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
              ELSE og.client_id
          END AS coido,
          og.short_name AS unit_num,
          ma.account_no AS pat_acct_nbr,
          mancc.code AS occur_span_code,
          mancc.code_date AS occur_span_from_date,
          mancc.code_date_to AS occur_span_to_date
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_nonclinical_code AS mancc
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON ma.id = mancc.mon_account_id
   AND ma.schema_id = mancc.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.schema_id = og.schema_id
   AND ma.org_id_provider = og.org_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv ON mancc.code_category = pv.id
   AND mancc.schema_id = pv.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON reforg.org_id = og.org_id
   AND reforg.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ma.account_no
   WHERE upper(rtrim(upper(pv.group_id))) = 'NONCLINICAL_CODE_CATEGORY'
     AND upper(rtrim(upper(pv.display_text))) = 'OCCURRENCE SPAN CODES' ) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.occur_span_code_seq = z.occur_span_code_seq WHEN MATCHED THEN
UPDATE
SET company_code = z.company_cd,
    coid = substr(z.coido, 1, 5),
    unit_num = substr(z.unit_num, 1, 5),
    pat_acct_num = z.pat_acct_nbr,
    occur_span_code = substr(z.occur_span_code, 1, 2),
    occur_span_from_date = z.occur_span_from_date,
    occur_span_to_date = z.occur_span_to_date,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        occur_span_code_seq,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        occur_span_code,
        occur_span_from_date,
        occur_span_to_date,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.occur_span_code_seq, z.company_cd, substr(z.coido, 1, 5), substr(z.unit_num, 1, 5), z.pat_acct_nbr, substr(z.occur_span_code, 1, 2), z.occur_span_from_date, z.occur_span_to_date, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             occur_span_code_seq
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_occur_span_code
      GROUP BY patient_dw_id,
               occur_span_code_seq
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account_occur_span_code');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Account_Occur_Span_Code');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;