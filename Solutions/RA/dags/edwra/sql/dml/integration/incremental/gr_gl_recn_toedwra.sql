DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/gr_gl_recn_tora_edwra.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/GR_GL_RECN_ToEDWRA.out;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/*
Mod1:Added columns Fourth_Day_Delay_Flag,Interim_Bill_Flag as part of  PBI 12501 - Enhancements to GL Recon PT 10/1/2018
*/ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_core_dataset_name }}.gr_gl_recn;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.gr_gl_recn AS mt USING
  (SELECT DISTINCT substr(coalesce(ros.company_code, 'H'), 1, 1) AS company_code,
                   grec.coid AS coid,
                   grec.cost_report_year_begin,
                   grec.pat_acct_num,
                   grec.gl_acct_num AS gl_acct_num,
                   grec.pat_last_name,
                   grec.admission_date,
                   grec.discharge_date,
                   grec.cost_report_year_end,
                   grec.log_type,
                   grec.total_gl_posted_acct_amt,
                   grec.total_ca_log_24_adj_amt,
                   grec.total_ca_log_65_adj_amt,
                   substr(coalesce(grec.log_24_section_name, 'NA'), 1, 25) AS log_24_section_name,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   substr(coalesce(ros.source_system_code, 'N'), 1, 1) AS source_system_code,
                   grec.fourth_day_delay_flag AS fourth_day_delay_ind,
                   grec.interim_bill_flag AS interim_bill_ind
   FROM {{ params.param_parallon_ra_stage_dataset_name}}.gr_gl_recn AS grec
   LEFT OUTER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON upper(rtrim(grec.coid)) = upper(rtrim(ros.coid))) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.cost_report_year_begin, DATE '1970-01-01') = coalesce(ms.cost_report_year_begin, DATE '1970-01-01')
     AND coalesce(mt.cost_report_year_begin, DATE '1970-01-02') = coalesce(ms.cost_report_year_begin, DATE '1970-01-02'))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (upper(coalesce(mt.gl_acct_num, '0')) = upper(coalesce(ms.gl_acct_num, '0'))
     AND upper(coalesce(mt.gl_acct_num, '1')) = upper(coalesce(ms.gl_acct_num, '1')))
AND (upper(coalesce(mt.pat_last_name, '0')) = upper(coalesce(ms.pat_last_name, '0'))
     AND upper(coalesce(mt.pat_last_name, '1')) = upper(coalesce(ms.pat_last_name, '1')))
AND (coalesce(mt.admission_date, DATE '1970-01-01') = coalesce(ms.admission_date, DATE '1970-01-01')
     AND coalesce(mt.admission_date, DATE '1970-01-02') = coalesce(ms.admission_date, DATE '1970-01-02'))
AND (coalesce(mt.discharge_date, DATE '1970-01-01') = coalesce(ms.discharge_date, DATE '1970-01-01')
     AND coalesce(mt.discharge_date, DATE '1970-01-02') = coalesce(ms.discharge_date, DATE '1970-01-02'))
AND (coalesce(mt.cost_report_year_end, DATE '1970-01-01') = coalesce(ms.cost_report_year_end, DATE '1970-01-01')
     AND coalesce(mt.cost_report_year_end, DATE '1970-01-02') = coalesce(ms.cost_report_year_end, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_type, '0')) = upper(coalesce(ms.log_type, '0'))
     AND upper(coalesce(mt.log_type, '1')) = upper(coalesce(ms.log_type, '1')))
AND (coalesce(mt.total_gl_posted_acct_amt, NUMERIC '0') = coalesce(ms.total_gl_posted_acct_amt, NUMERIC '0')
     AND coalesce(mt.total_gl_posted_acct_amt, NUMERIC '1') = coalesce(ms.total_gl_posted_acct_amt, NUMERIC '1'))
AND (coalesce(mt.total_ca_log_24_adj_amt, NUMERIC '0') = coalesce(ms.total_ca_log_24_adj_amt, NUMERIC '0')
     AND coalesce(mt.total_ca_log_24_adj_amt, NUMERIC '1') = coalesce(ms.total_ca_log_24_adj_amt, NUMERIC '1'))
AND (coalesce(mt.total_ca_log_65_adj_amt, NUMERIC '0') = coalesce(ms.total_ca_log_65_adj_amt, NUMERIC '0')
     AND coalesce(mt.total_ca_log_65_adj_amt, NUMERIC '1') = coalesce(ms.total_ca_log_65_adj_amt, NUMERIC '1'))
AND (upper(coalesce(mt.log_24_section_name, '0')) = upper(coalesce(ms.log_24_section_name, '0'))
     AND upper(coalesce(mt.log_24_section_name, '1')) = upper(coalesce(ms.log_24_section_name, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (upper(coalesce(mt.fourth_day_delay_ind, '0')) = upper(coalesce(ms.fourth_day_delay_ind, '0'))
     AND upper(coalesce(mt.fourth_day_delay_ind, '1')) = upper(coalesce(ms.fourth_day_delay_ind, '1')))
AND (upper(coalesce(mt.interim_bill_ind, '0')) = upper(coalesce(ms.interim_bill_ind, '0'))
     AND upper(coalesce(mt.interim_bill_ind, '1')) = upper(coalesce(ms.interim_bill_ind, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cost_report_year_begin,
        pat_acct_num,
        gl_acct_num,
        pat_last_name,
        admission_date,
        discharge_date,
        cost_report_year_end,
        log_type,
        total_gl_posted_acct_amt,
        total_ca_log_24_adj_amt,
        total_ca_log_65_adj_amt,
        log_24_section_name,
        dw_last_update_date_time,
        source_system_code,
        fourth_day_delay_ind,
        interim_bill_ind)
VALUES (ms.company_code, ms.coid, ms.cost_report_year_begin, ms.pat_acct_num, ms.gl_acct_num, ms.pat_last_name, ms.admission_date, ms.discharge_date, ms.cost_report_year_end, ms.log_type, ms.total_gl_posted_acct_amt, ms.total_ca_log_24_adj_amt, ms.total_ca_log_65_adj_amt, ms.log_24_section_name, ms.dw_last_update_date_time, ms.source_system_code, ms.fourth_day_delay_ind, ms.interim_bill_ind);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cost_report_year_begin,
             pat_acct_num,
             gl_acct_num,
             log_24_section_name
      FROM {{ params.param_parallon_ra_core_dataset_name }}.gr_gl_recn
      GROUP BY company_code,
               coid,
               cost_report_year_begin,
               pat_acct_num,
               gl_acct_num,
               log_24_section_name
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;