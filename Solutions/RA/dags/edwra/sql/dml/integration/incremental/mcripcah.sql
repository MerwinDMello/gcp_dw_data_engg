DECLARE DUP_COUNT INT64;

-- Translation time: 2025-03-24T19:00:36.316296Z
-- Translation job ID: a06adffd-7255-4680-b150-d2aa709466dc
-- Source: gs://eim-parallon-cs-datamig-dev-0002/ra_ddls_bulk_conversion/nm59fE/input/mcripcah.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/MCRIPCAH.out;;
/*
Mod1:Added logic to pull values based on :
max(CASE WHEN   account_number  LIKE '%*%' THEN 'Y' else 'N' end) as Interim_Bill_Flag,
max(case when section_name ='4th Day Delay' then 'Y' else 'N' end) as Fourth_Day_Delay_Flag
Logic change on coid as below
CASE WHEN GL.COID ='08165' AND GL.SUB_UNIT_NUM  =0005 THEN '08158'
WHEN GL.COID ='34241' AND GL.SUB_UNIT_NUM  =0002 THEN '34224', Case statement has been added.PR 446, PBI 12501 - Enhancements to GL Recon PT 10/1/2018
Mod2:Added logic for COID 25164 and 27535 -- PBI25112 AM 12/26/2019
*/ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn
WHERE upper(trim(gr_gl_recn.log_type, ' ')) = 'MCR-IPCAH'
  AND DATE(gr_gl_recn.dw_last_update_date_time) = current_date('US/Central');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS mt USING
  (SELECT DISTINCT 'H' AS company_code,
                   max(cclog24mcripcah.coid) AS coid,
                   parse_date('%m/%d/%y',REGEXP_EXTRACT(SPLIT(cclog24mcripcah.reporting_period, " -")[0], r'[0-9]+/[0-9]+/[0-9]+')) AS cost_report_year_begin,
                   parse_date('%m/%d/%y',REGEXP_EXTRACT(SPLIT(cclog24mcripcah.reporting_period, " -")[1], r'[0-9]+/[0-9]+/[0-9]+')) AS cost_report_year_end,
                   substr(max(CASE
                                  WHEN upper(cclog24mcripcah.rate_schedule_name) LIKE '%GOV MCPD IP%' THEN 'MCR-IPCAH'
                              END), 1, 10) AS log_type,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(CASE
                                                                                                    WHEN cclog24mcripcah.account_number LIKE '%*%' THEN translate(cclog24mcripcah.account_number, '*', '')
                                                                                                    ELSE cclog24mcripcah.account_number
                                                                                                END) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
                   TRIM(substr(max(cclog24mcripcah.patient_name), 1, 100)) AS pat_last_name,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code,
                   '501000' AS gl_acct_num, -- -Max(cast(cast(Disch_Date as timestamp(0)) as date format 'YYYY-MM-DD'))AS discharge_date,
 max(CASE
         WHEN cclog24mcripcah.disch_date LIKE '%-%' THEN parse_date('%Y-%m-%d',REGEXP_EXTRACT(cclog24mcripcah.disch_date, r'[0-9]+-[0-9]+-[0-9]+'))
         WHEN cclog24mcripcah.disch_date LIKE '%/%' THEN parse_date('%Y/%m/%d',REGEXP_EXTRACT(cclog24mcripcah.disch_date, r'[0-9]+/[0-9]+/[0-9]+'))
         ELSE NULL
     END) AS discharge_date,
 CAST(ROUND(sum(cclog24mcripcah.expected_contractual), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS total_ca_log_24_adj_amt,
 max(CASE
         WHEN cclog24mcripcah.account_number LIKE '%*%' THEN 'Y'
         ELSE 'N'
     END) AS interim_bill_ind,
 max(CASE
         WHEN upper(rtrim(cclog24mcripcah.section_name, ' ')) = '4TH DAY DELAY' THEN 'Y'
         ELSE 'N'
     END) AS fourth_day_delay_ind
   FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.cclog24mcripcah
   WHERE cclog24mcripcah.expected_contractual <> 0
     AND upper(cclog24mcripcah.file_type) LIKE 'CCLOG24MCRIPCAH_D%'
   GROUP BY upper(cclog24mcripcah.coid),
            3,
            4,
            upper(substr(CASE
                             WHEN upper(cclog24mcripcah.rate_schedule_name) LIKE '%GOV MCPD IP%' THEN 'MCR-IPCAH'
                         END, 1, 10)),
            6,
            upper(substr(cclog24mcripcah.patient_name, 1, 100)),
            8) AS ms ON mt.company_code = ms.company_code
AND mt.coid = ms.coid
AND mt.cost_report_year_begin = ms.cost_report_year_begin
AND (coalesce(mt.cost_report_year_end, DATE '1970-01-01') = coalesce(ms.cost_report_year_end, DATE '1970-01-01')
     AND coalesce(mt.cost_report_year_end, DATE '1970-01-02') = coalesce(ms.cost_report_year_end, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_type, '0')) = upper(coalesce(ms.log_type, '0'))
     AND upper(coalesce(mt.log_type, '1')) = upper(coalesce(ms.log_type, '1')))
AND mt.pat_acct_num = ms.pat_acct_num
AND (upper(coalesce(mt.pat_last_name, '0')) = upper(coalesce(ms.pat_last_name, '0'))
     AND upper(coalesce(mt.pat_last_name, '1')) = upper(coalesce(ms.pat_last_name, '1')))
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
AND mt.source_system_code = ms.source_system_code
AND mt.gl_acct_num = ms.gl_acct_num
AND (coalesce(mt.discharge_date, DATE '1970-01-01') = coalesce(ms.discharge_date, DATE '1970-01-01')
     AND coalesce(mt.discharge_date, DATE '1970-01-02') = coalesce(ms.discharge_date, DATE '1970-01-02'))
AND (coalesce(mt.total_ca_log_24_adj_amt, NUMERIC '0') = coalesce(ms.total_ca_log_24_adj_amt, NUMERIC '0')
     AND coalesce(mt.total_ca_log_24_adj_amt, NUMERIC '1') = coalesce(ms.total_ca_log_24_adj_amt, NUMERIC '1'))
AND (upper(coalesce(mt.interim_bill_ind, '0')) = upper(coalesce(ms.interim_bill_ind, '0'))
     AND upper(coalesce(mt.interim_bill_ind, '1')) = upper(coalesce(ms.interim_bill_ind, '1')))
AND (upper(coalesce(mt.fourth_day_delay_ind, '0')) = upper(coalesce(ms.fourth_day_delay_ind, '0'))
     AND upper(coalesce(mt.fourth_day_delay_ind, '1')) = upper(coalesce(ms.fourth_day_delay_ind, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cost_report_year_begin,
        cost_report_year_end,
        log_type,
        pat_acct_num,
        pat_last_name,
        dw_last_update_date_time,
        source_system_code,
        gl_acct_num,
        discharge_date,
        total_ca_log_24_adj_amt,
        interim_bill_ind,
        fourth_day_delay_ind)
VALUES (ms.company_code, ms.coid, ms.cost_report_year_begin, ms.cost_report_year_end, ms.log_type, ms.pat_acct_num, ms.pat_last_name, ms.dw_last_update_date_time, ms.source_system_code, ms.gl_acct_num, ms.discharge_date, ms.total_ca_log_24_adj_amt, ms.interim_bill_ind, ms.fourth_day_delay_ind);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cost_report_year_begin,
             pat_acct_num,
             gl_acct_num
      FROM `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn
      GROUP BY company_code,
               coid,
               cost_report_year_begin,
               pat_acct_num,
               gl_acct_num
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

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS b USING
  (SELECT max(cclog65mcripcah.coid) AS coid,
          max(CASE
                  WHEN cclog65mcripcah.patient_number LIKE '%*%' THEN translate(cclog65mcripcah.patient_number, '*', '')
                  ELSE cclog65mcripcah.patient_number
              END) AS patient_number,
          parse_date('%m/%d/%y',REGEXP_EXTRACT(SPLIT(cclog65mcripcah.reporting_period, " -")[0], r'[0-9]+/[0-9]+/[0-9]+')) AS costreportyearbegin,
          '501000' AS gl_acct_num,
          'H' AS company_code,
          sum(cclog65mcripcah.var_contractual) AS var_contractual
   FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.cclog65mcripcah
   WHERE cclog65mcripcah.var_contractual <> 0
     AND upper(cclog65mcripcah.report_name) LIKE 'CCLOG65%'
   GROUP BY upper(cclog65mcripcah.coid),
            upper(CASE
                      WHEN cclog65mcripcah.patient_number LIKE '%*%' THEN translate(cclog65mcripcah.patient_number, '*', '')
                      ELSE cclog65mcripcah.patient_number
                  END),
            3,
            4,
            5) AS a ON upper(rtrim(b.company_code, ' ')) = upper(rtrim(a.company_code, ' '))
AND upper(rtrim(b.coid, ' ')) = upper(rtrim(a.coid, ' '))
AND b.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(a.patient_number) AS FLOAT64)
AND upper(rtrim(b.gl_acct_num, ' ')) = upper(rtrim(a.gl_acct_num, ' '))
AND b.cost_report_year_begin = a.costreportyearbegin WHEN MATCHED THEN
UPDATE
SET total_ca_log_65_adj_amt = CAST(ROUND(a.var_contractual, 3, 'ROUND_HALF_EVEN') AS NUMERIC);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cost_report_year_begin,
             pat_acct_num,
             gl_acct_num
      FROM `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn
      GROUP BY company_code,
               coid,
               cost_report_year_begin,
               pat_acct_num,
               gl_acct_num
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

/* Added Accounts missing from Log 65 */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS mt USING
  (SELECT DISTINCT 'H' AS company_code,
                   max(a.coid) AS coid,
                   parse_date('%m/%d/%y',REGEXP_EXTRACT(SPLIT(a.reporting_period, " -")[0], r'[0-9]+/[0-9]+/[0-9]+')) AS cost_report_year_begin,
                   parse_date('%m/%d/%y',REGEXP_EXTRACT(SPLIT(a.reporting_period, " -")[1], r'[0-9]+/[0-9]+/[0-9]+')) AS cost_report_year_end,
                   substr(max(CASE
                                  WHEN upper(a.rate_schedule_name) LIKE '%GOV MCPD IP%' THEN 'MCR-IPCAH'
                              END), 1, 10) AS log_type,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(CASE
                                                                                                    WHEN a.patient_number LIKE '%*%' THEN translate(a.patient_number, '*', '')
                                                                                                    ELSE a.patient_number
                                                                                                END) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
                   TRIM(substr(max(a.patient_name), 1, 100)) AS pat_last_name,
                   '501000' AS gl_acct_num,
                   CAST(0 AS NUMERIC) AS total_ca_log_24_adj_amt,
                   ROUND(a.var_contractual, 3, 'ROUND_HALF_EVEN') AS total_ca_log_65_adj_amt,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time, -- -Max(cast(cast(discharge_date as timestamp(0)) as date format 'YYYY-MM-DD'))AS discharge_date,
 max(CASE
         WHEN a.discharge_date LIKE '%-%' THEN parse_date('%Y-%m-%d',REGEXP_EXTRACT(a.discharge_date, r'[0-9]+-[0-9]+-[0-9]+'))
         WHEN a.discharge_date LIKE '%/%' THEN parse_date('%Y/%m/%d',REGEXP_EXTRACT(a.discharge_date, r'[0-9]+/[0-9]+/[0-9]+'))
         ELSE NULL
     END) AS discharge_date,
 max(CASE
         WHEN a.patient_number LIKE '%*%' THEN 'Y'
         ELSE 'N'
     END) AS interim_bill_ind,
 'N' AS fourth_day_delay_ind
   FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.cclog65mcripcah AS a
   WHERE a.var_contractual <> 0
     AND upper(a.report_name) LIKE 'CCLOG65%'
     AND NOT EXISTS
       (SELECT 1
        FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.cclog24mcripcah AS b
        WHERE b.expected_contractual <> 0
          AND upper(b.file_type) LIKE 'CCLOG24MCRIPCAH_D%'
          AND upper(rtrim(b.coid, ' ')) = upper(rtrim(a.coid, ' '))
          AND upper(rtrim(b.account_number, ' ')) = upper(rtrim(a.patient_number, ' ')) )
   GROUP BY upper(a.coid),
            3,
            4,
            upper(substr(CASE
                             WHEN upper(a.rate_schedule_name) LIKE '%GOV MCPD IP%' THEN 'MCR-IPCAH'
                         END, 1, 10)),
            6,
            upper(substr(a.patient_name, 1, 100)),
            9,
            10,
            12) AS ms ON mt.company_code = ms.company_code
AND mt.coid = ms.coid
AND mt.cost_report_year_begin = ms.cost_report_year_begin
AND (coalesce(mt.cost_report_year_end, DATE '1970-01-01') = coalesce(ms.cost_report_year_end, DATE '1970-01-01')
     AND coalesce(mt.cost_report_year_end, DATE '1970-01-02') = coalesce(ms.cost_report_year_end, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_type, '0')) = upper(coalesce(ms.log_type, '0'))
     AND upper(coalesce(mt.log_type, '1')) = upper(coalesce(ms.log_type, '1')))
AND mt.pat_acct_num = ms.pat_acct_num
AND (upper(coalesce(mt.pat_last_name, '0')) = upper(coalesce(ms.pat_last_name, '0'))
     AND upper(coalesce(mt.pat_last_name, '1')) = upper(coalesce(ms.pat_last_name, '1')))
AND mt.gl_acct_num = ms.gl_acct_num
AND (coalesce(mt.total_ca_log_24_adj_amt, NUMERIC '0') = coalesce(ms.total_ca_log_24_adj_amt, NUMERIC '0')
     AND coalesce(mt.total_ca_log_24_adj_amt, NUMERIC '1') = coalesce(ms.total_ca_log_24_adj_amt, NUMERIC '1'))
AND (coalesce(mt.total_ca_log_65_adj_amt, NUMERIC '0') = coalesce(ms.total_ca_log_65_adj_amt, NUMERIC '0')
     AND coalesce(mt.total_ca_log_65_adj_amt, NUMERIC '1') = coalesce(ms.total_ca_log_65_adj_amt, NUMERIC '1'))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
AND (coalesce(mt.discharge_date, DATE '1970-01-01') = coalesce(ms.discharge_date, DATE '1970-01-01')
     AND coalesce(mt.discharge_date, DATE '1970-01-02') = coalesce(ms.discharge_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.interim_bill_ind, '0')) = upper(coalesce(ms.interim_bill_ind, '0'))
     AND upper(coalesce(mt.interim_bill_ind, '1')) = upper(coalesce(ms.interim_bill_ind, '1')))
AND (upper(coalesce(mt.fourth_day_delay_ind, '0')) = upper(coalesce(ms.fourth_day_delay_ind, '0'))
     AND upper(coalesce(mt.fourth_day_delay_ind, '1')) = upper(coalesce(ms.fourth_day_delay_ind, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cost_report_year_begin,
        cost_report_year_end,
        log_type,
        pat_acct_num,
        pat_last_name,
        gl_acct_num,
        total_ca_log_24_adj_amt,
        total_ca_log_65_adj_amt,
        source_system_code,
        dw_last_update_date_time,
        discharge_date,
        interim_bill_ind,
        fourth_day_delay_ind)
VALUES (ms.company_code, ms.coid, ms.cost_report_year_begin, ms.cost_report_year_end, ms.log_type, ms.pat_acct_num, ms.pat_last_name, ms.gl_acct_num, ms.total_ca_log_24_adj_amt, ms.total_ca_log_65_adj_amt, ms.source_system_code, ms.dw_last_update_date_time, ms.discharge_date, ms.interim_bill_ind, ms.fourth_day_delay_ind);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cost_report_year_begin,
             pat_acct_num,
             gl_acct_num
      FROM `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn
      GROUP BY company_code,
               coid,
               cost_report_year_begin,
               pat_acct_num,
               gl_acct_num
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

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS b USING
  (SELECT max(CASE
                  WHEN rtrim(gl.coid, ' ') = '08165'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                  WHEN rtrim(gl.coid, ' ') = '34241'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                  WHEN rtrim(gl.coid, ' ') = '25164'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                  ELSE gl.coid
              END) AS coid, --  WHEN GL.COID ='27535' and GL.SUB_UNIT_NUM = 0003 THEN '39385'
 gl.pat_acct_num,
 max(ltrim(CAST(gl.gl_account AS STRING))) AS gl_account,
 gr.cost_report_year_begin,
 'H' AS company_code,
 sum(gl.gl_proc_amt) AS total_gl_posted_acct_amt
   FROM `{{ params.param_parallon_ra_base_views_dataset_name }}`.pagl_transaction AS gl
   LEFT OUTER JOIN `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS gr ON upper(rtrim(gr.coid, ' ')) = upper(rtrim(CASE
                                                                                                                                     WHEN rtrim(gl.coid, ' ') = '08165'
                                                                                                                                          AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                                                                                                                                     WHEN rtrim(gl.coid, ' ') = '34241'
                                                                                                                                          AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                                                                                                                                     WHEN rtrim(gl.coid, ' ') = '25164'
                                                                                                                                          AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                                                                                                                                     ELSE gl.coid
                                                                                                                                 END, ' '))
   AND gl.pat_acct_num = gr.pat_acct_num
   WHERE gl.gl_account = 501000
     AND gl.pedate >= gr.cost_report_year_begin
     AND gl.pedate <= LAST_DAY(DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 1 MONTH))
     -- WHEN GL.COID ='27535' and GL.SUB_UNIT_NUM = 0003 THEN '39385'
     AND upper(rtrim(gr.log_type, ' ')) = 'MCR-IPCAH'
     AND rtrim(gl.coid, ' ') NOT IN('08165',
                                    '08158')
   GROUP BY upper(CASE
                      WHEN rtrim(gl.coid, ' ') = '08165'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                      WHEN rtrim(gl.coid, ' ') = '34241'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                      WHEN rtrim(gl.coid, ' ') = '25164'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                      ELSE gl.coid
                  END),
            2,
            upper(ltrim(CAST(gl.gl_account AS STRING))),
            4,
            5) AS a ON upper(rtrim(b.company_code, ' ')) = upper(rtrim(a.company_code, ' '))
AND upper(rtrim(b.coid, ' ')) = upper(rtrim(a.coid, ' '))
AND b.pat_acct_num = a.pat_acct_num
AND upper(rtrim(b.gl_acct_num, ' ')) = upper(rtrim(a.gl_account, ' '))
AND b.cost_report_year_begin = a.cost_report_year_begin WHEN MATCHED THEN
UPDATE
SET total_gl_posted_acct_amt = CAST(ROUND(a.total_gl_posted_acct_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cost_report_year_begin,
             pat_acct_num,
             gl_acct_num
      FROM `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn
      GROUP BY company_code,
               coid,
               cost_report_year_begin,
               pat_acct_num,
               gl_acct_num
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

-- HAVING (SUM (GL.GL_PROC_AMT) > 0.99
-- OR  SUM(GL.GL_PROC_AMT) < -0.99)
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS b USING
  (SELECT max(CASE
                  WHEN rtrim(gl.coid, ' ') = '08165'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                  ELSE gl.coid
              END) AS coid, -- WHEN GL.COID ='34241' AND GL.SUB_UNIT_NUM  =0002 THEN '34224'
 gl.pat_acct_num,
 max(CASE
         WHEN rtrim(ltrim(CAST(gl.gl_account AS STRING)), ' ') = '110115' THEN '501000'
         ELSE ltrim(CAST(gl.gl_account AS STRING))
     END) AS gl_account,
 gr.cost_report_year_begin,
 'H' AS company_code,
 sum(gl.gl_proc_amt) AS total_gl_posted_acct_amt
   FROM `{{ params.param_parallon_ra_base_views_dataset_name }}`.pagl_transaction AS gl
   LEFT OUTER JOIN `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS gr ON upper(rtrim(gr.coid, ' ')) = upper(rtrim(CASE
                                                                                                                                     WHEN rtrim(gl.coid, ' ') = '08165'
                                                                                                                                          AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                                                                                                                                     ELSE gl.coid
                                                                                                                                 END, ' '))
   AND gl.pat_acct_num = gr.pat_acct_num
   WHERE gl.gl_account IN(-- WHEN GL.COID ='34241' AND GL.SUB_UNIT_NUM  =0002 THEN '34224'
 110115,
 501000)
     AND gl.pedate >= gr.cost_report_year_begin
     AND gl.pedate <= LAST_DAY(DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 1 MONTH))
     AND upper(rtrim(gr.log_type, ' ')) = 'MCR-IPCAH'
     AND rtrim(gl.coid, ' ') IN('08165',
                                '08158')
   GROUP BY upper(CASE
                      WHEN rtrim(gl.coid, ' ') = '08165'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                      ELSE gl.coid
                  END),
            2,
            upper(CASE
                      WHEN rtrim(ltrim(CAST(gl.gl_account AS STRING)), ' ') = '110115' THEN '501000'
                      ELSE ltrim(CAST(gl.gl_account AS STRING))
                  END),
            4,
            5) AS a ON upper(rtrim(b.company_code, ' ')) = upper(rtrim(a.company_code, ' '))
AND upper(rtrim(b.coid, ' ')) = upper(rtrim(a.coid, ' '))
AND b.pat_acct_num = a.pat_acct_num
AND upper(rtrim(b.gl_acct_num, ' ')) = upper(rtrim(a.gl_account, ' '))
AND b.cost_report_year_begin = a.cost_report_year_begin WHEN MATCHED THEN
UPDATE
SET total_gl_posted_acct_amt = CAST(ROUND(a.total_gl_posted_acct_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cost_report_year_begin,
             pat_acct_num,
             gl_acct_num
      FROM `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn
      GROUP BY company_code,
               coid,
               cost_report_year_begin,
               pat_acct_num,
               gl_acct_num
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

-- HAVING (SUM (GL.GL_PROC_AMT) > 0.99
-- OR  SUM(GL.GL_PROC_AMT) < -0.99)
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;