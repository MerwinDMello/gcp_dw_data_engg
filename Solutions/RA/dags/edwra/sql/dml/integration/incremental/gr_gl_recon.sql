DECLARE DUP_COUNT INT64;

-- Translation time: 2025-03-24T19:00:36.316296Z
-- Translation job ID: a06adffd-7255-4680-b150-d2aa709466dc
-- Source: gs://eim-parallon-cs-datamig-dev-0002/ra_ddls_bulk_conversion/nm59fE/input/gr_gl_recon.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/GR_GL_RECON.out;;
/*
Mod1:Added columns Fourth_Day_Delay_Flag,Interim_Bill_Flag and include new GL_accounts in where condition as part of  PBI 12501 - Enhancements to GL Recon PT 10/1/2018
Mod2: modified insrt to mrge to remove deuplciates. added logic for COID 08158 and 34224
Mod3: Added logic for COID 25164 and 27535 -- PBI25112 AM 12/26/2019
Mod4: Added COID case statement while joining tables. -- PBI25112 AM 1/27/2019
*/ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS tgt USING
  (SELECT 'H' AS company_code,
          max(CASE
                  WHEN rtrim(gl.coid, ' ') = '08165'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                  WHEN rtrim(gl.coid, ' ') = '34241'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                  WHEN rtrim(gl.coid, ' ') = '25164'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                  ELSE gl.coid
              END) AS coid, --  -- WHEN GL.COID ='27535' and GL.SUB_UNIT_NUM = 0003 THEN '39385'
 gl.pat_acct_num,
 max(ltrim(CAST(gl.gl_account AS STRING))) AS gl_account,
 ry.rpt_yr_start_date,
 ry.rpt_yr_end_date,
 max(CASE gl.gl_account
         WHEN 501000 THEN 'MCR-IP'
         WHEN 501202 THEN 'MCR-IPF'
         WHEN 501302 THEN 'MCR-IRF'
         WHEN 501340 THEN 'LTCH'
         WHEN 501112 THEN 'SNF'
         WHEN 501502 THEN 'MCR-OP'
         WHEN 501500 THEN 'MCR-OPCAH'
         WHEN 501102 THEN 'MCR-IPSWB'
         ELSE 'UNKNOWN'
     END) AS log_type,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
 'N' AS source_system_code,
 sum(gl.gl_proc_amt) AS total_gl_amt,
 'N' AS fourth_day_delay_flag,
 'N' AS interim_bill_flag
   FROM `{{ params.param_parallon_ra_base_views_dataset_name }}`.pagl_transaction AS gl
   INNER JOIN
     (SELECT gl_report_year.coid,
             gl_report_year.rpt_yr_start_date,
             gl_report_year.rpt_yr_end_date
      FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.gl_report_year) AS ry ON upper(rtrim(gl.coid, ' ')) = upper(rtrim(ry.coid, ' '))
   LEFT OUTER JOIN `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS gr ON upper(rtrim(CASE
                                                                                                        WHEN rtrim(gl.coid, ' ') = '08165'
                                                                                                             AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                                                                                                        WHEN rtrim(gl.coid, ' ') = '34241'
                                                                                                             AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                                                                                                        WHEN rtrim(gl.coid, ' ') = '25164'
                                                                                                             AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                                                                                                        ELSE gl.coid
                                                                                                    END, ' ')) = upper(rtrim(gr.coid, ' '))
   AND gl.pat_acct_num = gr.pat_acct_num
   WHERE gl.pedate BETWEEN ry.rpt_yr_start_date AND ry.rpt_yr_end_date
     AND gl.pedate <= LAST_DAY(DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 1 MONTH))
     --  WHEN GL.COID ='27535' and GL.SUB_UNIT_NUM = 0003 THEN '39385'
     AND gl.gl_account IN(501000,
                          501202,
                          501302,
                          501340,
                          501112,
                          501502,
                          501500,
                          501102)
     AND gr.pat_acct_num IS NULL
   GROUP BY 1,
            upper(CASE
                      WHEN rtrim(gl.coid, ' ') = '08165'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                      WHEN rtrim(gl.coid, ' ') = '34241'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                      WHEN rtrim(gl.coid, ' ') = '25164'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                      ELSE gl.coid
                  END),
            3,
            upper(ltrim(CAST(gl.gl_account AS STRING))),
            5,
            6,
            upper(CASE gl.gl_account
                      WHEN 501000 THEN 'MCR-IP'
                      WHEN 501202 THEN 'MCR-IPF'
                      WHEN 501302 THEN 'MCR-IRF'
                      WHEN 501340 THEN 'LTCH'
                      WHEN 501112 THEN 'SNF'
                      WHEN 501502 THEN 'MCR-OP'
                      WHEN 501500 THEN 'MCR-OPCAH'
                      WHEN 501102 THEN 'MCR-IPSWB'
                      ELSE 'UNKNOWN'
                  END),
            8,
            12) AS src ON upper(rtrim(tgt.coid, ' ')) = upper(rtrim(src.coid, ' '))
AND tgt.pat_acct_num = src.pat_acct_num
AND upper(rtrim(tgt.gl_acct_num, ' ')) = upper(rtrim(src.gl_account, ' '))
AND tgt.cost_report_year_begin = src.rpt_yr_start_date
AND upper(rtrim(tgt.company_code, ' ')) = upper(rtrim(src.company_code, ' ')) WHEN MATCHED THEN
UPDATE
SET cost_report_year_end = src.rpt_yr_end_date,
    log_type = substr(src.log_type, 1, 10),
    dw_last_update_date_time = src.dw_last_update_date_time,
    source_system_code = src.source_system_code,
    total_gl_posted_acct_amt = CAST(ROUND(src.total_gl_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC),
    fourth_day_delay_ind = src.fourth_day_delay_flag,
    interim_bill_ind = src.interim_bill_flag WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        pat_acct_num,
        gl_acct_num,
        cost_report_year_begin,
        cost_report_year_end,
        log_type,
        dw_last_update_date_time,
        source_system_code,
        total_gl_posted_acct_amt,
        fourth_day_delay_ind,
        interim_bill_ind)
VALUES (src.company_code, src.coid, src.pat_acct_num, src.gl_account, src.rpt_yr_start_date, src.rpt_yr_end_date, substr(src.log_type, 1, 10), src.dw_last_update_date_time, src.source_system_code, CAST(ROUND(src.total_gl_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC), src.fourth_day_delay_flag, src.interim_bill_flag);


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

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS tgt USING
  (SELECT 'H' AS company_code,
          max(CASE
                  WHEN rtrim(gl.coid, ' ') = '08165'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                  WHEN rtrim(gl.coid, ' ') = '34241'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                  WHEN rtrim(gl.coid, ' ') = '25164'
                       AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                  ELSE gl.coid
              END) AS coid, --  -- WHEN GL.COID ='27535' and GL.SUB_UNIT_NUM = 0003 THEN '39385'
 gl.pat_acct_num,
 max(ltrim(CAST(gl.gl_account AS STRING))) AS gl_account,
 ry.rpt_yr_start_date,
 ry.rpt_yr_end_date,
 max(CASE gl.gl_account
         WHEN 501000 THEN 'MCR-IP'
         WHEN 501202 THEN 'MCR-IPF'
         WHEN 501302 THEN 'MCR-IRF'
         WHEN 501340 THEN 'LTCH'
         WHEN 501112 THEN 'SNF'
         WHEN 501502 THEN 'MCR-OP'
         WHEN 501500 THEN 'MCR-OPCAH'
         WHEN 501102 THEN 'MCR-IPSWB'
         ELSE 'UNKNOWN'
     END) AS log_type,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
 'N' AS source_system_code,
 sum(gl.gl_proc_amt) AS total_gl_amt,
 'N' AS fourth_day_delay_flag,
 'N' AS interim_bill_flag
   FROM `{{ params.param_parallon_ra_base_views_dataset_name }}`.pagl_transaction AS gl
   INNER JOIN
     (SELECT gl_report_year.coid,
             gl_report_year.rpt_yr_start_date,
             gl_report_year.rpt_yr_end_date
      FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.gl_report_year) AS ry ON upper(rtrim(gl.coid, ' ')) = upper(rtrim(ry.coid, ' '))
   WHERE gl.pedate BETWEEN ry.rpt_yr_start_date AND ry.rpt_yr_end_date
     AND gl.pedate <= LAST_DAY(DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 1 MONTH))
     AND gl.gl_account IN(501000,
                          501202,
                          501302,
                          501340,
                          501112,
                          501502,
                          501500,
                          501102)
     AND NOT EXISTS
       (SELECT 1
        FROM `{{ params.param_parallon_ra_core_dataset_name }}`.gr_gl_recn AS b
        WHERE upper(rtrim(b.coid, ' ')) = upper(rtrim(CASE
                                                          WHEN rtrim(gl.coid, ' ') = '08165'
                                                               AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                                                          WHEN rtrim(gl.coid, ' ') = '34241'
                                                               AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                                                          WHEN rtrim(gl.coid, ' ') = '25164'
                                                               AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                                                          ELSE gl.coid
                                                      END, ' '))
          AND b.pat_acct_num = gl.pat_acct_num
          AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(b.gl_acct_num) AS FLOAT64) = gl.gl_account )
   GROUP BY 1,
            upper(CASE
                      WHEN rtrim(gl.coid, ' ') = '08165'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 5 THEN '08158'
                      WHEN rtrim(gl.coid, ' ') = '34241'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '34224'
                      WHEN rtrim(gl.coid, ' ') = '25164'
                           AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(gl.sub_unit_num) AS FLOAT64) = 2 THEN '39385'
                      ELSE gl.coid
                  END),
            3,
            upper(ltrim(CAST(gl.gl_account AS STRING))),
            5,
            6,
            upper(CASE gl.gl_account
                      WHEN 501000 THEN 'MCR-IP'
                      WHEN 501202 THEN 'MCR-IPF'
                      WHEN 501302 THEN 'MCR-IRF'
                      WHEN 501340 THEN 'LTCH'
                      WHEN 501112 THEN 'SNF'
                      WHEN 501502 THEN 'MCR-OP'
                      WHEN 501500 THEN 'MCR-OPCAH'
                      WHEN 501102 THEN 'MCR-IPSWB'
                      ELSE 'UNKNOWN'
                  END),
            8,
            12
   HAVING total_gl_amt <> 0) AS src ON upper(rtrim(tgt.coid, ' ')) = upper(rtrim(src.coid, ' '))
AND tgt.pat_acct_num = src.pat_acct_num
AND upper(rtrim(tgt.gl_acct_num, ' ')) = upper(rtrim(src.gl_account, ' '))
AND tgt.cost_report_year_begin = src.rpt_yr_start_date
AND upper(rtrim(tgt.company_code, ' ')) = upper(rtrim(src.company_code, ' ')) WHEN MATCHED THEN
UPDATE
SET cost_report_year_end = src.rpt_yr_end_date,
    log_type = substr(src.log_type, 1, 10),
    dw_last_update_date_time = src.dw_last_update_date_time,
    source_system_code = src.source_system_code,
    total_gl_posted_acct_amt = CAST(ROUND(src.total_gl_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC),
    fourth_day_delay_ind = src.fourth_day_delay_flag,
    interim_bill_ind = src.interim_bill_flag WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        pat_acct_num,
        gl_acct_num,
        cost_report_year_begin,
        cost_report_year_end,
        log_type,
        dw_last_update_date_time,
        source_system_code,
        total_gl_posted_acct_amt,
        fourth_day_delay_ind,
        interim_bill_ind)
VALUES (src.company_code, src.coid, src.pat_acct_num, src.gl_account, src.rpt_yr_start_date, src.rpt_yr_end_date, substr(src.log_type, 1, 10), src.dw_last_update_date_time, src.source_system_code, CAST(ROUND(src.total_gl_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC), src.fourth_day_delay_flag, src.interim_bill_flag);


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

--  WHEN GL.COID ='27535' and GL.SUB_UNIT_NUM = 0003 THEN '39385'
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.gr_gl_recn AS mt USING
  (SELECT DISTINCT substr(coalesce(ros.company_code, 'H'), 1, 1) AS company_code,
                   grec.coid AS coid,
                   grec.cost_report_year_begin,
                   grec.pat_acct_num,
                   grec.gl_acct_num AS gl_acct_num,
                   substr(coalesce(grec.log_24_section_name, 'NA'), 1, 25) AS log_24_section_name,
                   substr(coalesce(ros.source_system_code, 'N'), 1, 1) AS source_system_code
   FROM {{ params.param_parallon_ra_core_dataset_name}}.gr_gl_recn AS grec
   LEFT OUTER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS ros 
   ON upper(rtrim(grec.coid)) = upper(rtrim(ros.coid))) AS ms 
   ON (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.cost_report_year_begin, DATE '1970-01-01') = coalesce(ms.cost_report_year_begin, DATE '1970-01-01')
     AND coalesce(mt.cost_report_year_begin, DATE '1970-01-02') = coalesce(ms.cost_report_year_begin, DATE '1970-01-02'))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (upper(coalesce(mt.gl_acct_num, '0')) = upper(coalesce(ms.gl_acct_num, '0'))
     AND upper(coalesce(mt.gl_acct_num, '1')) = upper(coalesce(ms.gl_acct_num, '1')))
WHEN MATCHED THEN
UPDATE
SET company_code = ms.company_code,
log_24_section_name = ms.log_24_section_name,
source_system_code = ms.source_system_code,
dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
;

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