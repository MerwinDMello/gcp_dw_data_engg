DECLARE DUP_COUNT INT64;

-- Translation time: 2025-03-24T19:00:36.316296Z
-- Translation job ID: a06adffd-7255-4680-b150-d2aa709466dc
-- Source: gs://eim-parallon-cs-datamig-dev-0002/ra_ddls_bulk_conversion/nm59fE/input/pagl_transaction.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- bteq << EOF > $1;;
-- IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

-- END IF;

-- BEGIN
-- SET _ERROR_CODE = 0;

-- TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly;


-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE = 1;


-- SET _ERROR_MSG = @@error.message;

-- END;

-- IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

-- END IF;

BEGIN
SET _ERROR_CODE = 0;

DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx
WHERE (DATE(entered_ts) < date_add(current_date('US/Central'), interval -30 MONTH)
OR DATE(entered_ts) = date_sub(current_date('US/Central'), interval 1 DAY));

BEGIN TRANSACTION;

MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.brbglx AS mt USING
  (SELECT DISTINCT b.stack AS STACK,
                   b.co_id,
                   b.pat_no,
                   b.entered_ts,
                   b.gl_src AS gl_src,
                   b.gl_proc_type,
                   b.gl_account,
                   b.gl_proc_amt,
                   b.gl_serv_dt,
                   b.proc_code,
                   b.gl_source AS gl_source,
                   b.factor,
                   b.pat_type AS pat_type,
                   b.dept,
                   b.fin_class,
                   b.gl_adjust_ind AS gl_adjust_ind,
                   b.foreign_fac_id AS foreign_fac_id,
                   b.foreign_dept AS foreign_dept,
                   b.foreign_pat_type AS foreign_pat_type,
                   b.foreign_gl_com AS foreign_gl_com,
                   b.foreign_fin_class,
                   b.process_ind AS process_ind,
                   b.pedate,
                   b.sub_unit_num AS sub_unit_num
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly AS b) AS ms ON mt.stack = ms.stack
AND mt.co_id = ms.co_id
AND mt.pat_no = ms.pat_no
AND mt.entered_ts = ms.entered_ts
AND mt.gl_src = ms.gl_src
AND mt.gl_proc_type = ms.gl_proc_type
AND mt.gl_account = ms.gl_account
AND mt.gl_proc_amt = ms.gl_proc_amt
AND (coalesce(mt.gl_serv_dt, DATE '1970-01-01') = coalesce(ms.gl_serv_dt, DATE '1970-01-01')
     AND coalesce(mt.gl_serv_dt, DATE '1970-01-02') = coalesce(ms.gl_serv_dt, DATE '1970-01-02'))
AND mt.proc_code = ms.proc_code
AND mt.gl_source = ms.gl_source
AND mt.factor = ms.factor
AND mt.pat_type = ms.pat_type
AND mt.dept = ms.dept
AND mt.fin_class = ms.fin_class
AND mt.gl_adjust_ind = ms.gl_adjust_ind
AND mt.foreign_fac_id = ms.foreign_fac_id
AND mt.foreign_dept = ms.foreign_dept
AND mt.foreign_pat_type = ms.foreign_pat_type
AND mt.foreign_gl_com = ms.foreign_gl_com
AND mt.foreign_fin_class = ms.foreign_fin_class
AND mt.process_ind = ms.process_ind
AND mt.pedate = ms.pedate
AND (upper(coalesce(mt.sub_unit_num, '0')) = upper(coalesce(ms.sub_unit_num, '0'))
     AND upper(coalesce(mt.sub_unit_num, '1')) = upper(coalesce(ms.sub_unit_num, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (STACK,
        co_id,
        pat_no,
        entered_ts,
        gl_src,
        gl_proc_type,
        gl_account,
        gl_proc_amt,
        gl_serv_dt,
        proc_code,
        gl_source,
        factor,
        pat_type,
        dept,
        fin_class,
        gl_adjust_ind,
        foreign_fac_id,
        foreign_dept,
        foreign_pat_type,
        foreign_gl_com,
        foreign_fin_class,
        process_ind,
        pedate,
        sub_unit_num)
VALUES (ms.stack, ms.co_id, ms.pat_no, ms.entered_ts, ms.gl_src, ms.gl_proc_type, ms.gl_account, ms.gl_proc_amt, ms.gl_serv_dt, ms.proc_code, ms.gl_source, ms.factor, ms.pat_type, ms.dept, ms.fin_class, ms.gl_adjust_ind, ms.foreign_fac_id, ms.foreign_dept, ms.foreign_pat_type, ms.foreign_gl_com, ms.foreign_fin_class, ms.process_ind, ms.pedate, ms.sub_unit_num);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT STACK,
             co_id,
             pat_no,
             entered_ts
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
      GROUP BY STACK,
               co_id,
               pat_no,
               entered_ts
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly');

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
FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
WHERE brbglx_dly.co_id IN(8158,
                          31)
  AND rtrim(brbglx_dly.sub_unit_num, ' ') NOT IN('0001',
                                                 '0002',
                                                 '0005',
                                                 '0003');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
WHERE brbglx_dly.co_id IN(2531)
  AND rtrim(brbglx_dly.sub_unit_num, ' ') NOT IN('0001',
                                                 '0002',
                                                 '0005',
                                                 '0003',
                                                 '0004',
                                                 '0006');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
WHERE brbglx_dly.co_id IN(8158,
                          31,
                          2531)
  AND brbglx_dly.sub_unit_num IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
WHERE brbglx_dly.co_id = 8158
  AND rtrim(brbglx_dly.sub_unit_num, ' ') = '0002';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
SET co_id = 584
WHERE brbglx_dly.co_id = 31
  AND rtrim(brbglx_dly.sub_unit_num, ' ') = '0002';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
SET co_id = 25164
WHERE brbglx_dly.co_id = 2531
  AND rtrim(brbglx_dly.sub_unit_num, ' ') = '0002';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly
SET co_id = 8165
WHERE brbglx_dly.co_id = 8158
  AND rtrim(brbglx_dly.sub_unit_num, ' ') = '0005';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;



DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.pagl_transaction
WHERE (DATE(entered_ts) < date_add(current_date('US/Central'), interval -30 MONTH)
OR DATE(entered_ts) = date_sub(current_date('US/Central'), interval 1 DAY));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.pagl_transaction AS mt USING
  (SELECT DISTINCT b.stack AS STACK,
                   o.coid AS coid,
                   o.unit_num AS unit_num,
                   b.pat_no,
                   b.entered_ts,
                   TRIM(b.gl_src) AS gl_src,
                   b.gl_proc_type,
                   b.gl_account,
                   b.gl_proc_amt,
                   b.gl_serv_dt,
                   b.proc_code,
                   TRIM(b.gl_source) AS gl_source,
                   b.factor,
                   TRIM(b.pat_type) AS patient_type,
                   b.dept,
                   b.fin_class,
                   TRIM(b.gl_adjust_ind) AS gl_adjust_ind,
                   TRIM(b.foreign_fac_id) AS foreign_fac_id,
                   TRIM(b.foreign_dept) AS foreign_dept,
                   TRIM(b.foreign_pat_type) AS foreign_pat_type,
                   TRIM(b.foreign_gl_com) AS foreign_gl_com,
                   b.foreign_fin_class,
                   b.process_ind AS process_ind,
                   b.pedate,
                   TRIM(b.sub_unit_num) AS sub_unit_num,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.brbglx_dly AS b
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.dim_organization AS o ON b.co_id = o.unit_num_sid) AS ms ON mt.stack = ms.stack
AND mt.coid = ms.coid
AND mt.unit_num = ms.unit_num
AND mt.pat_acct_num = ms.pat_no
AND mt.entered_ts = ms.entered_ts
AND mt.gl_src = ms.gl_src
AND mt.gl_proc_type = ms.gl_proc_type
AND mt.gl_account = ms.gl_account
AND mt.gl_proc_amt = ms.gl_proc_amt
AND (coalesce(mt.gl_serv_dt, DATE '1970-01-01') = coalesce(ms.gl_serv_dt, DATE '1970-01-01')
     AND coalesce(mt.gl_serv_dt, DATE '1970-01-02') = coalesce(ms.gl_serv_dt, DATE '1970-01-02'))
AND mt.proc_code = ms.proc_code
AND mt.gl_source = ms.gl_source
AND mt.factor = ms.factor
AND mt.patient_type = ms.patient_type
AND mt.dept = ms.dept
AND mt.financial_class = ms.fin_class
AND mt.gl_adjust_ind = ms.gl_adjust_ind
AND mt.foreign_fac_id = ms.foreign_fac_id
AND mt.foreign_dept = ms.foreign_dept
AND mt.foreign_pat_type = ms.foreign_pat_type
AND mt.foreign_gl_com = ms.foreign_gl_com
AND mt.foreign_fin_class = ms.foreign_fin_class
AND mt.process_ind = ms.process_ind
AND mt.pedate = ms.pedate
AND (upper(coalesce(mt.sub_unit_num, '0')) = upper(coalesce(ms.sub_unit_num, '0'))
     AND upper(coalesce(mt.sub_unit_num, '1')) = upper(coalesce(ms.sub_unit_num, '1')))
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
AND mt.source_system_code = ms.source_system_code WHEN NOT MATCHED BY TARGET THEN
INSERT (STACK,
        coid,
        unit_num,
        pat_acct_num,
        entered_ts,
        gl_src,
        gl_proc_type,
        gl_account,
        gl_proc_amt,
        gl_serv_dt,
        proc_code,
        gl_source,
        factor,
        patient_type,
        dept,
        financial_class,
        gl_adjust_ind,
        foreign_fac_id,
        foreign_dept,
        foreign_pat_type,
        foreign_gl_com,
        foreign_fin_class,
        process_ind,
        pedate,
        sub_unit_num,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.stack, ms.coid, ms.unit_num, ms.pat_no, ms.entered_ts, ms.gl_src, ms.gl_proc_type, ms.gl_account, ms.gl_proc_amt, ms.gl_serv_dt, ms.proc_code, ms.gl_source, ms.factor, ms.patient_type, ms.dept, ms.fin_class, ms.gl_adjust_ind, ms.foreign_fac_id, ms.foreign_dept, ms.foreign_pat_type, ms.foreign_gl_com, ms.foreign_fin_class, ms.process_ind, ms.pedate, ms.sub_unit_num, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT STACK,
             coid,
             unit_num,
             pat_acct_num,
             entered_ts
      FROM {{ params.param_parallon_ra_core_dataset_name }}.pagl_transaction
      GROUP BY STACK,
               coid,
               unit_num,
               pat_acct_num,
               entered_ts
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.pagl_transaction');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','PAGL_TRANSACTION');
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

---- EOF;;