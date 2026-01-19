DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_system_productivity_log_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CR_System_Productivity_Log_WRK  	   #
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 				   #
-- #  SOURCE		   	: EDWCR_Staging.CR_System_Productivity_Log_Stg     #
-- #	                                                                           #
-- #  INITIAL RELEASE	   	: 						   #
-- #  PROJECT             	: 	 		    				   #
-- #  ------------------------------------------------------------------------	   #
-- #                                                                                  #
-- ####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_SYSTEM_PRODUCTIVITY_LOG;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_System_Productivity_Log_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cr_system_productivity_log_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate WRK Table */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cr_system_productivity_log_wrk (system_productivity_log_id, cr_patient_id, tumor_id, system_user_id_code, system_change_status_date, source_system_code, dw_last_update_date_time)
SELECT x.system_productivity_log_id,
       x.cr_patient_id,
       x.tumor_id,
       x.system_user_id_code,
       b.calendar_date AS system_change_status_date,
       x.source_system_code,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  (SELECT stg.system_productivity_log_id,
          stg.cr_patient_id,
          stg.tumor_id,
          stg.system_user_id_code,
          CASE
              WHEN rtrim(substr(trim(format('%11d', stg.system_change_status_date)), 5, 4)) IN('9999',
                                                                                               '0000') THEN concat(substr(trim(format('%11d', stg.system_change_status_date)), 1, 4), '01', '01')
              WHEN rtrim(substr(trim(format('%11d', stg.system_change_status_date)), 7, 2)) IN('99',
                                                                                               '00') THEN concat(substr(trim(format('%11d', stg.system_change_status_date)), 1, 6), '01')
              WHEN rtrim(substr(trim(format('%11d', stg.system_change_status_date)), 5, 2)) IN('99',
                                                                                               '00') THEN concat(substr(trim(format('%11d', stg.system_change_status_date)), 1, 4), '01', substr(trim(format('%11d', stg.system_change_status_date)), 7, 2))
              ELSE trim(format('%11d', stg.system_change_status_date))
          END AS system_change_status_date,
          'M' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_system_productivity_log_stg AS stg) AS x
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.sys_calendar AS b ON (CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(substr(x.system_change_status_date, 1, 4)) AS FLOAT64) - 1900) * 10000 + CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(substr(x.system_change_status_date, 5, 2)) AS FLOAT64) * 100 + CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(substr(x.system_change_status_date, 7, 2)) AS FLOAT64) = (extract(YEAR
                                                                                                                                                                                                                                                                                                                                                                                                                                                                   FROM b.calendar_date) - 1900) * 10000 + extract(MONTH
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   FROM b.calendar_date) * 100 + extract(DAY
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         FROM b.calendar_date);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_System_Productivity_Log_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;