DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_system_productivity_log_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #########################################################################
-- #  TARGET TABLE		: EDWCR.CR_System_Productivity_Log             	#
-- #  TARGET  DATABASE	: EDWCR	 				        #
-- #  SOURCE		: EDWCR_STAGING.CR_System_Productivity_Log_WRK	#
-- #	                                                                #
-- #  INITIAL RELEASE	   	: 					#
-- #  PROJECT             	: 	 		    			#
-- #  ---------------------------------------------------------------------#
-- #                                                                       #
-- #########################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_CONTACT;;
 --' FOR SESSION;;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_system_productivity_log;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_system_productivity_log AS mt USING
  (SELECT DISTINCT cr_system_productivity_log_wrk.system_productivity_log_id,
                   cr_system_productivity_log_wrk.cr_patient_id,
                   cr_system_productivity_log_wrk.tumor_id,
                   cr_system_productivity_log_wrk.system_user_id_code,
                   cr_system_productivity_log_wrk.system_change_status_date,
                   cr_system_productivity_log_wrk.source_system_code AS source_system_code,
                   cr_system_productivity_log_wrk.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_system_productivity_log_wrk) AS ms ON mt.system_productivity_log_id = ms.system_productivity_log_id
AND (coalesce(mt.cr_patient_id, 0) = coalesce(ms.cr_patient_id, 0)
     AND coalesce(mt.cr_patient_id, 1) = coalesce(ms.cr_patient_id, 1))
AND (coalesce(mt.tumor_id, 0) = coalesce(ms.tumor_id, 0)
     AND coalesce(mt.tumor_id, 1) = coalesce(ms.tumor_id, 1))
AND (upper(coalesce(mt.system_user_id_code, '0')) = upper(coalesce(ms.system_user_id_code, '0'))
     AND upper(coalesce(mt.system_user_id_code, '1')) = upper(coalesce(ms.system_user_id_code, '1')))
AND (coalesce(mt.system_change_status_date, DATE '1970-01-01') = coalesce(ms.system_change_status_date, DATE '1970-01-01')
     AND coalesce(mt.system_change_status_date, DATE '1970-01-02') = coalesce(ms.system_change_status_date, DATE '1970-01-02'))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (system_productivity_log_id,
        cr_patient_id,
        tumor_id,
        system_user_id_code,
        system_change_status_date,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.system_productivity_log_id, ms.cr_patient_id, ms.tumor_id, ms.system_user_id_code, ms.system_change_status_date, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT system_productivity_log_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_system_productivity_log
      GROUP BY system_productivity_log_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_system_productivity_log');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_System_Productivity_Log');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF