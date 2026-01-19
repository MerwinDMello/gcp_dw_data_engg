DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_activity_priority.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RAD_ONC_ACTIVITY_PRIORITY                ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DimActivityTransaction 			##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_RAD_ONC_ACTVT_PRIORITY;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DimActivityTransaction');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_activity_priority;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_activity_priority AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(src.activity_priority_desc)) AS activity_priority_sk,
                                     substr(src.activity_priority_desc, 1, 20) AS activity_priority_desc,
                                     src.source_system_code AS source_system_code,
                                     src.dw_last_update_date_time
   FROM
     (SELECT DISTINCT CASE
                          WHEN upper(trim(dhd.activitypriority)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(dhd.activitypriority)
                      END AS activity_priority_desc,
                      'R' AS source_system_code,
                      datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimactivitytransaction AS dhd) AS src
   WHERE src.activity_priority_desc IS NOT NULL ) AS ms ON mt.activity_priority_sk = ms.activity_priority_sk
AND (upper(coalesce(mt.activity_priority_desc, '0')) = upper(coalesce(ms.activity_priority_desc, '0'))
     AND upper(coalesce(mt.activity_priority_desc, '1')) = upper(coalesce(ms.activity_priority_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (activity_priority_sk,
        activity_priority_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.activity_priority_sk, ms.activity_priority_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT activity_priority_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_activity_priority
      GROUP BY activity_priority_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_activity_priority');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RAD_ONC_ACTIVITY_PRIORITY');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;