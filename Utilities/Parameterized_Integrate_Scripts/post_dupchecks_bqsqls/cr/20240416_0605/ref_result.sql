DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_result.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RESULT		                        ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.REF_RESULT_Stg			##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_RESULT;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','REF_RESULT_Stg');
 BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_result AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.nav_result_desc))) +
     (SELECT coalesce(max(ref_result.nav_result_id), 0) AS id1
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_result) AS nav_result_id,
                                     substr(trim(type_stg.nav_result_desc), 1, 100) AS nav_result_desc,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT trim(ref_result_stg.nav_result_desc) AS nav_result_desc,
             ref_result_stg.source_system_code AS source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_result_stg
      WHERE upper(trim(ref_result_stg.nav_result_desc)) NOT IN
          (SELECT upper(trim(ref_result.nav_result_desc))
           FROM `hca-hin-dev-cur-ops`.edwcr.ref_result) ) AS type_stg) AS ms ON mt.nav_result_id = ms.nav_result_id
AND (upper(coalesce(mt.nav_result_desc, '0')) = upper(coalesce(ms.nav_result_desc, '0'))
     AND upper(coalesce(mt.nav_result_desc, '1')) = upper(coalesce(ms.nav_result_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (nav_result_id,
        nav_result_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.nav_result_id, ms.nav_result_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT nav_result_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_result
      GROUP BY nav_result_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_result');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RESULT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;