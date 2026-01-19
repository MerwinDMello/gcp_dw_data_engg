DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_physician_detail_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CN_Physician_Detail		 				        #
-- #  TARGET  DATABASE	   	: EDWCR	 						                            #
-- #  SOURCE		   	: EDWCR_STAGING.CN_Physician_Detail_STG_WRK		                #
-- #	                                                                             	#
-- #  INITIAL RELEASE	   	: 								                            #
-- #  PROJECT             	: 	 		    										    #
-- #  ------------------------------------------------------------------------		    #
-- #                                                                              	    #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PHYSICIAN_DETAIL;;
 --' FOR SESSION;;
 /* Insert the records which are having Physician_Id as NULL */ /* Physician_Id need to be generated */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(wrk.physician_name),
                                               upper(wrk.physician_phone_num)) +
     (SELECT coalesce(max(cn_physician_detail.physician_id), 900000) AS physician_id
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail) AS physician_id,
                                     substr(wrk.physician_name, 1, 100) AS physician_name,
                                     wrk.physician_phone_num AS physician_phone_num,
                                     'N' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT trim(cn_physician_detail_stg_wrk.physician_name) AS physician_name,
                      cn_physician_detail_stg_wrk.physician_phone_num
      FROM {{ params.param_cr_stage_dataset_name }}.cn_physician_detail_stg_wrk
      WHERE cn_physician_detail_stg_wrk.physician_id IS NULL ) AS wrk
   LEFT OUTER JOIN
     (SELECT cn_physician_detail.physician_id,
             cn_physician_detail.physician_name,
             cn_physician_detail.physician_phone_num
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
      WHERE cn_physician_detail.physician_id > 900000 ) AS tgt ON upper(rtrim(coalesce(trim(wrk.physician_name), 'XX'))) = upper(rtrim(coalesce(trim(tgt.physician_name), 'XX')))
   AND upper(rtrim(coalesce(trim(wrk.physician_phone_num), 'X'))) = upper(rtrim(coalesce(trim(tgt.physician_phone_num), 'X')))
   WHERE tgt.physician_id IS NULL ) AS ms ON mt.physician_id = ms.physician_id
AND (upper(coalesce(mt.physician_name, '0')) = upper(coalesce(ms.physician_name, '0'))
     AND upper(coalesce(mt.physician_name, '1')) = upper(coalesce(ms.physician_name, '1')))
AND (upper(coalesce(mt.physician_phone_num, '0')) = upper(coalesce(ms.physician_phone_num, '0'))
     AND upper(coalesce(mt.physician_phone_num, '1')) = upper(coalesce(ms.physician_phone_num, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (physician_id,
        physician_name,
        physician_phone_num,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.physician_id, ms.physician_name, ms.physician_phone_num, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT physician_id
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
      GROUP BY physician_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_physician_detail');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the records which are having Physician_Id NOT NULL */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
WHERE cn_physician_detail.physician_id <= 900000;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS mt USING
  (SELECT DISTINCT wrk.physician_id,
                   wrk.physician_name,
                   wrk.physician_phone_num AS physician_phone_num,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_physician_detail_stg_wrk AS wrk
   WHERE wrk.physician_id IS NOT NULL ) AS ms ON mt.physician_id = ms.physician_id
AND (upper(coalesce(mt.physician_name, '0')) = upper(coalesce(ms.physician_name, '0'))
     AND upper(coalesce(mt.physician_name, '1')) = upper(coalesce(ms.physician_name, '1')))
AND (upper(coalesce(mt.physician_phone_num, '0')) = upper(coalesce(ms.physician_phone_num, '0'))
     AND upper(coalesce(mt.physician_phone_num, '1')) = upper(coalesce(ms.physician_phone_num, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (physician_id,
        physician_name,
        physician_phone_num,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.physician_id, ms.physician_name, ms.physician_phone_num, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT physician_id
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
      GROUP BY physician_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_physician_detail');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Physician_Detail');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF