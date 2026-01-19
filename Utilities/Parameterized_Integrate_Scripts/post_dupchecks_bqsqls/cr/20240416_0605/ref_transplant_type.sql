DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_transplant_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_TRANSPLANT_TYPE                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :       EDWCR_staging.stg_PatientHemeTransplant     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_REF_TRANSPLANT_TYPE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_PatientHemeTransplant');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_transplant_type AS mt USING
  (SELECT DISTINCT
     (SELECT coalesce(max(ref_transplant_type.transplant_type_id), 0) AS maxid
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_transplant_type) + row_number() OVER (
                                                                                            ORDER BY upper(ssc.transplant_type_name)) AS transplant_type_id,
                                                                                           substr(ssc.transplant_type_name, 1, 20) AS transplant_type_name,
                                                                                           'N' AS source_system_code,
                                                                                           datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT stg_patienthemetransplant.type_type AS transplant_type_name
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_patienthemetransplant
      WHERE stg_patienthemetransplant.type_type IS NOT NULL ) AS ssc
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_transplant_type AS rtt ON upper(rtrim(ssc.transplant_type_name)) = upper(rtrim(rtt.transplant_type_name))
   WHERE rtt.transplant_type_name IS NULL ) AS ms ON mt.transplant_type_id = ms.transplant_type_id
AND (upper(coalesce(mt.transplant_type_name, '0')) = upper(coalesce(ms.transplant_type_name, '0'))
     AND upper(coalesce(mt.transplant_type_name, '1')) = upper(coalesce(ms.transplant_type_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (transplant_type_id,
        transplant_type_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.transplant_type_id, ms.transplant_type_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT transplant_type_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_transplant_type
      GROUP BY transplant_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_transplant_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_TRANSPLANT_TYPE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;