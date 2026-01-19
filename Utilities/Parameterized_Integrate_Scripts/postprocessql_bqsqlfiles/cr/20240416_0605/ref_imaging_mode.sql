DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_imaging_mode.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_IMAGING_MODE	                       ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.REF_IMAGING_MODE_Stg			##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_IMAGING_MODE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','REF_IMAGING_MODE_Stg');
 BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_imaging_mode AS mt USING
  (SELECT DISTINCT CAST(CAST(row_number() OVER (
                                                ORDER BY upper(trim(type_stg.imaging_mode_desc))) AS FLOAT64) + CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(
                                                                                                                                                                               (SELECT coalesce(max(ref_imaging_mode.imaging_mode_desc), format('%4d', 0)) AS id1
                                                                                                                                                                                FROM `hca-hin-dev-cur-ops`.edwcr.ref_imaging_mode)) AS FLOAT64) AS INT64) AS imaging_mode_id,
                   substr(trim(type_stg.imaging_mode_desc), 1, 255) AS imaging_mode_desc,
                   type_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT ref_imaging_mode_stg.imaging_mode_desc,
             ref_imaging_mode_stg.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_imaging_mode_stg
      WHERE upper(trim(ref_imaging_mode_stg.imaging_mode_desc)) NOT IN
          (SELECT upper(trim(ref_imaging_mode.imaging_mode_desc))
           FROM `hca-hin-dev-cur-ops`.edwcr.ref_imaging_mode) ) AS type_stg) AS ms ON mt.imaging_mode_id = ms.imaging_mode_id
AND (upper(coalesce(mt.imaging_mode_desc, '0')) = upper(coalesce(ms.imaging_mode_desc, '0'))
     AND upper(coalesce(mt.imaging_mode_desc, '1')) = upper(coalesce(ms.imaging_mode_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (imaging_mode_id,
        imaging_mode_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.imaging_mode_id, ms.imaging_mode_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT imaging_mode_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_imaging_mode
      GROUP BY imaging_mode_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_imaging_mode');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Imaging_Mode');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF