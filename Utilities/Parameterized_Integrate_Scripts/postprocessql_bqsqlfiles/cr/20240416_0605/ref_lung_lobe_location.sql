DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_lung_lobe_location.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_LUNG_LOBE_LOCATION                         ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.REF_LUNG_LOBE_LOCATION_Stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_LUNG_LOBE_LOCATION;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','REF_LUNG_LOBE_LOCATION_Stg');
 BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_lung_lobe_location AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.lung_lobe_location_desc))) +
     (SELECT coalesce(max(ref_lung_lobe_location.lung_lobe_location_id), 0) AS id1
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lung_lobe_location) AS lung_lobe_location_id,
                                     substr(trim(type_stg.lung_lobe_location_desc), 1, 255) AS lung_lobe_location_desc,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT trim(ref_lung_lobe_location_stg.lung_lobe_location_desc) AS lung_lobe_location_desc,
             ref_lung_lobe_location_stg.source_system_code AS source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lung_lobe_location_stg
      WHERE upper(trim(ref_lung_lobe_location_stg.lung_lobe_location_desc)) NOT IN
          (SELECT upper(trim(ref_lung_lobe_location.lung_lobe_location_desc))
           FROM `hca-hin-dev-cur-ops`.edwcr.ref_lung_lobe_location) ) AS type_stg) AS ms ON mt.lung_lobe_location_id = ms.lung_lobe_location_id
AND (upper(coalesce(mt.lung_lobe_location_desc, '0')) = upper(coalesce(ms.lung_lobe_location_desc, '0'))
     AND upper(coalesce(mt.lung_lobe_location_desc, '1')) = upper(coalesce(ms.lung_lobe_location_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (lung_lobe_location_id,
        lung_lobe_location_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.lung_lobe_location_id, ms.lung_lobe_location_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT lung_lobe_location_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lung_lobe_location
      GROUP BY lung_lobe_location_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_lung_lobe_location');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_LUNG_LOBE_LOCATION');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF