DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_facility.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_FACILITY                           ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.Ref_Facility_Stg    		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_FACILITY;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_Facility_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_facility AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.facility_name))) +
     (SELECT coalesce(max(ref_facility.facility_id), 0) AS id1
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_facility) AS facility_id,
                                     substr(trim(type_stg.facility_name), 1, 255) AS facility_name,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT ref_facility_stg.facility_name,
             ref_facility_stg.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_facility_stg
      WHERE upper(rtrim(ref_facility_stg.facility_name)) NOT IN
          (SELECT upper(trim(ref_facility.facility_name))
           FROM `hca-hin-dev-cur-ops`.edwcr.ref_facility
           WHERE ref_facility.facility_name IS NOT NULL ) ) AS type_stg
   WHERE type_stg.facility_name IS NOT NULL
     AND upper(trim(type_stg.facility_name)) <> '' ) AS ms ON mt.facility_id = ms.facility_id
AND (upper(coalesce(mt.facility_name, '0')) = upper(coalesce(ms.facility_name, '0'))
     AND upper(coalesce(mt.facility_name, '1')) = upper(coalesce(ms.facility_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (facility_id,
        facility_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.facility_id, ms.facility_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT facility_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_facility
      GROUP BY facility_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_facility');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_FACILITY');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF