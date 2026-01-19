DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_physician_specialty.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Physician_Speciality                     ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.Ref_Physician_Speciality_Stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_Ref_Physician_Speciality;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','REF_PHYSICIAN_SPECIALTY_STG');
 BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_physician_specialty AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.physician_specialty_desc))) +
     (SELECT coalesce(max(ref_physician_specialty.physician_specialty_id), 0) AS id1
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_physician_specialty) AS physician_specialty_id,
                                     substr(trim(type_stg.physician_specialty_desc), 1, 100) AS physician_specialty_desc,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT physician_specialty_desc AS physician_specialty_desc,
             ref_physician_specialty_stg.source_system_code AS source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_physician_specialty_stg
      CROSS JOIN UNNEST(ARRAY[ trim(ref_physician_specialty_stg.physician_speciality_desc) ]) AS physician_specialty_desc
      WHERE upper(trim(physician_specialty_desc)) NOT IN
          (SELECT upper(trim(ref_physician_specialty.physician_specialty_desc))
           FROM `hca-hin-dev-cur-ops`.edwcr.ref_physician_specialty) ) AS type_stg) AS ms ON mt.physician_specialty_id = ms.physician_specialty_id
AND (upper(coalesce(mt.physician_specialty_desc, '0')) = upper(coalesce(ms.physician_specialty_desc, '0'))
     AND upper(coalesce(mt.physician_specialty_desc, '1')) = upper(coalesce(ms.physician_specialty_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (physician_specialty_id,
        physician_specialty_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.physician_specialty_id, ms.physician_specialty_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT physician_specialty_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_physician_specialty
      GROUP BY physician_specialty_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_physician_specialty');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table ('EDWCR','REF_PHYSICIAN_SPECIALTY');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;