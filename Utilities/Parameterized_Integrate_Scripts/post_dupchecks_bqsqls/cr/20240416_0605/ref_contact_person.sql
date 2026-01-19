DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_contact_person.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_CONTACT_PERSON                           ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.Contact_Person_stg     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_CONTACT_PERSON;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Contact_Person_stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_contact_person AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.contact_person_desc))) +
     (SELECT coalesce(max(ref_contact_person.contact_person_id), 0) AS id1
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_contact_person) AS contact_person_id,
                                     substr(trim(type_stg.contact_person_desc), 1, 100) AS contact_person_desc,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT contact_person_stg.contact_person_desc,
             contact_person_stg.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.contact_person_stg
      WHERE upper(trim(contact_person_stg.contact_person_desc)) NOT IN
          (SELECT upper(trim(ref_contact_person.contact_person_desc))
           FROM `hca-hin-dev-cur-ops`.edwcr.ref_contact_person
           WHERE ref_contact_person.contact_person_desc IS NOT NULL ) ) AS type_stg
   WHERE type_stg.contact_person_desc IS NOT NULL
     AND upper(trim(type_stg.contact_person_desc)) <> '' ) AS ms ON mt.contact_person_id = ms.contact_person_id
AND (upper(coalesce(mt.contact_person_desc, '0')) = upper(coalesce(ms.contact_person_desc, '0'))
     AND upper(coalesce(mt.contact_person_desc, '1')) = upper(coalesce(ms.contact_person_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (contact_person_id,
        contact_person_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.contact_person_id, ms.contact_person_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT contact_person_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_contact_person
      GROUP BY contact_person_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_contact_person');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_CONTACT_PERSON');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;