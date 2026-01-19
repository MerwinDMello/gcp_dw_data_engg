DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_contact_purpose.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_CONTACT_PURPOSE                           ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.Contact_Purpose_stg     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_CONTACT_PURPOSE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Contact_Purpose_stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_contact_purpose AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.contact_purpose_desc))) +
     (SELECT coalesce(max(ref_contact_purpose.contact_purpose_id), 0) AS id1
      FROM {{ params.param_cr_core_dataset_name }}.ref_contact_purpose) AS contact_purpose_id,
                                     substr(trim(type_stg.contact_purpose_desc), 1, 100) AS contact_purpose_desc,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT contact_purpose_stg.contact_purpose_desc,
             contact_purpose_stg.source_system_code
      FROM {{ params.param_cr_stage_dataset_name }}.contact_purpose_stg
      WHERE upper(trim(contact_purpose_stg.contact_purpose_desc)) NOT IN
          (SELECT upper(trim(ref_contact_purpose.contact_purpose_desc))
           FROM {{ params.param_cr_core_dataset_name }}.ref_contact_purpose
           WHERE ref_contact_purpose.contact_purpose_desc IS NOT NULL ) ) AS type_stg
   WHERE type_stg.contact_purpose_desc IS NOT NULL
     AND upper(trim(type_stg.contact_purpose_desc)) <> '' ) AS ms ON mt.contact_purpose_id = ms.contact_purpose_id
AND (upper(coalesce(mt.contact_purpose_desc, '0')) = upper(coalesce(ms.contact_purpose_desc, '0'))
     AND upper(coalesce(mt.contact_purpose_desc, '1')) = upper(coalesce(ms.contact_purpose_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (contact_purpose_id,
        contact_purpose_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.contact_purpose_id, ms.contact_purpose_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT contact_purpose_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_contact_purpose
      GROUP BY contact_purpose_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_contact_purpose');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_CONTACT_PURPOSE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF