-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_contact_method.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_CONTACT_METHOD                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.REF_CONTACT_METHOD_STG   		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_REF_CONTACT_METHOD;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','REF_CONTACT_METHOD_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_contact_method AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY upper(trim(type_stg.contact_method_desc))) + (
          SELECT
              coalesce(max(ref_contact_method.contact_method_id), 0) AS id1
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_contact_method
        ) AS contact_method_id,
        substr(trim(type_stg.contact_method_desc), 1, 100) AS contact_method_desc,
        type_stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              ref_contact_method_stg.contact_method_desc,
              ref_contact_method_stg.source_system_code
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.ref_contact_method_stg
            WHERE upper(trim(ref_contact_method_stg.contact_method_desc)) NOT IN(
              SELECT
                  upper(trim(ref_contact_method.contact_method_desc))
                FROM
                  `hca-hin-dev-cur-ops`.edwcr.ref_contact_method
                WHERE ref_contact_method.contact_method_desc IS NOT NULL
            )
        ) AS type_stg
      WHERE type_stg.contact_method_desc IS NOT NULL
       AND upper(trim(type_stg.contact_method_desc)) <> ''
  ) AS ms
  ON mt.contact_method_id = ms.contact_method_id
   AND (upper(coalesce(mt.contact_method_desc, '0')) = upper(coalesce(ms.contact_method_desc, '0'))
   AND upper(coalesce(mt.contact_method_desc, '1')) = upper(coalesce(ms.contact_method_desc, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (contact_method_id, contact_method_desc, source_system_code, dw_last_update_date_time)
      VALUES (ms.contact_method_id, ms.contact_method_desc, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_CONTACT_METHOD');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
