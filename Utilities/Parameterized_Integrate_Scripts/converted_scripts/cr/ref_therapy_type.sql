-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_therapy_type.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_THERAPY_TYPE                             ##
-- ##  TARGET  DATABASE	   : EDWCR	 					                        ##
-- ##  SOURCE		   :"EDWCR_staging.Therapy_Type_stg                      		##
-- ##	                                                                            ##
-- ##  INITIAL RELEASE	   : 						                             	##
-- ##  PROJECT            	   : 	 		                         				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_REF_THERAPY_TYPE;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Therapy_Type_stg');
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_therapy_type AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY upper(trim(type_stg.therapy_type_desc))) + (
          SELECT
              coalesce(max(ref_therapy_type.therapy_type_id), 0) AS id1
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_therapy_type
        ) AS therapy_type_id,
        substr(trim(type_stg.therapy_type_desc), 1, 100) AS therapy_type_desc,
        type_stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              therapy_type_stg.therapy_type_desc,
              therapy_type_stg.source_system_code
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.therapy_type_stg
            WHERE upper(trim(therapy_type_stg.therapy_type_desc)) NOT IN(
              SELECT
                  upper(trim(ref_therapy_type.therapy_type_desc))
                FROM
                  `hca-hin-dev-cur-ops`.edwcr.ref_therapy_type
            )
        ) AS type_stg
  ) AS ms
  ON mt.therapy_type_id = ms.therapy_type_id
   AND (upper(coalesce(mt.therapy_type_desc, '0')) = upper(coalesce(ms.therapy_type_desc, '0'))
   AND upper(coalesce(mt.therapy_type_desc, '1')) = upper(coalesce(ms.therapy_type_desc, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (therapy_type_id, therapy_type_desc, source_system_code, dw_last_update_date_time)
      VALUES (ms.therapy_type_id, ms.therapy_type_desc, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_THERAPY_TYPE');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
