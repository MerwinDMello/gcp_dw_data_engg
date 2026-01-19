-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_status.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_Status		                        ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.REF_Status_Stg			##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_REF_Status;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','REF_Status_Stg');
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_status AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY upper(trim(type_stg.status_desc))) + (
          SELECT
              coalesce(max(ref_status.status_id), 0) AS id1
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status
        ) AS status_id,
        substr(type_stg.status_desc, 1, 255) AS status_desc,
        substr(type_stg.status_type_desc, 1, 30) AS status_type_desc,
        type_stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(ref_status_stg.status_desc) AS status_desc,
              trim(ref_status_stg.status_type_desc) AS status_type_desc,
              ref_status_stg.source_system_code AS source_system_code
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.ref_status_stg
            WHERE (upper(trim(ref_status_stg.status_desc)), upper(trim(ref_status_stg.status_type_desc))) NOT IN(
              SELECT AS STRUCT
                  -- --where trim(Status_Desc) not in (sel trim(Status_Desc) from EDWCR.REF_STATUS )   ---20190528--- New coulumn added as per CES-13414--
                  upper(trim(ref_status.status_desc)),
                  upper(trim(ref_status.status_type_desc))
                FROM
                  `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status
            )
        ) AS type_stg
  ) AS ms
  ON mt.status_id = ms.status_id
   AND (upper(coalesce(mt.status_desc, '0')) = upper(coalesce(ms.status_desc, '0'))
   AND upper(coalesce(mt.status_desc, '1')) = upper(coalesce(ms.status_desc, '1')))
   AND (upper(coalesce(mt.status_type_desc, '0')) = upper(coalesce(ms.status_type_desc, '0'))
   AND upper(coalesce(mt.status_type_desc, '1')) = upper(coalesce(ms.status_type_desc, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (status_id, status_desc, status_type_desc, source_system_code, dw_last_update_date_time)
      VALUES (ms.status_id, ms.status_desc, ms.status_type_desc, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_STATUS');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
