-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_side.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE        : EDWCR.Ref_Side		                        	   ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : EDWCR_STAGING.Ref_Side_STG				   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 							      	   ##
-- ##  PROJECT             : 	 		    				  	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_Ref_Side;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_Side_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_side AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY upper(trim(src.side_desc))) + (
          SELECT
              coalesce(max(ref_side.side_id), 0) AS id1
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_side
        ) AS side_id,
        substr(trim(src.side_desc), 1, 30) AS side_desc,
        src.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              ref_side_stg.side_desc,
              ref_side_stg.source_system_code
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.ref_side_stg
            WHERE upper(trim(ref_side_stg.side_desc)) NOT IN(
              SELECT
                  upper(trim(ref_side.side_desc))
                FROM
                  `hca-hin-dev-cur-ops`.edwcr.ref_side
                WHERE ref_side.side_desc IS NOT NULL
            )
        ) AS src
      WHERE src.side_desc IS NOT NULL
       AND upper(rtrim(src.side_desc)) <> ''
  ) AS ms
  ON mt.side_id = ms.side_id
   AND (upper(coalesce(mt.side_desc, '0')) = upper(coalesce(ms.side_desc, '0'))
   AND upper(coalesce(mt.side_desc, '1')) = upper(coalesce(ms.side_desc, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (side_id, side_desc, source_system_code, dw_last_update_date_time)
      VALUES (ms.side_id, ms.side_desc, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Side');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
