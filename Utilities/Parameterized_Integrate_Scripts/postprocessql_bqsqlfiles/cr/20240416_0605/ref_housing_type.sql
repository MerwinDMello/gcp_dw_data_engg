DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_housing_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE			: 	EDWCR.Ref_Housing_Type             		#
-- #  TARGET  DATABASE	   	: 	EDWCR		 				#
-- #  SOURCE		   	: 	EDWCR_STAGING.Ref_Housing_Type_stg		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             		:
-- #  Created by			:       Syntel   					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_Ref_Housing_Type;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','Ref_Housing_Type_stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert data into Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_housing_type AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(dis.housing_type_name)) + coalesce(
                                                                                          (SELECT max(coalesce(a.housing_type_id, 0)) AS max_key
                                                                                           FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_housing_type AS a), 0) AS housing_type_id,
                                     substr(trim(dis.housing_type_name), 1, 100) AS housing_type_name,
                                     'N' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT trim(st.housing_type_name) AS housing_type_name
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_housing_type_stg AS st
      WHERE st.housing_type_name IS NOT NULL ) AS dis
   WHERE NOT EXISTS
       (SELECT 1
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_housing_type AS rcdc
        WHERE upper(trim(rcdc.housing_type_name)) = upper(trim(dis.housing_type_name)) ) ) AS ms ON mt.housing_type_id = ms.housing_type_id
AND (upper(coalesce(mt.housing_type_name, '0')) = upper(coalesce(ms.housing_type_name, '0'))
     AND upper(coalesce(mt.housing_type_name, '1')) = upper(coalesce(ms.housing_type_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (housing_type_id,
        housing_type_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.housing_type_id, ms.housing_type_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT housing_type_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_housing_type
      GROUP BY housing_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_housing_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Collect Stats on Core Table */ -- CALL dbadmin_procs.collect_stats_table ('edwcr','Ref_Housing_Type');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

---- EOF