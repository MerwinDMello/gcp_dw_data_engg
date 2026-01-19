DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_ref_icd_oncology.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_ICD_Oncology			          ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : EDWCR_staging.Ref_ICD_Oncology_stg			   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 								   ##
-- ##  PROJECT            	   : 	 		    				   ##
-- ##  ------------------------------------------------------------------------	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Ref_ICD_Oncology;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_ICD_Oncology_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.ref_icd_oncology
WHERE upper(ref_icd_oncology.icd_oncology_code) NOT IN
    (SELECT upper(ref_icd_oncology_stg.icd_oncology_code) AS icd_oncology_code
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_icd_oncology_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_icd_oncology AS mt USING
  (SELECT DISTINCT stg.icd_oncology_code AS icd_oncology_code,
                   stg.icd_oncology_type_code AS icd_oncology_type_code,
                   stg.icd_oncology_category_type_cd AS icd_oncology_category_type_code,
                   stg.icd_oncology_site_desc,
                   stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_icd_oncology_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_icd_oncology AS rio ON upper(rtrim(stg.icd_oncology_code)) = upper(rtrim(rio.icd_oncology_code))
   WHERE rio.icd_oncology_code IS NULL ) AS ms ON mt.icd_oncology_code = ms.icd_oncology_code
AND mt.icd_oncology_type_code = ms.icd_oncology_type_code
AND mt.icd_oncology_category_type_code = ms.icd_oncology_category_type_code
AND (upper(coalesce(mt.icd_oncology_site_desc, '0')) = upper(coalesce(ms.icd_oncology_site_desc, '0'))
     AND upper(coalesce(mt.icd_oncology_site_desc, '1')) = upper(coalesce(ms.icd_oncology_site_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (icd_oncology_code,
        icd_oncology_type_code,
        icd_oncology_category_type_code,
        icd_oncology_site_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.icd_oncology_code, ms.icd_oncology_type_code, ms.icd_oncology_category_type_code, ms.icd_oncology_site_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT icd_oncology_code,
             icd_oncology_type_code,
             icd_oncology_category_type_code
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_icd_oncology
      GROUP BY icd_oncology_code,
               icd_oncology_type_code,
               icd_oncology_category_type_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_icd_oncology');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Ref_ICD_Oncology');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF