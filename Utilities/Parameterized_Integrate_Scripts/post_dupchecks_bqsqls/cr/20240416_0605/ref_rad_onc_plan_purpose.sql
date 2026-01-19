DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_plan_purpose.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RAD_ONC_PLAN_PURPOSE                ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DimPlan 			##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_RAD_ONC_PLAN_PURPOSE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DimPlan');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_plan_purpose;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_plan_purpose AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(src.plan_purpose_name)) AS plan_purpose_sk,
                                     substr(src.plan_purpose_name, 1, 50) AS plan_purpose_name,
                                     src.source_system_code AS source_system_code,
                                     src.dw_last_update_date_time
   FROM
     (SELECT DISTINCT CASE
                          WHEN upper(trim(dhd.planintent)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(dhd.planintent)
                      END AS plan_purpose_name,
                      'R' AS source_system_code,
                      datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimplan AS dhd) AS src
   WHERE src.plan_purpose_name IS NOT NULL ) AS ms ON mt.plan_purpose_sk = ms.plan_purpose_sk
AND (upper(coalesce(mt.plan_purpose_name, '0')) = upper(coalesce(ms.plan_purpose_name, '0'))
     AND upper(coalesce(mt.plan_purpose_name, '1')) = upper(coalesce(ms.plan_purpose_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (plan_purpose_sk,
        plan_purpose_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.plan_purpose_sk, ms.plan_purpose_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT plan_purpose_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_plan_purpose
      GROUP BY plan_purpose_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_plan_purpose');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RAD_ONC_PLAN_PURPOSE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;