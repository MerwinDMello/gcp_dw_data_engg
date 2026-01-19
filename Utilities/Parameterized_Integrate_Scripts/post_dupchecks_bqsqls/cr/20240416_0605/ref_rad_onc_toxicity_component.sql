DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_toxicity_component.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Rad_Onc_Toxicity_Component                ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_FACTPATIENTTOXICITY 			##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_TOXICITY_COMPONENT;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','stg_FACTPATIENTTOXICITY');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_toxicity_component;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_toxicity_component AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(src.toxicity_component_name)) AS toxicity_component_sk,
                                     substr(src.toxicity_component_name, 1, 50) AS toxicity_component_name,
                                     src.source_system_code AS source_system_code,
                                     src.dw_last_update_date_time
   FROM
     (SELECT DISTINCT stg_factpatienttoxicity.toxicitycomponentname AS toxicity_component_name,
                      'R' AS source_system_code,
                      datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_factpatienttoxicity) AS src
   WHERE src.toxicity_component_name IS NOT NULL ) AS ms ON mt.toxicity_component_sk = ms.toxicity_component_sk
AND (upper(coalesce(mt.toxicity_component_name, '0')) = upper(coalesce(ms.toxicity_component_name, '0'))
     AND upper(coalesce(mt.toxicity_component_name, '1')) = upper(coalesce(ms.toxicity_component_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (toxicity_component_sk,
        toxicity_component_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.toxicity_component_sk, ms.toxicity_component_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT toxicity_component_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_toxicity_component
      GROUP BY toxicity_component_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_toxicity_component');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Rad_Onc_Toxicity_Component');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;