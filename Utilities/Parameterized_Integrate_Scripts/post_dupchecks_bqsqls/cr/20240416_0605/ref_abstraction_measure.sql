DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_abstraction_measure.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Abstraction_Measure                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.Cancer_Abstraction_Values_Stg 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_Ref_Abstraction_Measure;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','Cancer_Abstraction_Values_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_abstraction_measure AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(src.abstraction_field)) +
     (SELECT coalesce(max(ref_abstraction_measure.abstraction_measure_sk), 0)
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_abstraction_measure) AS abstraction_measure_sk,
                                     substr(src.abstraction_field, 1, 50) AS abstraction_measure_name,
                                     'H' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT cancer_abstraction_values_stg.abstraction_field
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.cancer_abstraction_values_stg) AS src
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_abstraction_measure AS tgt ON upper(trim(tgt.abstraction_measure_name)) = upper(trim(src.abstraction_field))
   WHERE tgt.abstraction_measure_name IS NULL ) AS ms ON mt.abstraction_measure_sk = ms.abstraction_measure_sk
AND mt.abstraction_measure_name = ms.abstraction_measure_name
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (abstraction_measure_sk,
        abstraction_measure_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.abstraction_measure_sk, ms.abstraction_measure_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT abstraction_measure_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_abstraction_measure
      GROUP BY abstraction_measure_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_abstraction_measure');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Abstraction_Measure');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;