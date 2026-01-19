DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_test_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		        : EDWCR.REF_TEST_TYPE	                        #
-- #  TARGET  DATABASE	   	: EDWCR	 					#
-- #  SOURCE		   	: EDWCR_staging.REF_TEST_TYPE_STG		#
-- #	                                                                        #
-- #  INITIAL RELEASE	   	:Sandhya Jawagi 08/22/2019 						#
-- #  PROJECT                      : 	 		    			#
-- #  ------------------------------------------------------------------------	#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_REF_TEST_TYPE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','REF_TEST_TYPE_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_test_type AS mt USING
  (SELECT DISTINCT
     (SELECT coalesce(max(ref_test_type.test_type_id), 0)
      FROM {{ params.param_cr_core_dataset_name }}.ref_test_type) + row_number() OVER (
                                                                                       ORDER BY upper(a.test_type_desc),
                                                                                                upper(a.test_sub_type_desc)) AS test_type_id,
                                                                                      a.test_type_desc,
                                                                                      substr(a.test_sub_type_desc, 1, 100) AS test_sub_type_desc,
                                                                                      'N' AS source_system_code,
                                                                                      datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT r.test_type_desc,
             r.test_sub_type_desc
      FROM {{ params.param_cr_stage_dataset_name }}.ref_test_type_stg AS r
      LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_test_type AS rtt ON upper(rtrim(coalesce(trim(r.test_sub_type_desc), '##'))) = upper(rtrim(coalesce(trim(rtt.test_sub_type_desc), '##')))
      AND upper(rtrim(coalesce(trim(r.test_type_desc), '##'))) = upper(rtrim(coalesce(trim(rtt.test_type_desc), '##')))
      WHERE rtt.test_type_id IS NULL
        AND r.test_sub_type_desc IS NOT NULL ) AS a) AS ms ON mt.test_type_id = ms.test_type_id
AND (upper(coalesce(mt.test_type_desc, '0')) = upper(coalesce(ms.test_type_desc, '0'))
     AND upper(coalesce(mt.test_type_desc, '1')) = upper(coalesce(ms.test_type_desc, '1')))
AND (upper(coalesce(mt.test_sub_type_desc, '0')) = upper(coalesce(ms.test_sub_type_desc, '0'))
     AND upper(coalesce(mt.test_sub_type_desc, '1')) = upper(coalesce(ms.test_sub_type_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (test_type_id,
        test_type_desc,
        test_sub_type_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.test_type_id, ms.test_type_desc, ms.test_sub_type_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT test_type_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_test_type
      GROUP BY test_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_test_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','REF_TEST_TYPE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF