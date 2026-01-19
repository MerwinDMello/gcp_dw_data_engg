DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_eapg_unassigned_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Sean Wilson
          Date: 10/27/2014
          Name: Ref_CC_EAPG_Unassigned_Code.sql
          Purpose: Builds the CC_EAPG_Unassigned_Code table used within the Business Objects AD-HOC Universe
                    for reporting.
          Mod1: Changed target table name due to 30 character limitation on 11/10/2014 SW.
          Mod2: Changed to merge script on 11/11/2014 SW.
          Mod3: Added schema id of 1 on 11/13/2014 SW.
		  Mod4: Add Company Code, Coid  2/19/2016  JC
		  Mod5: Add Coid join  3/15/2016 JC
		Mod6:  -  Added Job name to the query band statement 04062021
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA260;');
 BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_unassigned_code AS x USING
  (SELECT max(rccos.company_code) AS company_code,
          max(rccos.coid) AS coid,
          max(cdef.code) AS eapg_unassigned_code,
          max(cdef.code_name) AS eapg_unassigned_description
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.code_def AS cdef
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = cdef.schema_id
   WHERE upper(cdef.code_type) LIKE 'EAPG%'
     AND upper(cdef.code_name) LIKE 'UNASSIGNED%'
     AND cdef.schema_id = 1
   GROUP BY upper(rccos.company_code),
            upper(rccos.coid),
            upper(cdef.code),
            upper(cdef.code_name)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND upper(rtrim(x.eapg_unassigned_code)) = upper(rtrim(z.eapg_unassigned_code)) WHEN MATCHED THEN
UPDATE
SET eapg_unassigned_desc = z.eapg_unassigned_description,
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        eapg_unassigned_code,
        eapg_unassigned_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, substr(z.eapg_unassigned_code, 1, 10), z.eapg_unassigned_description, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT eapg_unassigned_code
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_unassigned_code
      GROUP BY eapg_unassigned_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_unassigned_code');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_EAPG_Unassigned_Code');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;