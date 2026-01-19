DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_project.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Sean Wilson
          Date: 1/8/2016
          Name: Ref_CC_Project.sql
       Purpose: Builds the project reference table used within the Business Objects AD-HOC Universe
                for reporting.
          Mod1: Changed script to pull from all establishment levels, then group by to get all related
                projects on 7/5/2017 SW.
	Mod2:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA287;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_project AS x USING
  (SELECT max(refog.company_code) AS company_code,
          max(refog.coid) AS coid,
          mp.id AS project_id,
          TRIM(max(mp.description)) AS project_desc,
          TRIM(max(mp.name)) AS project_name,
          max(CASE
                  WHEN mp.is_work_queue_excludable = 1 THEN 'Y'
                  ELSE 'N'
              END) AS work_queue_excluadable_ind
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_project AS mp
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_project_establishment AS mpe ON mpe.mon_project_id = mp.id
   AND mpe.schema_id = mp.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment_org AS seo ON mpe.sec_establishment_id = seo.establishment_id
   AND mpe.schema_id = seo.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS refog ON refog.org_id = seo.org_id
   AND refog.schema_id = seo.schema_id
   GROUP BY upper(refog.company_code),
            upper(refog.coid),
            3,
            upper(mp.description),
            upper(mp.name),
            upper(CASE
                      WHEN mp.is_work_queue_excludable = 1 THEN 'Y'
                      ELSE 'N'
                  END)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.project_id = z.project_id WHEN MATCHED THEN
UPDATE
SET project_name = substr(z.project_name, 1, 100),
    project_desc = substr(z.project_desc, 1, 255),
    work_queue_exclusion_ind = substr(z.work_queue_excluadable_ind, 1, 1),
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        project_id,
        project_name,
        project_desc,
        work_queue_exclusion_ind,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, z.project_id, substr(z.project_name, 1, 100), substr(z.project_desc, 1, 255), substr(z.work_queue_excluadable_ind, 1, 1), 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             project_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_project
      GROUP BY company_code,
               coid,
               project_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_project');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Project');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;