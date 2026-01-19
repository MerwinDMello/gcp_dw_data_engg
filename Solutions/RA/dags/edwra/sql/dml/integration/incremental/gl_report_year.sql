-- Translation time: 2025-03-24T19:00:36.316296Z
-- Translation job ID: a06adffd-7255-4680-b150-d2aa709466dc
-- Source: gs://eim-parallon-cs-datamig-dev-0002/ra_ddls_bulk_conversion/nm59fE/input/gl_report_year.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/GL_Report_Year.out;;
/*
Script to fix coid entries in GL_REPORT_YEAR table.
*/ BEGIN
SET _ERROR_CODE = 0;

    MERGE INTO
    {{ params.param_parallon_ra_stage_dataset_name }}.gl_report_year AS tgt
    USING
    (
    SELECT
        schema_id, ssc_name, coid, unit_num, facility_name, org_id, 
        cost_export_year, rpt_yr_start_date, rpt_yr_end_date, dw_last_update_date
    FROM
        {{ params.param_parallon_ra_stage_dataset_name }}.gl_report_year_temp AS stg ) AS src
    ON
    tgt.schema_id = src.schema_id
    AND tgt.coid = src.coid
    AND tgt.unit_num = src.unit_num
    AND tgt.org_id = src.org_id
    WHEN MATCHED
    THEN 
        UPDATE 
        SET schema_id = src.schema_id,
		ssc_name = src.ssc_name,
		coid = src.coid,
		unit_num = src.unit_num,
		facility_name = src.facility_name,
		org_id = src.org_id,
		cost_export_year = src.cost_export_year,
		rpt_yr_start_date = src.rpt_yr_start_date,
		rpt_yr_end_date = src.rpt_yr_end_date,
		dw_last_update_date = src.dw_last_update_date
    WHEN NOT MATCHED BY TARGET
    THEN
        INSERT
        (schema_id, ssc_name, coid, unit_num, facility_name, org_id, 
        cost_export_year, rpt_yr_start_date, rpt_yr_end_date, dw_last_update_date)
        VALUES
        (src.schema_id, src.ssc_name, src.coid, src.unit_num, src.facility_name, src.org_id, 
        src.cost_export_year, src.rpt_yr_start_date, src.rpt_yr_end_date, src.dw_last_update_date);


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.gl_report_year
WHERE rtrim(gl_report_year.coid, ' ') IN('08165',
                                         '34241',
                                         '25164');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_parallon_ra_stage_dataset_name }}.gl_report_year (schema_id, ssc_name, coid, unit_num, facility_name, org_id, cost_export_year, rpt_yr_start_date, rpt_yr_end_date, dw_last_update_date)
SELECT gl_report_year.schema_id,
       gl_report_year.ssc_name,
       '08165' AS coid,
       gl_report_year.unit_num,
       gl_report_year.facility_name,
       gl_report_year.org_id,
       gl_report_year.cost_export_year,
       gl_report_year.rpt_yr_start_date,
       gl_report_year.rpt_yr_end_date,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date
FROM {{ params.param_parallon_ra_stage_dataset_name }}.gl_report_year
WHERE rtrim(gl_report_year.coid, ' ') = '08158'
UNION DISTINCT
SELECT gl_report_year.schema_id,
       gl_report_year.ssc_name,
       '34241' AS coid,
       gl_report_year.unit_num,
       gl_report_year.facility_name,
       gl_report_year.org_id,
       gl_report_year.cost_export_year,
       gl_report_year.rpt_yr_start_date,
       gl_report_year.rpt_yr_end_date,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date
FROM {{ params.param_parallon_ra_stage_dataset_name }}.gl_report_year
WHERE rtrim(gl_report_year.coid, ' ') = '34224'
UNION DISTINCT
SELECT -- -added for new coid's as well----
 gl_report_year.schema_id, 
 gl_report_year.ssc_name, 
 '25164' AS coid, 
 gl_report_year.unit_num, 
 gl_report_year.facility_name, 
 gl_report_year.org_id, 
 gl_report_year.cost_export_year, 
 gl_report_year.rpt_yr_start_date, 
 gl_report_year.rpt_yr_end_date, 
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date 
FROM {{ params.param_parallon_ra_stage_dataset_name }}.gl_report_year 
WHERE rtrim(gl_report_year.coid, ' ') = '39385';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;