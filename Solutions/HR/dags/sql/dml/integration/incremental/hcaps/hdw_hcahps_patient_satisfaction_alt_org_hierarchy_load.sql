BEGIN

DECLARE current_ts datetime;
DECLARE DUP_COUNT INT64; 
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
  
TRUNCATE TABLE {{params.param_hr_core_dataset_name}}.patient_satisfaction_alt_org_hierarchy;

BEGIN TRANSACTION;
INSERT INTO {{params.param_hr_core_dataset_name}}.patient_satisfaction_alt_org_hierarchy (company_code, coid, facility_name, group_code, division_code, market_code, parent_coid, parent_coid_cons_facility_code, lob_code, sub_lob_code, client_id, facility_claim_control_num, facility_claim_control_num_name, facility_rollup_sw, fsed_ind, asd_region_name, location_mnemonic_cs, unit_num, survey_start_date, survey_end_date, source_system_code, dw_last_update_date_time)
    SELECT
        f.company_code,
        g.coid,
        g.facility_name,
        substr(g.group_code, 2, 5),
        substr(g.division_code, 2, 5),
        substr(g.market_code, 2, 5),
        g.parent_coid,
        g.parent_coid,
        g.lob_code,
        g.sub_lob_code,
        CAST(g.pg_client_id as INT64),
        g.provider,
        g.ccn_name,
        CAST(g.ccn_flag as INT64),
        g.fsed_flag,
        g.asd_region,
        NULL AS location_mnemonic,
        g.unitno,
        g.survey_start_date AS survey_start_date,
        g.survey_end_date AS survey_end_date,
        '5' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{params.param_hr_stage_dataset_name}}.gallup AS g
        INNER JOIN {{params.param_pub_views_dataset_name}}.fact_facility AS f ON g.coid = f.coid        
  ;



/* Test Unique Primary Index constarint set in Teradata*/
SET DUP_COUNT =(
  select count(*)
  from (
  select Company_Code ,Coid 
  from {{params.param_hr_core_dataset_name}}.patient_satisfaction_alt_org_hierarchy
  group by Company_Code ,Coid 
  having count(*)>1
  )
);
IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE =concat('Duplicates are not allowed in the table : edwhr_copy.Patient_satisfaction_alt_org_hierarchy');
ELSE  
  COMMIT  TRANSACTION;
END IF;



END
