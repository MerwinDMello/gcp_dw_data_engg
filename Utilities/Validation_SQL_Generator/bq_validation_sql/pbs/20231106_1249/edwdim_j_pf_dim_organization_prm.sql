##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

DECLARE expected_value, actual_value, difference NUMERIC;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

DECLARE exp_values_list, act_values_list ARRAY<STRING>;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = Null;

SET sourcesysnm = ''; -- This needs to be added

SET srcdataset_id = (select arr[offset(1)] from (select split("{{ params.param_pbs_stage_dataset_name }}" , '.') as arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (select arr[offset(1)] from (select split("{{ params.param_pbs_core_dataset_name }}" , '.') as arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET exp_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT trim(format('%20d', count(*))) AS source_string
FROM
  (SELECT dim_org_stg_mthy.coid,
          dim_org_stg_mthy.aso_bso_storage_code,
          dim_org_stg_mthy.org_name_parent,
          dim_org_stg_mthy.org_name_child,
          dim_org_stg_mthy.org_alias_name,
          dim_org_stg_mthy.org_coid_alias_name,
          dim_org_stg_mthy.alias_table_name,
          dim_org_stg_mthy.consolidation_code,
          dim_org_stg_mthy.storage_code,
          dim_org_stg_mthy.two_pass_calc_code,
          dim_org_stg_mthy.formula_text,
          dim_org_stg_mthy.member_solve_order_num,
          dim_org_stg_mthy.org_level_uda_name,
          dim_org_stg_mthy.org_hier_name
   FROM {{ params.param_pbs_stage_dataset_name }}.dim_org_stg_mthy
   UNION DISTINCT SELECT dim_org_stg_mthy_pf_t3185_dnd.coid,
                         dim_org_stg_mthy_pf_t3185_dnd.aso_bso_storage_code,
                         dim_org_stg_mthy_pf_t3185_dnd.org_name_parent,
                         dim_org_stg_mthy_pf_t3185_dnd.org_name_child,
                         dim_org_stg_mthy_pf_t3185_dnd.org_alias_name,
                         dim_org_stg_mthy_pf_t3185_dnd.org_coid_alias_name,
                         dim_org_stg_mthy_pf_t3185_dnd.alias_table_name,
                         dim_org_stg_mthy_pf_t3185_dnd.consolidation_code,
                         dim_org_stg_mthy_pf_t3185_dnd.storage_code,
                         dim_org_stg_mthy_pf_t3185_dnd.two_pass_calc_code,
                         dim_org_stg_mthy_pf_t3185_dnd.formula_text,
                         dim_org_stg_mthy_pf_t3185_dnd.member_solve_order_num,
                         dim_org_stg_mthy_pf_t3185_dnd.org_level_uda_name,
                         dim_org_stg_mthy_pf_t3185_dnd.org_hier_name
   FROM {{ params.param_pbs_stage_dataset_name }}.dim_org_stg_mthy_pf_t3185_dnd
   UNION DISTINCT SELECT dim_esb_organization_homehealth_dnd.coid,
                         dim_esb_organization_homehealth_dnd.aso_bso_storage_code,
                         dim_esb_organization_homehealth_dnd.org_name_parent,
                         dim_esb_organization_homehealth_dnd.org_name_child,
                         dim_esb_organization_homehealth_dnd.org_alias_name,
                         dim_esb_organization_homehealth_dnd.org_coid_alias_name,
                         dim_esb_organization_homehealth_dnd.alias_table_name,
                         dim_esb_organization_homehealth_dnd.consolidation_code,
                         dim_esb_organization_homehealth_dnd.storage_code,
                         dim_esb_organization_homehealth_dnd.two_pass_calc_code,
                         dim_esb_organization_homehealth_dnd.formula_text,
                         dim_esb_organization_homehealth_dnd.member_solve_order_num,
                         dim_esb_organization_homehealth_dnd.org_level_uda_name,
                         dim_esb_organization_homehealth_dnd.org_hier_name
   FROM {{ params.param_pbs_stage_dataset_name }}.dim_esb_organization_homehealth_dnd) AS aa ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT trim(format('%20d', count(*))) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.dim_esb_organization ;)
);

SET idx_length = (SELECT ARRAY_LENGTH(act_values_list));

LOOP
  SET idx = idx + 1;

  IF idx > idx_length THEN
    BREAK;
  END IF;

  SET expected_value = CAST(exp_values_list[ORDINAL(idx)] AS NUMERIC);
  SET actual_value = CAST(act_values_list[ORDINAL(idx)] AS NUMERIC);

  SET difference = 
    CASE 
    WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64)
    WHEN expected_value = 0 and actual_value = 0 Then 0
    ELSE actual_value
    END;

  SET audit_status = 
  CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
  END;

  IF idx = 1 THEN
    SET audit_type = "RECORD_COUNT";
  ELSE
    SET audit_type = CONCAT("VALIDATION_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO "{{ params.param_pbs_audit_dataset_name }}".audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, job_name, audit_time, audit_status
   );

END LOOP;
