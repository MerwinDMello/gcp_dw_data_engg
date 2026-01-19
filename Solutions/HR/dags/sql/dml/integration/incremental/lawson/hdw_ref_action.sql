declare ts datetime;
declare dup_count int64;
set ts = datetime_trunc(current_datetime('US/Central'), second);

begin
create temp table persactype_temp as (
SELECT Action_Code
				,Lawson_Company_Num
				,Active_Flag
				,Action_Desc
				,Source_System_Code
FROM(				
SELECT 
TRIM(X.Action_Code) AS Action_Code, 
X.Company AS Lawson_Company_Num, 
cast(X.Active_Flag as int64) AS Active_Flag,
TRIM(COALESCE(X.Description,'Unknown')) AS Action_Desc,
'L' AS Source_System_Code
FROM {{ params.param_hr_stage_dataset_name }}.persactype X)
);
-- UNION ALL

-- SELECT 
-- TRIM(X.Action_Code) AS Action_Code, 
-- X.Company AS Lawson_Company_Num, 
-- X.Active_Flag AS Active_Flag,
-- TRIM(COALESCE(X.Description,'Unknown')) AS Action_Desc,
-- 'A' AS Source_System_Code
-- FROM {{ params.param_hr_stage_dataset_name }}.MSH_PERSACTYPE_STG X

-- UNION ALL

-- SELECT 
-- TRIM(MH.HCA_Action_Cd) AS Action_Code, 
-- 300 AS Lawson_Company_Num, 
-- '1' AS Active_Flag,
-- TRIM(COALESCE(pers.Description,'Unknown')) AS Action_Desc,
-- 'A' AS Source_System_Code
-- FROM {{ params.param_hr_stage_dataset_name }}.MH_TO_HCA_REASON_CODE MH
-- 	LEFT JOIN {{ params.param_hr_stage_dataset_name }}.Persactype pers
-- 	ON pers.Action_Code = MH.HCA_Action_Cd
-- )A

begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_action tgt
using persactype_temp src
on tgt.action_code = src.action_code
and tgt.lawson_company_num = src.lawson_company_num
and tgt.active_flag = src.active_flag
when matched then 
update set
tgt.action_desc = src.action_desc,
tgt.source_system_code = src.source_system_code,
tgt.dw_last_update_date_time = ts
when not matched then
insert(action_code, lawson_company_num, active_flag, action_desc, source_system_code, dw_last_update_date_time)
values(action_code, lawson_company_num, active_flag, action_desc, source_system_code, ts);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       action_code, lawson_company_num, active_flag
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_action
    GROUP BY
       action_code, lawson_company_num, active_flag
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_action');
  ELSE
COMMIT TRANSACTION;
END IF;
END;


