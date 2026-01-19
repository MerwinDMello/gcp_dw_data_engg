BEGIN
DECLARE
DUP_COUNT INT64;
declare  
    current_ts datetime;
  set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot;

BEGIN TRANSACTION;
-- Create metric data 
INSERT INTO {{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot(
Employee_SID, 
Requisition_SID,
Position_SID, 
Date_Id,
Analytics_Msr_Sid,
Snapshot_Date,
Dept_SID,
Job_Class_SID,
Job_Code_SID,
Location_Code,
Coid,
Company_Code,
Functional_Dept_Num,
Sub_Functional_Dept_Num,
Auxiliary_Status_SID,
Employee_Status_SID,
Key_Talent_Id,
Integrated_LOB_Id,
Action_Code, 
Action_Reason_Text, 
Lawson_Company_Num,
Process_Level_Code,
Work_Schedule_Code,
Recruiter_Owner_User_SID,
Requisition_Approval_Date,
Employee_Num,
Metric_Numerator_Qty,
Metric_Denominator_Qty,
Source_System_Code, 
DW_Last_Update_Date_Time)

SELECT
Employee_SID, 
Requisition_SID,
Position_SID, 
Date_Id,
Analytics_Msr_Sid,
date(current_ts) AS Snapshot_Date,
Dept_SID,
Job_Class_SID,
Job_Code_SID,
Location_Code,
Coid,
Company_Code,
Functional_Dept_Num,
Sub_Functional_Dept_Num,
Auxiliary_Status_SID,
Employee_Status_SID,
Key_Talent_Id,
Integrated_LOB_Id,
Action_Code, 
Action_Reason_Text, 
Lawson_Company_Num,
Process_Level_Code,
Work_Schedule_Code,
Recruiter_Owner_User_SID,
Requisition_Approval_Date,
Employee_Num,
Metric_Numerator_Qty,
Metric_Denominator_Qty,
Source_System_Code, 
current_ts AS DW_Last_Update_Date_Time

FROM {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric;

    SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_SID ,Requisition_SID ,Position_SID ,Date_Id ,Analytics_Msr_Sid ,Snapshot_Date 
    FROM
      {{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot
    GROUP BY
      Employee_SID ,Requisition_SID ,Position_SID ,Date_Id ,Analytics_Msr_Sid ,Snapshot_Date 
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot');
  ELSE
COMMIT TRANSACTION;
END IF;

TRUNCATE TABLE {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot;

BEGIN TRANSACTION;
-- Create metric data 
INSERT INTO {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot (
Employee_SID,
Snapshot_Date,
Employee_Num,
Employee_First_Name,
Employee_Last_Name,
Employee_Middle_Name,
Ethnic_Origin_Code,
Gender_Code,
Adjusted_Hire_Date,
Birth_Date,
Acute_Experience_Start_Date,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Code,
DW_Last_Update_Date_Time)

SELECT
Employee_SID,
date(current_ts) AS Snapshot_Date,
Employee_Num,
Employee_First_Name,
Employee_Last_Name,
Employee_Middle_Name,
Ethnic_Origin_Code,
Gender_Code,
Adjusted_Hire_Date,
Birth_Date,
Acute_Experience_Start_Date,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Code,
current_ts AS DW_Last_Update_Date_Time

FROM {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail;

    SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       Employee_SID ,Snapshot_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot
    GROUP BY
       Employee_SID ,Snapshot_Date 
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot');
  ELSE
COMMIT TRANSACTION;
END IF;

END;





