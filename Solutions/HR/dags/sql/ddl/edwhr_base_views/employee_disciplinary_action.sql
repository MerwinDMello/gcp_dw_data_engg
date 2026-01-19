/*EDWHR_BASE_VIEWS.Employee*/
/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.employee_disciplinary_action AS SELECT
Employee_SID,
Disciplinary_Ind,
Disciplinary_Action_Num,
Valid_From_Date,
Valid_To_Date,
Disciplinary_Desc,
Creation_Date,
Action_Category_Code,
Report_Date,
Reported_By_Employee_Num,
Reported_By_Name,
Action_Status_Code,
Action_Outcome_Desc,
Action_Outcome_Date,
Days_Out_Cnt,
Department_SID,
Location_Code,
Job_Code_SID,
Comment_Desc,
Supervisor_Employee_Num,
Last_Update_Date,
Last_Update_User_34_Login_Code,
Employee_Num,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Code,
DW_Last_Update_Date_Time
FROM {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action;