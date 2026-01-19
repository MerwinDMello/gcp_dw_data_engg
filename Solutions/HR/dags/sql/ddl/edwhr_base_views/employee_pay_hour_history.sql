/*EDWHR_BASE_VIEWS.Employee_Pay_Hour_History*/
/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.employee_pay_hour_history AS SELECT
Employee_SID,
Check_Id,
Pay_Summary_Group_Code,
Time_Seq_Id,
Valid_From_Date,
Valid_To_Date,
Employee_Num,
Work_Hour_Amt,
Hourly_Rate_Amt,
Wage_Amt,
Transaction_Date,
Dept_SID,
Payroll_Year_Num,
Home_Process_Level_Code,
GL_Account_Num,
GL_Sub_Account_Num,
GL_Company_Num,
Account_Unit_Num,
Pay_Period_End_Date,
Lawson_Company_Num,
Process_Level_Code,
Delete_Ind,
Active_DW_Ind,
Source_System_Code,
DW_Last_Update_Date_Time
FROM {{ params.param_hr_core_dataset_name }}.employee_pay_hour_history;