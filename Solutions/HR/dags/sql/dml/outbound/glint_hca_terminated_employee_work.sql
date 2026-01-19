SELECT
  STATUS,
  Employee_Status,
  Employee_Status_Desc,
  Group_Num,
  Division_Num,
  Market_Num,
  HR_Company_Curr,
  COID,
  process_level_home_curr,
  dept_num_home_curr,
  LOB,
  Sub_LOB,
  First_Name,
  Last_Name,
  Employee_Num,
  Employee_3_4_ID,
  Email_Address,
  Anniversary_Date,
  RN_Experience,
  Birthdate,
  Date_Hired,
  Action_Code,
  Action_Reason,
  Personal_Email,
  CAST(div(date_diff(current_date(), BIRTHDATE, DAY), 365) AS INT)AS Age,
  Action_Eff_Date
FROM
  {{ params.param_hr_stage_dataset_name }}.glint_hca_terminated_employee_work
WHERE
  BIRTHDATE >'1920-01-01'
  AND hr_company_curr<>300
ORDER BY
  1,
  2,
  4,
  5,
  6,
  7,
  8,
  10,
  14,
  13