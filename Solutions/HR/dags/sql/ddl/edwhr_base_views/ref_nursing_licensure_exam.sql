CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_nursing_licensure_exam AS
SELECT  
Exam_Id,
Exam_Name,
Exam_Desc,
Source_System_Code,
DW_Last_Update_Date_Time
FROM {{ params.param_hr_core_dataset_name }}.ref_nursing_licensure_exam;