SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr_base_views.Ref_Rad_Onc_Treatment_type
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM edwcr_dmx_ac.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_RAD_ONC_TREATMENT_TYPE')