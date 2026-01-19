select 'J_CN_PATIENT_MEDICAL_ONCOLOGY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr_base_views.CN_PATIENT_MEDICAL_ONCOLOGY WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_MEDICAL_ONCOLOGY');