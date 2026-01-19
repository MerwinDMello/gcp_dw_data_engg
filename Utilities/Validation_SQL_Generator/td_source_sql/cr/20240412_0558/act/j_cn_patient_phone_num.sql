select 'J_CN_PATIENT_PHONE_NUM'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.CN_Patient_Phone_Num WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_PHONE_NUM');