select 'J_CN_PATIENT_HEME_TRANSPLANT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.CN_PATIENT_HEME_TRANSPLANT
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM edwcr_dmx_ac.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_HEME_TRANSPLANT')
