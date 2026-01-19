select 'J_CR_RO_RAD_ONC_ACTVT_PRIORITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.REF_RAD_ONC_ACTIVITY_PRIORITY
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) 
as Job_Start_Date_Time FROM edwcr_dmx_ac.ETL_JOB_RUN where Job_Name = 'J_CR_RO_RAD_ONC_ACTVT_PRIORITY')
