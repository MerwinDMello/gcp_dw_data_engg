select 'J_CR_RO_Fact_rad_Onc_Treatment_History'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.Fact_rad_Onc_Treatment_History
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM edwcr_dmx_ac.ETL_JOB_RUN where Job_Name = 'J_CR_RO_Fact_rad_Onc_Treatment_History')
