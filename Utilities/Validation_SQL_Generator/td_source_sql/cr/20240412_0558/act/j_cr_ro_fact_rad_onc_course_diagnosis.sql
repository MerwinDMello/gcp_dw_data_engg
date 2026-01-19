select 'J_CR_RO_Fact_Rad_Onc_Course_Diagnosis'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.Fact_Rad_Onc_Course_Diagnosis
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM edwcr_dmx_ac.ETL_JOB_RUN where Job_Name = 'J_CR_RO_Fact_Rad_Onc_Course_Diagnosis')
