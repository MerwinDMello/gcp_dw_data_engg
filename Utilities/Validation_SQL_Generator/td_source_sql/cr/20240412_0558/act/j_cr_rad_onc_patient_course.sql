select 'J_CR_Rad_Onc_Patient_Course'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.Rad_Onc_Patient_Course
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM edwcr_dmx_ac_base_views.ETL_JOB_RUN where Job_Name = 'J_CR_Rad_Onc_Patient_Course')
