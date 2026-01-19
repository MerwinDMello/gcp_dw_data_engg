select 'J_CDM_ADHOC_CA_Patient_Abnormality'||','||
Coalesce(cast(count(*) as varchar(20)), 0)||',' as SOURCE_STRING 
FROM EDWCDM.CA_Patient_Abnormality 
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCDM_AC.ETL_JOB_RUN where Job_Name = 'J_CDM_ADHOC_CA_Patient_Abnormality')
