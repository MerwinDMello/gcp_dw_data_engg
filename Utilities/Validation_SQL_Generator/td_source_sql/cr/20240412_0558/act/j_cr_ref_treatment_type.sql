select 'J_CR_REF_TREATMENT_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.Ref_CR_Treatment_Type  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CR_REF_TREATMENT_TYPE');