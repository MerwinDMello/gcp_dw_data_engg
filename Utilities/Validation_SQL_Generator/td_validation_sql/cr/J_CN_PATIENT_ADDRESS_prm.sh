export JOBNAME='J_CN_PATIENT_ADDRESS'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_ADDRESS'||','|| cast(count(*) as varchar(20))||', ' as SOURCE_STRING 
FROM
(
SELECT *  FROM $NCR_STG_SCHEMA.CN_PATIENT_ADDRESS_Stg
where Nav_Patient_Id not in ( Select Nav_Patient_Id from $NCR_TGT_SCHEMA.CN_Patient_Address )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_ADDRESS')

)A;"


export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_ADDRESS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_BASE_VIEWS.CN_Patient_Address WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_ADDRESS');"




