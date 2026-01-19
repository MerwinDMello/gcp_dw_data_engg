export JOBNAME='J_CN_PERSON'
export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PERSON'||','|| cast(count(*) as varchar(20))||', ' as SOURCE_STRING 
FROM
(
SELECT 
Nav_Patient_Id
,Birth_Date
,First_Name
,Last_Name
,Middle_Name
,Perferred_Name
,Gender_Code
,Preferred_Langauage_Text
,Death_Date
,Patient_Email_Text
,'N'Source_System_Code
,DW_Last_Update_Date_Time
FROM $NCR_STG_SCHEMA.CN_PERSON_Stg
where Nav_Patient_Id not in ( Select Nav_Patient_Id from $EDWCR_BASE_VIEWS.CN_PERSON)

)SRC;"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PERSON'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from $EDWCR_BASE_VIEWS.CN_PERSON
where DW_LAST_UPDATE_DATE_TIME >= (Select max(Job_Start_Date_time) from $NCR_AC_VIEW.ETL_JOB_RUN where Job_Name='J_CN_PERSON');"
