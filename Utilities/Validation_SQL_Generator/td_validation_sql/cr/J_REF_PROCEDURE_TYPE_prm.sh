export JOBNAME='J_REF_PROCEDURE_TYPE'
export AC_EXP_SQL_STATEMENT="SELECT	'J_REF_PROCEDURE_TYPE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Procedure_Type_Desc,
Procedure_Sub_Type_Desc ,
Source_System_Code
FROM EDWCR_Staging.Procedure_Type_stg
where (trim(Procedure_Type_Desc),trim(COALESCE(Procedure_Sub_Type_Desc,'') )) not in (sel trim(Procedure_Type_Desc),trim(COALESCE(Procedure_Sub_Type_Desc,'') ) from EDWCR_BASE_VIEWS.REF_PROCEDURE_TYPE Where Procedure_Type_Desc IS NOT NULL  )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_PROCEDURE_TYPE')
) A;"
export AC_ACT_SQL_STATEMENT="select 'J_REF_PROCEDURE_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_Base_Views.REF_PROCEDURE_TYPE WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_PROCEDURE_TYPE');"
