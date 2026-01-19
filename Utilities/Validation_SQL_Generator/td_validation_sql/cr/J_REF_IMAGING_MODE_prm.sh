export JOBNAME='J_REF_IMAGING_MODE'

export AC_EXP_SQL_STATEMENT="
SELECT	'J_REF_IMAGING_MODE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Imaging_Mode_Desc,
Source_System_Code
FROM EDWCR_Staging.Ref_Imaging_Mode_Stg 
where trim(Imaging_Mode_Desc) not in (sel trim(Imaging_Mode_Desc) from EDWCR.Ref_Imaging_Mode )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_IMAGING_MODE')
) A;"


export AC_ACT_SQL_STATEMENT="select 'J_REF_IMAGING_MODE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Imaging_Mode WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_IMAGING_MODE');"
