
export JOBNAME='J_CR_REF_TRANSPLANT_TYPE'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_REF_TRANSPLANT_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM (Sel 'X' as ccnt from
(
sel  distinct Type_Type as Transplant_Type_Name from EDWCR_Staging.stg_PatientHemeTransplant
where Type_Type is not null
) SSC  
Left Outer Join EDWCR_BASE_VIEWS.REF_TRANSPLANT_TYPE RTT
on SSC.Transplant_Type_Name = RTT.Transplant_Type_Name
where RTT.Transplant_Type_Name is null
) iq"

export AC_ACT_SQL_STATEMENT="select 'J_CR_REF_TRANSPLANT_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.REF_TRANSPLANT_TYPE
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


