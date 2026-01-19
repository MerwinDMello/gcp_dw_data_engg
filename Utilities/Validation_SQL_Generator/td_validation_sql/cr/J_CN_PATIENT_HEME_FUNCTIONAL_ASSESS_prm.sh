
export JOBNAME='J_CN_PATIENT_HEME_FUNCTIONAL_ASSESS'

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
EDWCR_Staging.CN_Patient_Heme_Functional_Assess_STG STG

LEFT JOIN EDWCR_Base_Views.Ref_Test_Type Ref 
on  Ref.Test_Sub_Type_Desc =STG.TestType 
and Ref.Test_Type_Desc='Functional Assessment'

where STG.HBSource NOT IN (
SELECT 
Hashbite_SSK
from EDWCR.CN_Patient_Heme_Func_Assess)"

export AC_ACT_SQL_STATEMENT="select '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_Patient_Heme_Func_Assess
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


