export JOBNAME='J_REF_DIAGNOSIS_NAVIGATION'
export AC_EXP_SQL_STATEMENT="select 'J_REF_DIAGNOSIS_NAVIGATION'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[DimDiagnosisName]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_DIAGNOSIS_NAVIGATION' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Diagnosis_Navigation"
