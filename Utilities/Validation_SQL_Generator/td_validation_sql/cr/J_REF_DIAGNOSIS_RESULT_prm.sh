export JOBNAME='J_REF_DIAGNOSIS_RESULT'
export AC_EXP_SQL_STATEMENT="select 'J_REF_DIAGNOSIS_RESULT'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[RefDiagnosisResult]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_DIAGNOSIS_RESULT' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Diagnosis_Result"
