export JOBNAME='J_REF_BIOPSY_RESULT'


export AC_EXP_SQL_STATEMENT="select 'J_REF_BIOPSY_RESULT'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[RefBiopsyResult]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_BIOPSY_RESULT' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Biopsy_Result"
