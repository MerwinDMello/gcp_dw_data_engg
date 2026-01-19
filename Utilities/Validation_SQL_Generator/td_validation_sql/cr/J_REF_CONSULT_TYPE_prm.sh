export JOBNAME='J_REF_CONSULT_TYPE'
export AC_EXP_SQL_STATEMENT="select 'J_REF_CONSULT_TYPE'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[RefConsultType]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_CONSULT_TYPE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Consult_Type"


