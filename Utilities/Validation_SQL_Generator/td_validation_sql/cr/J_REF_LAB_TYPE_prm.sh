export JOBNAME='J_REF_LAB_TYPE'
export AC_EXP_SQL_STATEMENT="select 'J_REF_LAB_TYPE'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[RefLabType]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_LAB_TYPE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Lab_Type"


