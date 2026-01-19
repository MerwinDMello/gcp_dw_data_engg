export JOBNAME='J_REF_NAVIGATOR'

export AC_EXP_SQL_STATEMENT="select 'J_REF_NAVIGATOR'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[DimNavigator]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_NAVIGATOR' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Navigator"
