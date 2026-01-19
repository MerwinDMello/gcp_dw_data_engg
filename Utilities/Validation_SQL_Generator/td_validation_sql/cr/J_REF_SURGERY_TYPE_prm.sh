export JOBNAME='J_REF_SURGERY_TYPE'

export AC_EXP_SQL_STATEMENT="select 'J_REF_SURGERY_TYPE'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[DimSurgeryType]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_SURGERY_TYPE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Surgery_Type"

