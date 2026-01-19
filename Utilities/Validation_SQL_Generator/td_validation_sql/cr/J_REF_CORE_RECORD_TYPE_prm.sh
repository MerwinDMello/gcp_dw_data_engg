export JOBNAME='J_REF_CORE_RECORD_TYPE'
export AC_EXP_SQL_STATEMENT="select 'J_REF_CORE_RECORD_TYPE'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[RefCoreRecord]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_CORE_RECORD_TYPE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Core_Record_Type"


