export JOBNAME='J_REF_TUMOR_TYPE'
export AC_EXP_SQL_STATEMENT="select 'J_REF_TUMOR_TYPE'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[DimTumorType]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_TUMOR_TYPE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Tumor_Type where Source_System_Code='N'"


