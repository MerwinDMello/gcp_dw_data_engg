export JOBNAME='J_REF_REFERRING_PHYSICIAN'

export AC_EXP_SQL_STATEMENT="select 'J_REF_REFERRING_PHYSICIAN'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[DimReferringMD] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_REFERRING_PHYSICIAN' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Referring_Physician"

