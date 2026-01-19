export JOBNAME='J_CR_RO_DIMDRUG_STG'

export AC_EXP_SQL_STATEMENT="select 'J_CR_RO_DIMDRUG_STG'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [VarianEDW].[EDW].[DimDrug] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_DIMDRUG_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_DIMDRUG"

