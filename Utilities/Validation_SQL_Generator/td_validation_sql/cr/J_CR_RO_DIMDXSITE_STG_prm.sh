export JOBNAME='J_CR_RO_DIMDXSITE_STG'

export AC_EXP_SQL_STATEMENT="select 'J_CR_RO_DIMDXSITE_STG'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [VarianEDW].[EDW].[DimDxSite] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_DIMDXSITE_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_DIMDXSITE"

