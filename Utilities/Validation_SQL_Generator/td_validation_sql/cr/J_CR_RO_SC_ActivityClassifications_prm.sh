export JOBNAME='J_CR_RO_SC_ActivityClassifications'

export AC_EXP_SQL_STATEMENT="select 'J_CR_RO_SC_ActivityClassifications'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [VarianEDW].[dbo].[SC_ActivityClassifications] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_SC_ActivityClassifications' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_DimResource"

