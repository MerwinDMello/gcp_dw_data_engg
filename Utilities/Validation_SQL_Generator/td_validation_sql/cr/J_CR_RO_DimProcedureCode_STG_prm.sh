export JOBNAME='J_CR_RO_DimProcedureCode_STG'

export AC_EXP_SQL_STATEMENT="select 'J_CR_RO_DIMPATIENT_STG'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [VarianEDW].[EDW].[DimProcedureCode] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_DIMPATIENT_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_DimProcedureCode"

