export JOBNAME='J_CR_RO_SC_Modalities_STG'

export AC_EXP_SQL_STATEMENT="select 'J_CR_RO_SC_Modalities_STG'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [VarianEDW].[dbo].[SC_Modalities] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_SC_Modalities_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_SC_Modalities"

