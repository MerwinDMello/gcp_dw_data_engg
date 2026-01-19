export JOBNAME='J_REF_BREAST_CANCER_TYPE_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_BREAST_CANCER_TYPE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from (SELECT distinct GeneticsBRCAType FROM Navadhoc.dbo.PatientGeneticTesting (NOLOCK)
where GeneticsBRCAType is not null)
ab"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_BREAST_CANCER_TYPE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_BREAST_CANCER_TYPE_STG"
