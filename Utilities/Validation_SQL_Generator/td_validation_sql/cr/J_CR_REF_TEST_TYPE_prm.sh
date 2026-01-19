export JOBNAME='J_CR_REF_TEST_TYPE_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CR_REF_TEST_TYPE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
select [TestType] as Test_Sub_Type_Desc, 'Disease Assessment' as Test_Type_Desc FROM [dbo].[PatientHemeDiseaseAssessment]
union
select [TestType] as Test_Sub_Type_Desc, 'Functional Assessment' as Test_Type_Desc FROM [dbo].[PatientHemeFunctionalAssess]

) A"
export AC_ACT_SQL_STATEMENT="Select 'J_CR_REF_TEST_TYPE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_TEST_TYPE_STG"
