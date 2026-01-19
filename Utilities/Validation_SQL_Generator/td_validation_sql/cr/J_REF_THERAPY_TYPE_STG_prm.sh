export JOBNAME='J_REF_THERAPY_TYPE_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_THERAPY_TYPE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from (SELECT Distinct AssociateTherapyType FROM PatientComplication (NOLOCK)
where AssociateTherapyType is not null and ltrim(rtrim(AssociateTherapyType)) <> '')a"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_THERAPY_TYPE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.Therapy_Type_stg"
