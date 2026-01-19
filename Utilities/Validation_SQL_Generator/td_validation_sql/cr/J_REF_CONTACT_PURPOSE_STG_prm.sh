export JOBNAME='J_REF_CONTACT_PURPOSE_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_CONTACT_PURPOSE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from (SELECT Distinct purposeofcontact FROM Navadhoc.dbo.PatientCommunication (NOLOCK)
where purposeofcontact is not null and ltrim(rtrim(purposeofcontact)) <> '')
ab"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_CONTACT_PURPOSE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CONTACT_PURPOSE_STG"
