export JOBNAME='J_REF_RESULT_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_RESULT_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
Select distinct SurgeryPathResultType as Nav_Result_Desc from Navadhoc.dbo.PatientSurgery (NOLOCK) where SurgeryPathResultType is not null and SurgeryPathResultType <>'' 
  union
  Select distinct SurgeryPathMarginResult as Nav_Result_Desc from Navadhoc.dbo.PatientSurgery (NOLOCK) where SurgeryPathMarginResult is not null and SurgeryPathMarginResult <>'' 
  union
  Select distinct SurgeryPathOncoTypeDxResult as Nav_Result_Desc from Navadhoc.dbo.PatientSurgery (NOLOCK) where SurgeryPathOncoTypeDxResult is not null and SurgeryPathOncoTypeDxResult <>'' 
  UNION
  Select distinct ComplicationOutcome as Nav_Result_Desc  from Navadhoc.dbo.PatientComplication (NOLOCK) where ComplicationOutcome is not null and ComplicationOutcome <>'' 
  UNION
  Select distinct BxPathResultType as Nav_Result_Desc from Navadhoc.dbo.PatientBiopsy (NOLOCK) where BxPathResultType is not null and BxPathResultType <>'' 
  UNION
  Select distinct BxPathMarginResult as Nav_Result_Desc from Navadhoc.dbo.PatientBiopsy (NOLOCK) where BxPathMarginResult is not null and BxPathMarginResult <>'' 
  UNION
  Select distinct BxPathOncoTypeDxResult as Nav_Result_Desc from Navadhoc.dbo.PatientBiopsy (NOLOCK) where BxPathOncoTypeDxResult is not null and BxPathOncoTypeDxResult <>'' ) A"
export AC_ACT_SQL_STATEMENT="Select 'J_REF_RESULT_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_RESULT_STG"
