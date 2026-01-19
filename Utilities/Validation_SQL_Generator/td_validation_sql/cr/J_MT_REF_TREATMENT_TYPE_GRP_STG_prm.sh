export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_MT_REF_TREATMENT_TYPE_GRP_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_MT_REF_TREATMENT_TYPE_GRP_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from
(
Select 
 DISTINCT

 CASE WHEN RxType = 'D' THEN 'BIOPSY'
  WHEN RxType = 'S' THEN 'SURGERY'
  WHEN RxType = 'R' THEN 'RAD ONC'
  WHEN RxType = 'C' THEN 'MED ONC'
  WHEN RxType = 'H' THEN 'MED ONC'
  WHEN RxType = 'I' THEN 'MED ONC'
  WHEN RxType = 'T' THEN 'MED ONC'
  WHEN RxType = 'O' THEN 'OTHER'
  WHEN RxType = 'P' THEN 'OTHER'
  ELSE 'NULL' END AS 'iNavigate Group Crosswalk Description'
, RxType as 'Treatment Type Grouping Code'
, CASE WHEN RxType = 'D' THEN 'Surgical Diagnostic/Staging Procedure (Biopsy)'
  WHEN RxType = 'S' THEN 'Surgery'
  WHEN RxType = 'R' THEN 'Radiation'
  WHEN RxType = 'C' THEN 'Chemotherapy'
  WHEN RxType = 'H' THEN 'Hormone Therapy'
  WHEN RxType = 'I' THEN 'Immunotherapy'
  WHEN RxType = 'T' THEN 'Hematologic Transplant and Endocrine Procedures'
  WHEN RxType = 'O' THEN 'Other Treatment'
  WHEN RxType = 'P' THEN 'Palliative Care'
  ELSE 'NULL' END AS 'Treatment Type Grouping Description'
FROM MRegistry.dbo.Treatment
WHERE RxType in ('D','S','R','C','H','I','T','O','P') 
 )A"


export AC_ACT_SQL_STATEMENT="Select 'J_MT_REF_TREATMENT_TYPE_GRP_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_TREATMENT_TYPE_GROUP_STG"




