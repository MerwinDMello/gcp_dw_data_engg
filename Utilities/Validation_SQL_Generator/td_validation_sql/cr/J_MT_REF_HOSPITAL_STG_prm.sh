export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_MT_REF_HOSPITAL_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_SITE_LOCATION_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from
(
Select 
row_number() over(order by Hospital_Code1 Asc ) as Hospital_id,
Hospital_Code1,Hospital_Name from
(
select distinct
 case when Facility is null or Facility ='' then cast(-99 as char(10)) else Hospital_Code end as Hospital_Code1,
 case when Facility is null or Facility ='' then 'Unknown Hospital' else Facility end as Hospital_Name from 
(Select
DISTINCT
TreatmentHospID as Hospital_Code
,CASE WHEN t2.FacilityID IS NULL THEN t4.FName ELSE t2.FName END as Facility

FROM
MRegistry.dbo.Treatment t1
LEFT JOIN MDictionary.dbo.Facilities t2
on t1.TreatmentHospID = t2.NODBClientID
LEFT JOIN MDictionary.dbo.GlobalParameters t3
on t1.TreatmentHospID = t3.FIN
LEFT JOIN MDictionary.dbo.Facilities t4
on t3.FacilityID = t4.FacilityID
union
Select
DISTINCT
DrugHospID as Hospital_Code
,CASE WHEN t2.FacilityID IS NULL THEN t4.FName ELSE t2.FName END as Facility

FROM
MRegistry.dbo.Drug t1
LEFT JOIN MDictionary.dbo.Facilities t2
on t1.DrugHospID = t2.NODBClientID
LEFT JOIN MDictionary.dbo.GlobalParameters t3
on t1.DrugHospID = t3.FIN
LEFT JOIN MDictionary.dbo.Facilities t4
on t3.FacilityID = t4.FacilityID
union
Select
DISTINCT
Radiationhospid as Hospital_Code
,CASE WHEN t2.FacilityID IS NULL THEN t4.FName ELSE t2.FName END as Facility

FROM
MRegistry.dbo.Radiation t1
LEFT JOIN MDictionary.dbo.Facilities t2
on t1.Radiationhospid = t2.NODBClientID
LEFT JOIN MDictionary.dbo.GlobalParameters t3
on t1.Radiationhospid = t3.FIN
LEFT JOIN MDictionary.dbo.Facilities t4
on t3.FacilityID = t4.FacilityID
)
a ) b
 )A"


export AC_ACT_SQL_STATEMENT="Select 'J_MT_REF_HOSPITAL_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.Ref_Hospital_Stg"




