export JOBNAME='J_CN_PHYSICIAN_DETAIL_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_DETAIL_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from
(

 Select
NULL as Physician_Id,
Gynecologist as Physician_Name,
GynecologistPhone as Physician_Phone_Num
from DimPatient

UNION ALL

Select
NULL as Physician_Id,
PrimaryCarePhysician as Physician_Name,
PCPPhone as Physician_Phone_Num
from DimPatient

UNION ALL

Select 
NULL as Physician_Id,
EndTreatmentPhysician,
NULL as Physician_Phone_Num
from 
PatientTumor

UNION ALL

Select 
MedicalSpecialistDimId as Physician_Id,
SpecialistName as Physician_Name,
NULL as Physician_Phone_Num
from
DimMedicalSpecialist

) SRC where (LTRIM(RTRIM(SRC.Physician_Name)) is NOT NULL or LTRIM(RTRIM(SRC.Physician_Phone_Num)) is NOT NULL) and

(LTRIM(RTRIM(SRC.Physician_Name)) <> '' or LTRIM(RTRIM(SRC.Physician_Phone_Num)) <>'')"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_DETAIL_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Physician_Detail_STG"




