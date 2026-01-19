Select
SRC.Physician_Id as Physician_Id,
REPLACE(REPLACE(LTRIM(RTRIM(SRC.Physician_Name)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as Physician_Name,
LTRIM(RTRIM(SRC.Physician_Phone_Num)) as Physician_Phone_Num,
'v_currtimestamp' as dw_last_update_date_time
from
(

 Select
NULL as Physician_Id,
Gynecologist as Physician_Name,
GynecologistPhone as Physician_Phone_Num
from navadhoc.dbo.DimPatient

UNION 

Select
NULL as Physician_Id,
PrimaryCarePhysician as Physician_Name,
PCPPhone as Physician_Phone_Num
from navadhoc.dbo.DimPatient

UNION 

Select 
NULL as Physician_Id,
EndTreatmentPhysician,
NULL as Physician_Phone_Num
from 
navadhoc.dbo.PatientTumor

UNION 

Select 
MedicalSpecialistDimId as Physician_Id,
SpecialistName as Physician_Name,
NULL as Physician_Phone_Num
from
navadhoc.dbo.DimMedicalSpecialist

UNION

Select 
NULL as Physician_Id,
Hematologist,
NULL as Physician_Phone_Num
from 
navadhoc.dbo.PatientHeme

) SRC where (LTRIM(RTRIM(SRC.Physician_Name)) is NOT NULL or LTRIM(RTRIM(SRC.Physician_Phone_Num)) is NOT NULL) and

(LTRIM(RTRIM(SRC.Physician_Name)) <> '' or LTRIM(RTRIM(SRC.Physician_Phone_Num)) <>'');