export JOBNAME='J_CN_PATIENT_ADDRESS_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_ADDRESS_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(

select 
D.PatientDimID
,PatientAddress1
,PatientAddress2
,PatientCity
,PatientState
,PatientZip
,Housing
,LocalHousingAddress
,OtherLocalHousingAddress
from dbo.DimPatient D
Left Join (
Select   
Housing,
LocalHousingAddress,
OtherLocalHousingAddress,
PatientDimID, 
row_number() over(partition by PatientDimID order by Housing,LocalHousingAddress,OtherLocalHousingAddress desc) as rn
From Navadhoc.dbo.PatientHeme 
) H
ON D.PatientDimID = H.PatientDimID AND H.rn=1
)DimPatient

"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_ADDRESS_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_PATIENT_ADDRESS_STG"


