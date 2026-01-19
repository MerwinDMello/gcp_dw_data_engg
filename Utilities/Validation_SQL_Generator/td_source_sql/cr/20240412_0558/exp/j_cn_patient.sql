Select 'J_CN_PATIENT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
STG.Nav_Patient_Id,
STG.Navigator_Id,
Case When TRIM(STG.Coid) is NULL then '-1' else STG.COID end as COID,
'H' as Company_Code,
STG.Patient_market_URN,
STG.Medical_Record_Num,
STG.EMPI_Text,
PD1.Physician_ID as Gynecologist_Physician_Id,
PD2.Physician_ID as Primary_Care_Physician_Id,
CF.Facility_Mnemonic_CS as Facility_Mnemonic_CS,
CF.Network_Mnemonic_CS as Network_Mnemonic_CS,
STG.Nav_Create_Date,
'N' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from
edwcr_staging.CN_Patient_STG STG
Left Outer Join edwcr.CN_Physician_Detail PD1
on
COALESCE(TRIM(STG.Gynecologist),'X') = COALESCE(TRIM(PD1.Physician_Name),'X') and
COALESCE(TRIM(STG.GynecologistPhone),'X') = COALESCE(TRIM(PD1.Physician_Phone_Num),'X')
Left Outer Join edwcr.CN_Physician_Detail PD2
on
COALESCE(TRIM(STG.PrimaryCarePhysician),'XX') = COALESCE(TRIM(PD2.Physician_Name),'XX') and
COALESCE(TRIM(STG.PCPPhone),'XX') = COALESCE(TRIM(PD2.Physician_Phone_Num),'XX')
Left Outer Join edw_pub_views.clinical_Facility CF
on
STG.COID = CF.COID
and CF.Facility_Active_Ind='Y' 
--where STG.COID is NOT NULL
where STG.Nav_Patient_Id not in ( Select Nav_Patient_Id from edwcr.CN_Patient)
qualify row_number() over(partition by STG.Nav_Patient_Id  order by Primary_Care_Physician_Id desc)=1
) SRC