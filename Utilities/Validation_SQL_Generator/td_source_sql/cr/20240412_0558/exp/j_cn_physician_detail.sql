Select 'J_CN_PHYSICIAN_DETAIL'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
ROW_NUMBER() OVER( ORDER BY WRK.Physician_Name,WRK.Physician_Phone_Num)+(SEL COALESCE(MAX(Physician_ID), 900000) AS Physician_ID FROM edwcr.CN_Physician_Detail) as Physician_ID,
WRK.Physician_Name,
WRK.Physician_Phone_Num,
'N' as Source_System_Code,
Current_Timestamp(0) as  DW_Last_Update_Date_Time
from edwcr_staging.CN_Physician_Detail_STG_WRK WRK
Left outer join (Select Physician_Id,Physician_Name,Physician_Phone_Num from edwcr.CN_Physician_Detail where Physician_Id>900000 ) TGT
on 
COALESCE(TRIM(WRK.Physician_Name),'XX')=COALESCE(TRIM(TGT.Physician_Name),'XX') and
COALESCE(TRIM(WRK.Physician_Phone_Num),'X')=COALESCE(TRIM(TGT.Physician_Phone_Num),'X')
where TGT.Physician_ID is NULL and
WRK.Physician_ID is NULL
UNION ALL
Select 
WRK.Physician_ID,
WRK.Physician_Name,
WRK.Physician_Phone_Num,
'N' as Source_System_Code,
Current_Timestamp(0) as  DW_Last_Update_Date_Time
from edwcr_staging.CN_Physician_Detail_STG_WRK WRK
Left outer join (Select Physician_Id,Physician_Name,Physician_Phone_Num from edwcr.CN_Physician_Detail where Physician_Id<=900000 ) TGT
on
COALESCE(TRIM(WRK.Physician_Name),'XX')=COALESCE(TRIM(TGT.Physician_Name),'XX') and
COALESCE(TRIM(WRK.Physician_Phone_Num),'X')=COALESCE(TRIM(TGT.Physician_Phone_Num),'X')
where TGT.Physician_ID is NULL and
WRK.Physician_ID is NOT NULL
) SRC