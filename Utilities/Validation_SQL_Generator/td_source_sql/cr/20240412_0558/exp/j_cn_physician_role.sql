Select 'J_CN_PHYSICIAN_ROLE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from
(
Select 
Physician_Id
,Physician_Role_Code
from edwcr_staging.CN_Physician_Role_Stg 
Union	
Select 	
Physician_Id	,
'Gyn'	
From edwcr.CN_Physician_Detail 	
inner join edwcr.CN_patient	
On Physician_Id=Gynecologist_Physician_Id	
Union	
Select 	
Physician_Id,	
'PCP'	
From edwcr.CN_Physician_Detail 	
inner join edwcr.CN_patient	
On Physician_Id=Primary_Care_Physician_Id	
Union	
Select 	
Physician_Id,	
'ETP'	
From edwcr.CN_Physician_Detail 	
inner join edwcr.CN_Patient_Tumor 
On Physician_Id=Treatment_End_Physician_Id
)ab where (Physician_Id,Physician_Role_Code) not in ( Select Physician_Id,Physician_Role_Code from edwcr.CN_PHYSICIAN_ROLE ) 
