SELECT 'J_MHB_Ref_MHB_Alert_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
select *from 
(
sel
RDC.RDC_SID ,
UserGroup_Id as MHB_User_Group_Id,
UserGroupName as  User_Group_Name,
COALESCE(CF.Company_Code,'H') as Company_Code ,
coalesce(CF.coid, '99999') as COID,
'B' as Source_System_Code,
current_timestamp(0) as DW_Last_Update_Date_Time
from  edwci_staging.vwUserGroups UG
inner join Edwci_Base_Views.Ref_MHB_Regional_Data_Center RDC
 on RDC.RDC_Desc =oreplace(UG.databasename,'heartbeatDW_','')
 Left Join 
Edwcl_Base_Views.Clinical_Facility  CF
on  trim(CF.Facility_Mnemonic_CS) = trim(UG.UnitCode)
where (RDC.RDC_SID ,UserGroup_Id) not in (sel RDC_SID,
MHB_User_Group_Id from Edwci.Ref_MHB_User_Group)
)X
) Q