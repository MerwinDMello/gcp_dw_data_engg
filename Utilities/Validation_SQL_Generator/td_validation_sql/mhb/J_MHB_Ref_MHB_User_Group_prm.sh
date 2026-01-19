
 #  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_User_Group'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_User_Group'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.Ref_MHB_User_Group where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_Alert_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
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
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#    