#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Group_Chat_Message'


export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM (
SELECT
RDC.RDC_SID AS RDC_SID,
U10.User_Login_Name AS Recipient_User_Login_Name,
STG.Messages_Id AS MHB_Group_Chat_Message_Id,
STG.Sent_Date_Time AS Message_Sent_Date_Time,
coalesce(CK.Patient_DW_Id,999999999999999999) AS Patient_DW_ID,
COALESCE(CK.Company_Code,'H') AS Company_Code,
coalesce(CK.Coid,'99999') AS Coid,
U20.User_Login_Name AS Sender_User_Login_Name,
RMU2.MHB_Unit_Id AS Sender_MHB_Unit_Id,
coalesce(CF22.Coid, '99999') AS Sender_Coid,
coalesce(CFL5.Location_Mnemonic_CS,'Unknown') AS Sender_Location_Mnemonic_CS,
U20.MHB_User_Role_Sid AS Sender_MHB_User_Role_SID,
STG.Sender_Platform AS Sender_Platform_Name,
STG.Group_Chat_Name AS Group_Chat_Name,
STG.Group_Chat_DisplayName AS Group_Chat_Display_Name,
CASE WHEN STG.QuickPick = 'TRUE' THEN 'Y' WHEN STG.QuickPick = 'FALSE' THEN 'N' END AS Quick_Pick_Ind,
CASE WHEN STG.Urgent = 'TRUE' THEN 'Y' WHEN STG.Urgent = 'FALSE' THEN 'N' END AS Urgent_Message_Ind,
CAST(TRIM(COALESCE(REGEXP_SUBSTR(STG.Patient_VisitNumber, '[0-9]+'), '999999999999')) AS DECIMAL(12,0)) AS Pat_Acct_Num,
coalesce(CF41.Coid, '99999') AS Patient_Coid,
U10.MHB_User_Role_Sid AS Recipient_MHB_User_Role_SID,
RMU51.MHB_Unit_Id AS Recipient_MHB_Unit_Id,
coalesce(CFL2.Location_Mnemonic_CS,'Unknown') AS Recipient_Location_Mnemonic_CS,
coalesce(CF53.coid, '99999') AS Recipient_Coid,
STG.InferredUpdate AS Inferred_Update_Sw,
STG.LastTimeStamp AS MHB_Last_Enter_Date_Time,
--TRIM(REGEXP_REPLACE(STG.MESSAGE_CONTENT, '[^a-z A-Z 0-9 ! ?]','',1,0,'i')) AS Message_Content_Text,
TRIM(STG.MESSAGE_CONTENT) AS Message_Content_Text,
STG.Delivered_Date_Time AS Message_Delivered_Date_Time,
STG.Read_Date_Time AS Message_Read_Date_Time,
'H' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time

from EDWCI_Staging.vwGroupChatMessages STG

INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_REGIONAL_DATA_CENTER RDC
ON SUBSTR(TRIM(STG.DATABASENAME),POSITION('_' IN TRIM(STG.DATABASENAME))+1) = TRIM(RDC.RDC_DESC)

inner join edwci_base_views.MHB_User U10
on stg.Recipient_UserName = U10.User_Login_Name
AND RDC.RDC_SID = U10.RDC_SID
--AND U10.Active_DW_Ind = 'Y'

Left join Edwci_Base_Views.Ref_MHB_User_Role R10
on coalesce(trim(stg.Recipient_Role),'Unknown') = trim(R10.MHB_User_Role_Desc)
--and R10.MHB_User_Role_Sid = U10.MHB_User_Role_Sid

Left join  Edw_Pub_Views.Clinical_Facility CF2
on  trim(CF2.Facility_Mnemonic_CS) = STG.Patient_Facility_Code
AND CF2.Facility_Active_Ind = 'Y'

Left join EDWCDM_BASE_VIEWS.Clinical_Acctkeys CK 
on CK.coid = CF2.coid 
and  CK.Pat_Acct_Num = CAST(TRIM(COALESCE(REGEXP_SUBSTR(STG.Patient_VisitNumber, '[0-9]+'), '999999999999')) AS DECIMAL(12,0))

inner join edwci_base_views.MHB_User U20
on stg.Sender_UserName = U20.User_Login_Name
AND RDC.RDC_SID = U20.RDC_SID
--AND U20.Active_DW_Ind = 'Y'

Left join Edwci_Base_Views.Ref_MHB_User_Role R20
on coalesce(trim(stg.Sender_Role),'Unknown') = trim(R20.MHB_User_Role_Desc)
--and R20.MHB_User_Role_Sid = U20.MHB_User_Role_Sid

Inner join edwci_base_views.Ref_MHB_Unit RMU2
on RMU2.RDC_Sid = RDC.RDC_SID
and stg.Sender_Unit_Id = RMU2.MHB_Unit_Id

Left Join Edwcl_Base_Views.Clinical_Facility CF22
on trim(CF22.Facility_Mnemonic_CS) = trim(stg.Sender_FacilityCode)
AND CF22.Facility_Active_Ind = 'Y'

Left Join Edwcl_Base_Views.Clinical_Facility CF41
on  trim(CF41.Facility_Mnemonic_CS) = trim(stg.Patient_Facility_Code)
AND CF41.Facility_Active_Ind = 'Y'

Left join Edwcl_Base_Views.Clinical_Facility_Location CFL5
on trim(CFL5.Location_Mnemonic_CS) = trim(SUBSTR(stg.Sender_Unit_Code,POSITION('_' IN Sender_Unit_Code)+1))
and CFL5.COID = CF41.COID

Inner join edwci_base_views.Ref_MHB_Unit RMU51
on RMU51.RDC_Sid = RDC.RDC_SID
and stg.Recipient_Unit_Id = RMU51.MHB_Unit_Id

Left Join Edwcl_Base_Views.Clinical_Facility CF53
on  trim(CF53.Facility_Mnemonic_CS) = trim(stg.Recipient_FacilityCode)
AND CF53.Facility_Active_Ind = 'Y'

Left join Edwcl_Base_Views.Clinical_Facility_Location CFL2
on trim(CFL2.Location_Mnemonic_CS) = trim(SUBSTR(stg.Recipient_Unit_Code,POSITION('_' IN Recipient_Unit_Code)+1))
and CFL2.COID = CF53.COID

where cast(stg.Sent_Date_Time as date) Between U10.Eff_From_Date and U10.Eff_To_Date
and cast(stg.Sent_Date_Time as date) Between U20.Eff_From_Date and U20.Eff_To_Date

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30) A

"

export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from EDWCI.MHB_Group_Chat_Message where dw_last_update_date_time(date)=current_date
 ) Q"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   