#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_WCTP_Outbound_Message'


export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM (
SELECT 
RDC.RDC_SID AS RDC_SID,
STG.Message_Id,
COALESCE(CF.Company_Code, 'H') as Company_Code,
coalesce(CF.Coid,'99999') as Coid,
U.User_Login_Name,
coalesce(CF2.Coid,'99999') as Sender_Coid,
coalesce(CFL.Location_Mnemonic_CS,'Unknown') as Sender_Location_Mnemonic_CS,
RMU.MHB_Unit_Id as Sender_MHB_Unit_Id,
USR.MHB_User_Role_Sid  as Sender_MHB_User_Role_SID,
USR.User_Login_Name as Recipient_User_Login_Name,
coalesce(CF.Coid,'99999') as Recipient_Coid,
coalesce(CFL2.Location_Mnemonic_CS,'Unknown') as Recipient_Location_Mnemonic_CS,
RMU2.MHB_Unit_Id as Recipient_MHB_Unit_Id,
USR.MHB_User_Role_Sid,
Coalesce(Ref.WCTP_Destination_Name_SID,-1) as WCTP_Destination_Name_SID,
Sent_Date_Time as Message_Sent_Date_Time,
Delivered_Date_Time as Message_Delivered_Date_Time,
Read_Date_Time as Message_Read_Date_Time,
MessagePayload as Message_Payload_Text,
AcceptedStatus as Accepted_Status_Sw,
InferredUpdate as Inferred_Update_Num,
LastTimeStamp as MHB_Last_Update_Date_Time,
'H' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time

from EDWCI_Staging.vwWCTPOutboundMessages STG


INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_REGIONAL_DATA_CENTER RDC
--ON OREPLACE(TRIM(A.DATABASENAME),'HEARTBEATDW_','')  = TRIM(RDC.RDC_DESC)
ON SUBSTR(TRIM(STG.DATABASENAME),POSITION('_' IN TRIM(STG.DATABASENAME))+1) = TRIM(RDC.RDC_DESC)

Left join Edwcl_Base_Views.Clinical_Facility CF
on  trim(CF.Facility_Mnemonic_CS) = trim(STG.Recipient_FacilityCode)

inner join edwci_base_views.MHB_User U
on stg.Sender_UserName = U.User_Login_Name 
AND RDC.RDC_SID = U.RDC_SID

Left join Edwci_Base_Views.Ref_MHB_User_Role R
on coalesce(trim(stg.Sender_Role),'Unknown') = trim(R.MHB_User_Role_Desc)
and R.MHB_User_Role_Sid = U.MHB_User_Role_Sid

inner join edwci_base_views.MHB_User USR
on stg.Recipient_UserName = USR.User_Login_Name 
AND RDC.RDC_SID = USR.RDC_SID

Left join Edwci_Base_Views.Ref_MHB_User_Role UR
on coalesce(trim(stg.Recipient_Role),'Unknown') = trim(UR.MHB_User_Role_Desc)
and UR.MHB_User_Role_Sid = USR.MHB_User_Role_Sid

Left Join Edwcl_Base_Views.Clinical_Facility CF2
on  trim(CF2.Facility_Mnemonic_CS) = trim(stg.Sender_FacilityCode)

Left join Edwcl_Base_Views.Clinical_Facility_Location CFL
on  trim(CFL.Location_Mnemonic_CS) = trim(SUBSTR(stg.Sender_Unit_Code,POSITION('_' IN Sender_Unit_Code)+1))

Inner join edwci_base_views.Ref_MHB_Unit RMU
on RMU.RDC_Sid = RDC.RDC_SID
and stg.Sender_Unit_Id = RMU.MHB_Unit_Id

Left join Edwcl_Base_Views.Clinical_Facility_Location CFL2
on  trim(CFL2.Location_Mnemonic_CS) = trim(SUBSTR(stg.Recipient_Unit_Code,POSITION('_' IN Recipient_Unit_Code)+1))

Inner join edwci_base_views.Ref_MHB_Unit RMU2
on RMU2.RDC_Sid =RDC.RDC_SID
and stg.Recipient_Unit_Id = RMU2.MHB_Unit_Id

Left Join Edwci_Base_Views.Ref_MHB_WCTP_Destination_Name  Ref 
on Coalesce(STG.DestinationName, 'Unknown') = Ref.WCTP_Destination_Name

where  cast(stg.Sent_Date_Time as date) Between U.Eff_From_Date  and U.Eff_To_Date 
AND cast(stg.Sent_Date_Time as date) Between USR.Eff_From_Date  and USR.Eff_To_Date 

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) A

"

export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from EDWCI.MHB_WCTP_Outbound_Message  where dw_last_update_date_time(date)=current_date
 ) Q"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   