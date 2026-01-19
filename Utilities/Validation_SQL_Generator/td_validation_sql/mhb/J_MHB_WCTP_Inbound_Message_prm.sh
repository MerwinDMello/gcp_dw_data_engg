#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_WCTP_Inbound_Message'

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM (
SELECT
RDC.RDC_SID,
STG.Message_ID,
COALESCE(CF.Company_Code, 'H') as Company_Code,
coalesce(CF.Coid,'99999') as Coid,
U.User_Login_Name,
coalesce(CF.Coid,'99999') as Recipient_Coid,
coalesce(CFL.Location_Mnemonic_CS,'Unknown') as Recipient_Location_Mnemonic_CS,
RMU2.MHB_Unit_Id as Recipient_MHB_Unit_Id,
R.MHB_User_Role_Sid as Recipient_MHB_User_Role_SID,
STG.Sender_Information,
STG.Sender_Message_Id,
STG.Sender_Transaction_Id,
Cast(TRIM(STG.Received_Date_Time) as timestamp(6)) as Message_Received_Date_Time,
Cast(TRIM(STG.Sent_Date_Time) as timestamp(6)) as Message_Sent_Date_Time,
CASE when TRIM(STG.Delivered_Date_Time) = '' then NULL else Cast(TRIM(STG.Delivered_Date_Time) as timestamp(6)) end as Message_Delivered_Date_Time,
CASE when TRIM(STG.Read_Date_Time) = '' then NULL else Cast(TRIM(STG.Read_Date_Time) as timestamp(6)) end as Message_Read_Date_Time,
STG.Message_Status,
STG.Message_Parse_Status,
Coalesce(MDP.Message_Delivery_Priority_SID,-1) as Message_Delivery_Priority_SID,
STG.Message_Payload as Message_Payload_Text,
Coalesce(SN.WCTP_Source_Name_SID,-1) as WCTP_Source_Name_SID,
Coalesce(ST.WCTP_Source_Type_SID,-1) as WCTP_Source_Type_SID,
Coalesce(UAT.User_Action_Type_SID,-1) as User_Action_Type_SID,
STG.Notify_When_Queued as Notify_When_Queued_Sw,
STG.Notify_When_Delivered as Notify_When_Delivered_Sw,
STG.Notify_When_Read as Notify_When_Read_Sw,
STG.InferredUpdate as Inferred_Update_Num,
STG.LastTimeStamp as MHB_Last_Update_Date_Time,
'H' as Source_System_Code,
current_Timestamp(0) as DW_Last_Update_Date_Time
FROM EDWCI_STaging.vwWCTPInboundMessages STG

INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_REGIONAL_DATA_CENTER RDC
--ON OREPLACE(TRIM(A.DATABASENAME),'HEARTBEATDW_','')  = TRIM(RDC.RDC_DESC)
ON SUBSTR(TRIM(STG.DATABASENAME),POSITION('_' IN TRIM(STG.DATABASENAME))+1) = TRIM(RDC.RDC_DESC)

Left join Edwcl_Base_Views.Clinical_Facility CF
on  trim(CF.Facility_Mnemonic_CS) = trim(STG.Recipient_FacilityCode)

inner join edwci_base_views.MHB_User U
on stg.Recipient_UserName = U.User_Login_Name 
AND RDC.RDC_SID = U.RDC_SID

Left join Edwci_Base_Views.Ref_MHB_User_Role R
on coalesce(trim(stg.Recipient_Role),'Unknown') = trim(R.MHB_User_Role_Desc)
and R.MHB_User_Role_Sid = U.MHB_User_Role_Sid

/*Left Join Edwcl_Base_Views.Clinical_Facility CF2
on  trim(CF2.Facility_Mnemonic_CS) = trim(stg.Recipient_FacilityCode)*/

Left join Edwcl_Base_Views.Clinical_Facility_Location CFL
on  trim(CFL.Location_Mnemonic_CS) = trim(SUBSTR(stg.Recipient_Unit_Code,POSITION('_' IN Recipient_Unit_Code)+1))

Inner join edwci_base_views.Ref_MHB_Unit RMU2 
on RMU2.RDC_Sid = RDC.RDC_SID 
and stg.Recipient_Unit_Id = RMU2.MHB_Unit_Id

Left Join Edwci_Base_Views.Ref_MHB_Message_Delivery_Priority  MDP 
on Coalesce(STG.Message_Delivery_Priority, 'Unknown') = MDP.Message_Delivery_Priority_Desc

Left Join Edwci_Base_Views.Ref_MHB_WCTP_Source_Name  SN 
on Coalesce(STG.Source_Name, 'Unknown') = SN.WCTP_Source_Name

Left Join Edwci_Base_Views.Ref_MHB_WCTP_Source_Type ST 
on Coalesce(STG.Source_Type, 'Unknown') = ST.WCTP_Source_Type_Desc

Left Join Edwci_Base_Views.Ref_MHB_User_Action_Type UAT 
on Coalesce(STG.User_Action, 'Unknown') = UAT.User_Action_Type_Desc

where cast(substr(stg.Received_Date_Time,1,10) as date ) Between U.Eff_From_Date  and U.Eff_To_Date 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30

) A
"


export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from EDWCI.MHB_WCTP_Inbound_Message  where dw_last_update_date_time(date)=current_date
 ) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   