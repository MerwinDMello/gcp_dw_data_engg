SELECT 'J_MHB_Message'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
Sel  rdc.rdc_sid  as RDC_SID,
Messages_Id  as MHB_Message_Id
from (
SELECT Messages_Id
      ,Sender_FacilityCode
      ,Sent_Date_Time
      ,Message_Content
      ,Urgent
      ,QuickPick
	  ,databasename
  FROM edwci_staging.vwTextMessages
  GROUP BY Messages_Id
      ,Sender_FacilityCode
      ,Sent_Date_Time
      ,Message_Content
      ,Urgent
      ,QuickPick
	  ,databasename) X
	  inner join Edwci_Base_Views.Ref_MHB_Regional_Data_Center RDC
      on RDC.RDC_Desc =oreplace(X.databasename,'heartbeatDW_','')
	  Left Join 
Edwcl_Base_Views.Clinical_Facility  CF
on  trim(CF.Facility_Mnemonic_CS) = trim(X.Sender_FacilityCode)
) Q