#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Message'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Message'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Message where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Message'||','||CAST(Count(*) AS VARCHAR (20))||','
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
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
# 