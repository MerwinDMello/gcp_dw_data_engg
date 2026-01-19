set -x

export View_Name=vwWCTPInboundMessages
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
JOBNAME1=${Job_Name}_${DBname}
export JOBNAME=`echo $JOBNAME1|cut -c1-100`
export SQL_IMOBILE_DB='SQL_IMOBILE'
#export SQL_IMOBILE_USER='ImobileDataLoad_User'
#export SQL_IMOBILE_PASSWORD='U$sBe0XI95'

###########EXPECTED###################

export AC_EXP_SQL_STATEMENT="SELECT '${Job_Name}'+'_'+'${DBname}'+ ',' + cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING
FROM (
SELECT [Message_Id]
      ,[Recipient_User_Id]
      ,[Recipient_Username]
      ,[Recipient_Full_Name]
      ,[Recipient_Role]
      ,[Recipient_Unit_Id]
      ,[Recipient_Unit]
      ,[Recipient_Unit_Code]
      ,[Recipient_Hospital]
      ,[Recipient_FacilityCode]
      ,[Sent_Date_Time]
      ,[Received_Date_Time]
      ,[Sender_Information]
      ,[Source_Name]
      ,[Source_Type]
      ,[Sender_Message_Id]
      ,[Sender_Transaction_Id]
      ,[Delivered_Date_Time]
      ,[Read_Date_Time]
      ,[Message_Payload]
      ,[User_Action]
      ,[Notify_When_Queued]
      ,[Notify_When_Delivered]
      ,[Notify_When_Read]
      ,[Message_Status]
      ,[Message_Parse_Status]
      ,[Message_Delivery_Priority]
      ,[InferredUpdate]
      ,[LastTimeStamp]
      ,'$DBname' as Databasename
FROM $DBname.dbo.vwWCTPInboundMessages
where 
(LastTimeStamp > '${From_Date}'
AND LastTimeStamp <= '${To_Date}') OR (InferredUpdate=1)
)a"


##############ACTUAL##################


export TARGET_DIR=/etl/ST/EDWCI/SrcFiles
export AC_ACT_INPUT_FILE='J_MHB_Vw_WCTP_Inbound_Messages_Stg_'${DBname}'.txt' 
export P_ACT_Delimiter='|' 
export P_ACT_Control_Total_Field='1,'
export P_ACT_Number_of_Fields='30'
export P_ACT_Control_Total_Type='1,'
