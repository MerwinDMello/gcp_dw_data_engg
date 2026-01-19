set -x

export View_Name=vwPhotoSaveAudits
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
SELECT	Sent_DateTime, Sent_Message_Order_No, From_User_Id, Patient_Id,
		From_UserName, From_User, From_User_Role, From_Unit_Id, From_Unit,
		From_Unit_Code, From_Hospital, From_FacilityCode, To_Group_Id,
		To_Group, Broadcast_Target_Type, To_Unit_Id, To_Unit, To_Unit_Code,
		To_Hospital, To_FacilityCode,cast( Message as varchar(8000))  as Message, Urgent, QuickPick, Patient_Name,
		Patient_MRN, Patient_VisitNumber, Patient_Facility_Code,
		Received_DateTime, Event_ID,Recipient_UserName, Recipient_User_Role, 
        Recipient_FacilityCode,Recipient_Unit_Code,Recipient_Unit_Id,
        '$DBname' As Databasename
FROM $DBname.dbo.vwBroadcastMessages
where 
(LastTimeStamp > '${From_Date}'
AND LastTimeStamp <= '${To_Date}') OR (InferredUpdate=1)
)a"


##############ACTUAL##################


export TARGET_DIR=/etl/ST/EDWCI/SrcFiles
export AC_ACT_INPUT_FILE='J_MHB_Vw_Broadcast_Messages_Stg_'${DBname}'.txt' 
export P_ACT_Delimiter='|' 
export P_ACT_Control_Total_Field='1,'
export P_ACT_Number_of_Fields='35'
export P_ACT_Control_Total_Type='1,'
 