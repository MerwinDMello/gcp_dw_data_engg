export JOBNAME='J_NavQue_History_Stg'
export AC_EXP_SQL_STATEMENT="Select 'J_NavQue_History_Stg'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
Select 
NavQHistoryFactID as NavQue_History_Id,
NavQActionDimID as NavQue_Action_Id,
NavQReasonDimID as NavQue_Reason_Id,
TumorTypeDimID as Tumor_Type_Id,
NavigatorDimID as Navigator_Id,
COID as COID,
'H' as Company_Code,
MessageControlID as Message_Control_Id_Text,
MessageDate as Message_Date,
NavQInsertDate as NavQue_Insert_Date,
NavQActionDate as NavQue_Action_Date,
MRN as Medical_Record_Num,
Substring(NetworkURN,4,len(NetworkURN)) as Patient_Market_URN,
substring(NetworkURN,1,3) as Network_Mnemonic_CS,
TOC as Transition_Of_Care_Score_Num,
CASE WHEN NavigatedPatient='Navigated' THEN 'Y'
WHEN  NavigatedPatient='Not Navigated' THEN 'N'
ELSE NULL END AS Navigated_Patient_Ind,
"Source" as Message_Source_Flag,
concat('0x',CONVERT(varchar(50),HBSource,2)) AS Hashbite_SSK,
'N' as Source_System_Code
FROM Navadhoc.dbo.NavQHistory (NOLOCK)
) A"

export AC_ACT_SQL_STATEMENT="Select 'J_NavQue_History_Stg'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.NavQue_History_Stg"
