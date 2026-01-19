export JOBNAME='J_CDM_ADHOC_CA_PROC_LIST'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING 
FROM (
SELECT *
FROM 
(
select 
null as Proc_List_SK,
trim(DC.Proc_Category_Id) as Proc_Category_Id ,
trim(S.Server_SK) as Server_SK ,
trim(ID) as ID,
trim(WrkGrpCode) as WrkGrpCode,
trim(KingdomCode) as KingdomCode,
trim(Kingdom) as Kingdom,
trim(Phylum) as Phylum,
trim(ProcedureName) as ProcedureName,
trim(DBType) as DBType,
trim(STSDup250) as STSDup250,
trim(STSDup30) as STSDup30,
trim(STSTerm250) as STSTerm250,
trim(STSID250) as STSID250,
trim(STSTerm30) as STSTerm30,
trim(STSID30) as STSID30,
trim(IPCCC) as IPCCC,
trim(CPTCode) as CPTCode,
trim(ICD9Code) as ICD9Code,
trim(RACHS1) as RACHS1,
trim(Price) as Price,
trim(Inactive) as Inactive,
trim(PxMCategory) as PxMCategory,
trim(PxSCategory) as PxSCategory,
trim(PxValve) as PxValve,
trim(PxCABG) as PxCABG,
trim(PxMechSupp) as PxMechSupp,
trim(PxTx) as PxTx,
trim(PxHCSP) as PxHCSP,
trim(PxPacemaker) as PxPacemaker,
trim(PxVAD) as PxVAD,
trim(ACCProc) as ACCProc,
trim(PxNOS) as PxNOS,
trim(PxModifier) as PxModifier,
trim(AristleBasicLvl) as AristleBasicLvl,
trim(AristleBasicScore) as AristleBasicScore,
trim(STSTerm30_SP) as STSTerm30_SP,
trim(STSID30_SP) as STSID30_SP,
trim(ABTSCode) as ABTSCode,
trim(STATScore) as STATScore,
trim(STATCategory) as STATCategory,
trim(STSRankOrder30) as STSRankOrder30,
trim(STSDup32) as STSDup32,
trim(STSTerm32) as STSTerm32,
trim(STSID32) as STSID32,
trim(STSBaseTerm30) as STSBaseTerm30,
trim(STSTerm32_SP) as STSTerm32_SP,
trim(STSID32_SP) as STSID32_SP,
trim(STSBaseTerm32) as STSBaseTerm32,
trim(STSTerm33) as STSTerm33,
trim(STSID33) as STSID33,
trim(STSDup33) as STSDup33,
trim(STSTerm33_SP) as STSTerm33_SP,
trim(STSID33_SP) as STSID33_SP,
trim(STSBaseTerm33) as STSBaseTerm33,
trim(STG.Server_Name) as Server_Name,
trim(STG.Full_Server_NM) as Full_Server_NM,
STG.DW_Last_Update_Date_Time as DW_Last_Update_Date_Time
from EDWCDM_STAGING.CardioAccess_ProcedureList_STG Stg 
Left Outer Join Ref_CA_Proc_Category DC 
on trim(STG.PxMCategory) = trim(DC.Proc_Category_Name) 
And trim(STG.PxSCategory) = trim(DC.Proc_Sub_Category_Name)
inner join EDWCDM.CA_Server S 
on trim(Stg.Full_Server_NM) = trim(S.Server_Name)
) P
)Q;"

export AC_ACT_SQL_STATEMENT="
 select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING 
 FROM (Select * from  EDWCDM.CA_PROC_LIST)a;"

