
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT * FROM (
select  
DISTINCT
 NULL as Patient_Case_SK,
SERVER_SK,
TRIM(CaseNumber) AS CaseNumber,
 TRIM(HospitalizationID) AS HospitalizationID,
A.Patient_SK AS PatID,
 TRIM(HospitalID) AS HospitalID,
 TRIM(STSVendorID) AS STSVendorID,
 TRIM(AParticID) AS AParticID, 
 TRIM(CParticID) AS CParticID,
 TRIM(TParticID) AS TParticID,
 TRIM(OpType) AS OpType, 
 TRIM(DataBaseType) AS DataBaseType,
 SurgDt AS SurgDt , 
 TRIM(OREntryT) AS OREntryT, 
 TRIM(SIStartT) AS SIStartT, 
 TRIM(SIStopT) AS SIStopT, 
 TRIM(ORExitT) AS ORExitT, 
 ECLSOffDT AS ECLSOffDT, 
 TRIM(HeightCm) AS HeightCm, 
 TRIM(WeightKg) AS WeightKg , 
 TRIM(MultiDay) AS MultiDay,
 TRIM(IMedAprot) AS IMedAprot,
 TRIM(IMedEACA) AS IMedEACA, 
 TRIM(IMedDesmo) AS IMedDesmo, 
 TRIM(IMedTran) AS IMedTran, 
 TRIM(IMedAprotD) AS IMedAprotD,
 TRIM(IMedEACAD) AS IMedEACAD, 
 TRIM(IMedDesmoD) ASIMedDesmoD , 
 TRIM(IMedTranD) AS IMedTranD, 
 TRIM(ProcLoc) AS ProcLoc,
 TRIM(ReOpInAdm) AS ReOpInAdm ,
 TRIM(PrvCtOpN) AS PrvCtOpN, 
 TRIM(PrvOCtOpN) AS PrvOCtOpN, 
 TRIM(XClampTmNC) AS XClampTmNC,
 TRIM(Surgeon) AS Surgeon, 
 TRIM(AsstSurgeon) AS AsstSurgeon, 
 TRIM(Resident) AS Resident ,
 TRIM(PrimAnesName) AS PrimAnesName,
 TRIM(SecAnes) AS SecAnes, 
 TRIM(FelRes) AS FelRes, 
 TRIM(CRNA) AS CRNA, 
 TRIM(NonCVPhys) AS NonCVPhys, 
 TRIM(SecAnesID2) AS SecAnesID2, 
 TRIM(CRNAID) AS CRNAID ,
 TRIM(Nurse) AS Nurse, 
 TRIM(Perfusionist) AS Perfusionist, 
 TRIM(PhysicianAssistant) AS PhysicianAssistant, 
 TRIM(IBdProd) AS IBdProd,
 TRIM(IBdProdRef) AS IBdProdRef, 
 TRIM(IBdRBC) AS IBdRBC, 
 TRIM(IBdRBCDE) AS IBdRBCDE, 
 TRIM(IBdRBCU) AS IBdRBCU, 
 TRIM(IBdRBCM) AS IBdRBCM, 
 TRIM(IBdFFP) AS IBdFFP,
 TRIM(IBdFFPDE) AS IBdFFPDE , 
 TRIM(IBdFFPU) AS IBdFFPU , 
 TRIM(IBdFFPM) AS IBdFFPM , 
 TRIM(IBdCryo) AS IBdCryo, 
 TRIM(IBdCryoDE) AS IBdCryoDE , 
 TRIM(IBdCryoU) AS IBdCryoU, 
 TRIM(IBdCryoM) AS IBdCryoM, 
 TRIM(IBdPlat) AS IBdPlat,
 TRIM(IBdPlatDE) AS IBdPlatDE,
 TRIM(IBdPlatU) AS IBdPlatU,
 TRIM(IBdPlatM) AS IBdPlatM, 
 TRIM(IBdWB) AS IBdWB, 
 TRIM(IBdWBDE) AS IBdWBDE, 
 TRIM(IBdWBFresh) ASIBdWBFresh ,
 TRIM(IBdWBU) AS IBdWBU,
 TRIM(IBdWBM) AS IBdWBM, 
 TRIM(IBdFVIIa) AS IBdFVIIa ,
 TRIM(IBdFVIIaD) AS IBdFVIIaD,
 TRIM(CDataVrsn) AS CDataVrsn, 
 TRIM(ADataVrsn) AS ADataVrsn,
 TRIM(TDataVrsn) AS TDataVrsn,
 TRIM(ReasonForSupportID) AS ReasonForSupportID, 
 TRIM(ECLSModeID) AS ECLSModeID , 
 TRIM(ECLSOpenChest) AS ECLSOpenChest, 
 TRIM(ECLSBloodPrime) AS ECLSBloodPrime ,
 TRIM(EquipUsedMembLungID) AS EquipUsedMembLungID ,
 TRIM(EquipUsedHeatExchID) AS EquipUsedHeatExchID,
 TRIM(EquipUsedPumpID) AS EquipUsedPumpID,
 TRIM(EquipUsedHemofilterID) AS EquipUsedHemofilterID, 
 TRIM(PumpFlowUnitsID) AS PumpFlowUnitsID, 
 TRIM(PumpFlow4thHourFlow) AS PumpFlow4thHourFlow ,
 TRIM(PumpFlow24thHourFlow) AS PumpFlow24thHourFlow, 
 CreateDate  AS CreateDate,
 UpdateDate  AS UpdateDate, 
 TRIM(UpdateBy) AS UpdateBy, 
 TRIM(ECLSUniqueID) AS ECLSUniqueID , 
 TRIM(ECLSRunNumber) AS ECLSRunNumber,
 TRIM(EventGUID) AS EventGUID, 
 TRIM(BloodGasGUID) AS BloodGasGUID,
 TRIM(HemodynamicGUID) AS HemodynamicGUID, 
 TRIM(VentSettingGUID) AS VentSettingGUID ,
  TRIM(OpStatus) AS OpStatus, 
 TRIM(CHSSElig) AS CHSSElig, 
 TRIM(STSTLink) AS STSTLink, 
 TRIM(CaseLinkNum) AS CaseLinkNum, 
 TRIM(CPBStandBy) AS CPBStandBy, 
 TRIM(Notes) AS Notes , 
 TRIM(BloodLossEst) AS BloodLossEst ,
 TRIM(Specimens) AS Specimens, 
 TRIM(ClinTrial) AS ClinTrial,
 TRIM(ClinTrialPatID) AS ClinTrialPatID, 
 TRIM(AnesPresent) AS AnesPresent, 
 TRIM(CoSurgeon) AS CoSurgeon,
 TRIM(STG.Server_Name) AS Server_Name,
 TRIM(Full_Server_NM) AS Full_Server_NM, 
 STG.DW_Last_Update_Date_Time as DW_Last_Update_Date_Time
 from EDWCDM_STAGING.CardioAccess_Cases_STG  Stg
 Inner Join EDWCDM.CA_Server
 on Stg.Full_Server_Nm = CA_Server.Server_Name
left outer join 
 ( Select Patient_SK,Source_Patient_Id ,Server_Name From
 EDWCDM.CA_Patient  C
 Inner Join EDWCDM.CA_Server S on
 C.Server_SK=S.Server_SK
   ) A
 on Stg.PatId = A.Source_Patient_Id
 and Stg.Full_Server_NM=A.Server_Name )a)b;