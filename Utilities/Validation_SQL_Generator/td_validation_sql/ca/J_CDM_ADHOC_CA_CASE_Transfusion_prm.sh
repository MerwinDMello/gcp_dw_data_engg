export JOBNAME='J_CDM_ADHOC_CA_CASE_Transfusion'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT  * FROM (
select 
	 null as Case_Transfusion_SK ,
      A.Patient_Case_SK as Patient_Case_SK ,
      TRIM(EDWCDM.CA_Server.Server_SK) as Server_SK ,
	  TransfusionId ,
      Casenumber ,
      Transfusion ,
      BldProdPRBCDur ,
      BldProdFFPDur ,
      BldProdFreshPDur ,
      BldProdSnglPlatDur ,
      BldProdIndPlatDur ,
      BldProdCryoDur ,
      BldProdFreshWBDur ,
      BldProdWBDur ,
      TransfusBldProdLT24 ,
      BldProdPRBCLT24 ,
      BldProdFFPLT24 ,
      BldProdFreshPLT24 ,
      BldProdSnglPlatLT24 ,
      BldProdIndPlatLT24 ,
      BldProdCryoLT24 ,
      BldProdFreshWBLT24 ,
      BldProdWBLT24 ,
      TransfusBldProdGT24 ,
      BldProdPRBCGT24 ,
      BldProdFFPGT24 ,
      BldProdFreshPGT24 ,
      BldProdSnglPlatGT24 ,
      BldProdIndPlatGT24 ,
      BldProdCryoGT24 ,
      BldProdFreshWBGT24 ,
      BldProdWBGT24 ,
      DirDonorUnits ,
      AutologousTrans ,
      CellSavSal ,
      TransfusBldProdAny ,
      CreateDate ,
      LastUpdate ,
      trim(UpdatedBy) as UpdatedBy,
      BldProdCryoMLBef ,
      BldProdCryoMLGT24 ,
      BldProdCryoMLLT24 ,
      BldProdFFPMLBef ,
      BldProdFFPMLGT24 ,
      BldProdFFPMLLT24 ,
      BldProdFreshPMLBef ,
      BldProdFreshPMLGT24 ,
      BldProdFreshPMLLT24 ,
      BldProdFreshWBMLBef ,
      BldProdFreshWBMLGT24 ,
      BldProdFreshWBMLLT24 ,
      BldProdPlatMLBef ,
      BldProdPlatMLGT24 ,
      BldProdPlatMLLT24 ,
      BldProdPRBCMLBef ,
      BldProdPRBCMLGT24 ,
      BldProdPRBCMLLT24 ,
      BldProdWBMLBef ,
      BldProdWBMLGT24 ,
      BldProdWBMLLT24 ,
      TransfusBldProdBefore ,
      CellSavSalML ,
      trim(STG.Server_Name) as Server_Name ,
      trim(STG.Full_Server_NM) as Full_Server_NM ,
      stg.DW_Last_Update_Date_Time 
	  from EDWCDM_STAGING.CardioAccess_Transfusion_STG Stg
       Inner Join EDWCDM.CA_Server
       on trim(Stg.Full_Server_Nm) = trim(CA_Server.Server_Name)
      left outer join 
      ( Select Patient_Case_SK,Source_Patient_Case_Num ,Server_Name From
        EDWCDM.CA_Patient_Case  C
       Inner Join EDWCDM.CA_Server S on
      C.Server_SK=S.Server_SK
      ) A
      on Stg.CaseNumber = A.Source_Patient_Case_Num 
      and trim(Stg.Full_Server_NM)= trim(A.Server_Name))a)b;"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_CASE_Transfusion)a;"

