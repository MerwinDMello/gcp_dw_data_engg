
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT
         perfusionNumber,
         Tmp_Site_Id,
         Lowest_Core_Tmp_Amt,
         Full_server_Nm		 
FROM (
        SELECT  perfusionNumber,CaseNumber, 1  Tmp_Site_Id, LowCTmpBla as Lowest_Core_Tmp_Amt , Full_server_Nm FROM EDWCDM_STAGING.CardioAccess_Perfusion_STG
union
SELECT perfusionNumber,CaseNumber, 2 Tmp_Site_Id, LowCTmpEso as Lowest_Core_Tmp_Amt,  Full_server_Nm FROM EDWCDM_STAGING.CardioAccess_Perfusion_STG
union
SELECT perfusionNumber,CaseNumber, 3  Tmp_Site_Id, LowCTmpNas as Lowest_Core_Tmp_Amt,  Full_server_Nm FROM EDWCDM_STAGING.CardioAccess_Perfusion_STG
union
SELECT perfusionNumber,CaseNumber, 4  Tmp_Site_Id, LowCTmpRec as Lowest_Core_Tmp_Amt ,  Full_server_Nm FROM EDWCDM_STAGING.CardioAccess_Perfusion_STG
union
SELECT perfusionNumber,CaseNumber,5  Tmp_Site_Id, LowCTmpTym as Lowest_Core_Tmp_Amt,  Full_server_Nm FROM EDWCDM_STAGING.CardioAccess_Perfusion_STG
union
SELECT perfusionNumber,CaseNumber,6  Tmp_Site_Id, LowCTmpOth as Lowest_Core_Tmp_Amt,  Full_server_Nm FROM EDWCDM_STAGING.CardioAccess_Perfusion_STG
)UNIONS
WHERE  Lowest_Core_Tmp_Amt  IS NOT NULL
)b