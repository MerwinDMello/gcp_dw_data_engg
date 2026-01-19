export JOBNAME='J_CDM_ADHOC_CA_Perfusion_Tmp_Site'

export AC_EXP_SQL_STATEMENT="
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

)b"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_Perfusion_Tmp_Site
where DW_Last_Update_Date_Time >= (SELECT Max(Job_Start_date_Time) FROM EDWCDM_DMX_AC.etl_job_run WHERE JOB_NAME = '$JOBNAME' AND Job_Start_date_Time IS NOT NULL )
) b"
