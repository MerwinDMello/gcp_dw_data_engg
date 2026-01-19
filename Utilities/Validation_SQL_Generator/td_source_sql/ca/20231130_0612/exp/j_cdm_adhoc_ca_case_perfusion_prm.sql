
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT 
null AS Case_Perfusion_SK  ,
A.Patient_Case_SK AS Patient_Case_SK,
csrv.Server_SK AS Server_SK,
STG.PerfusionNumber AS Source_Case_Perfusion_Id,
STG.CPBTm AS CPB_Bypass_Time_Minute_Num,
STG.XClampTm AS Cross_Clamp_Time_Minute_Num,
STG.DHCATm AS Circulatory_Arrest_Time_Minute_Num,
STG.CoolTime AS Cool_Time_Minute_Num,
STG.RewarmTime AS Rewarm_Time_Minute_Num,
STG.CPerfUtil AS CPerf_Utilized_Id,
STG.CPerfTime AS CPerf_Utilized_Time_Minute_Num,
STG.CPerfCanInn AS  CPerf_Inn_Art_Cann_Site_Id,
STG.CPerfCanRSub AS CPerf_Right_Subcl_Cann_Site_Id,
STG.CPerfCanRAx AS CPerf_Right_Axil_Art_Cann_Site_Id,
STG.CPerfCanRCar AS CPerf_Right_Car_Art_Cann_Site_Id,
STG.CPerfCanLCar AS CPerf_Left_Car_Art_Cann_Site_Id,
STG.CPerfCanSVC AS CPerf_SVC_Cann_Site_Id,
STG.CPerfPer AS CPerf_Period_Num,
STG.CPerfFlow AS CPerf_Flow_Rt_Id,
STG.ABldGasMgt AS  ABGM_Cooling_Id,
STG.HCTPriCircA AS HCT_Prior_Cir_Arrest_Amt,
STG.CplegiaAdmin AS Cplegia_Admin_Id,
STG.CPlegiaDose AS Cplegia_Dose_Num,
STG.CplegiaRatioBS AS Cplegia_BS_Ratio_Num,
STG.CplegiaRatioCS AS Cplegia_Solution_Ratio_Num,
STG.CplegInRtAAR AS Cplegia_AAR_In_Route_Num,
STG.CplegInRtARCO AS Cplegia_ARCO_In_Route_Num,
STG.CplegInRtALCO AS Cplegia_ALCO_In_Route_Num,
STG.CplegInRtRCS AS Cplegia_RCS_In_Route_Num,
STG.CplegSubRtAAR AS Cplegia_AAR_Subq_Route_Num,
STG.CplegSubRtARCO AS Cplegia_ARCO_Subq_Route_Num,
STG.CplegSubRtALCO AS  Cplegia_ALCO_Subq_Route_Num,
STG.CplegSubRtRCS AS Cplegia_RCS_Subq_Route_Num,
STG.CplegTemp AS Cplegia_Tmp_Num,
STG.CplegTotalVol AS Cplegia_Total_Volume_Num,
STG.LngMyoIscInt AS LMII_Minute_Num,
STG.CplegSol AS Cplegia_Solution_Num,
STG.LowHCT AS Lowest_HCT_CPB_Num,
STG.CerebralFlowType AS Cerebral_Flow_Type_Id,
STG.CPBPrimed AS CPB_Blood_Prime_Num,
STG.CplegiaDeliv AS Cplegia_Delivery_Id,
STG.CplegiaType AS Cplegia_Type_Id,
STG.HCTFirst AS First_HCT_CPB_Amt,
STG.HCTLast AS  Last_HCT_CPB_Amt,
STG.HCTPost AS  HCT_Post_CPB_Amt,
STG.Ultrafiltration AS  Ultrafiltration_Performed_Id,
CAST(CAST(STG.CreateDate AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
CAST(CAST(STG.LastUpdate  AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
STG.UpdatedBy AS Updated_By_3_4_Id,
'C' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time
FROM EDWCDM_STAGING.CardioAccess_Perfusion_STG STG
LEFT JOIN (Sel Source_Patient_Case_Num, Server_Name, Patient_Case_SK  From EDWCDM.CA_Patient_Case C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)A
On STG.CaseNumber = A.Source_Patient_Case_Num
and STG.Full_Server_NM = A.Server_Name
INNER JOIN  EDWCDM.CA_SERVER csrv
ON STG.Full_Server_NM  =csrv.Server_Name 
		  
LEFT JOIN EDWCDM.CA_CASE_Perfusion CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Case_Perfusion_Id = STG.PerfusionNumber
where CH.Server_SK is null and CH.Source_Case_Perfusion_Id is null)a)b