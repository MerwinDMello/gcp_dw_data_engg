SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT Row_Number() OVER (
 ORDER BY Cast(dp.DimSiteID AS Int),
 Cast(dp.DimProcedureCodeID AS Int)) AS Procedure_Code_SK,
 rtt.Treatment_Type_SK,
 rr.Site_SK,
 DimProcedureCodeID,
 ProcedureCode,
 Description,
 ProcedureCodeDescription,
 ActiveInd,
 Log_Id,
 Run_Id,
 'R' AS Source_System_Code,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT Cast(DimSiteID AS Int) AS DimSiteID,
 Cast(DimProcedureCodeID AS Int) AS DimProcedureCodeID,
 ProcedureCode,
 Description,
 ProcedureCodeDescription,
 ActiveInd,
 Cast(LogID AS Int) AS Log_Id,
 Cast(RunID AS Int) AS Run_Id
 FROM EDWCR_Staging.stg_DimProcedureCode) dp
 LEFT OUTER JOIN
 (SELECT DISTINCT Treatment_Type,
 Treatment_Category,
 Procedure_Code
 FROM EDWCR_Staging.stg_SC_Modalities) sm ON sm.Procedure_Code=dp.ProcedureCode
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Rad_Onc_Treatment_type rtt ON sm.Treatment_Category= rtt.Treatment_Category_Desc
 AND sm.Treatment_Type=rtt.Treatment_Type_Desc
 INNER JOIN EDWCR_BASE_VIEWS.Ref_Rad_Onc_Site rr ON dp.DimSiteID=rr.Source_Site_Id)STG