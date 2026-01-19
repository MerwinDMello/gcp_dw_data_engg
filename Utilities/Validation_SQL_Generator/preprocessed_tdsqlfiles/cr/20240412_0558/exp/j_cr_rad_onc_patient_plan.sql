SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr_staging.STG_DimPlan dp
INNER JOIN edwcr.Ref_Rad_Onc_Plan_Purpose rpp ON dp.PlanIntent=rpp.Plan_Purpose_Name
INNER JOIN edwcr.Ref_Rad_Onc_Site rr ON rr.Source_Site_Id = dp.DimSiteID
LEFT OUTER JOIN edwcr.Rad_Onc_Patient_Course rpc ON rpc.Source_Patient_Course_Id = dp.DimCourseID
LEFT JOIN edwcr.Rad_Onc_Patient_Plan core ON rr.Site_SK = Core.Site_SK
AND dp.DimPlanID = Core.Source_Patient_Plan_Id
WHERE core.Patient_Plan_SK IS NULL