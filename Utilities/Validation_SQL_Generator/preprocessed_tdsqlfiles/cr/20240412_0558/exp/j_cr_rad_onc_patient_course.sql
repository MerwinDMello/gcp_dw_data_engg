SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr_staging.STG_DimCourse dp
INNER JOIN edwcr.Ref_Rad_Onc_Site rs ON rs.Source_Site_Id=dp.DimSiteID
LEFT JOIN edwcr.Rad_Onc_Patient ra ON dp.DimPatientID = ra.Source_Patient_Id
AND rs.Site_Sk=ra.Site_SK