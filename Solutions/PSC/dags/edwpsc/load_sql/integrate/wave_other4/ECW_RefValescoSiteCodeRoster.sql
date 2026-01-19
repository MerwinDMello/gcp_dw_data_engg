
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefValescoSiteCodeRoster ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefValescoSiteCodeRoster (SiteCode, SendingApplication, COCID, Speciality, POS, OverrideNeeded, COID_Name, FacilityCOID, AddressLine1, AddressLine2, City, State, Zip, Phone, Tin, PracticeName, Region, EcwSiteID, EcwFacilityName, Division, ActiveStatus, Notes, Created, Modified, DWLastUpdateDateTime)
SELECT TRIM(source.SiteCode), TRIM(source.SendingApplication), TRIM(source.COCID), TRIM(source.Speciality), TRIM(source.POS), TRIM(source.OverrideNeeded), TRIM(source.COID_Name), TRIM(source.FacilityCOID), TRIM(source.AddressLine1), TRIM(source.AddressLine2), TRIM(source.City), TRIM(source.State), TRIM(source.Zip), TRIM(source.Phone), TRIM(source.Tin), TRIM(source.PracticeName), TRIM(source.Region), TRIM(source.EcwSiteID), TRIM(source.EcwFacilityName), TRIM(source.Division), TRIM(source.ActiveStatus), TRIM(source.Notes), source.Created, source.Modified, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefValescoSiteCodeRoster as source;
