
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPhyNavigationURL ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPhyNavigationURL (dwcRefPhyNavigationURLKey, DefaultAccess, Document_ID, Project, Image_Path, URL)
SELECT source.dwcRefPhyNavigationURLKey, TRIM(source.DefaultAccess), TRIM(source.Document_ID), TRIM(source.Project), TRIM(source.Image_Path), TRIM(source.URL)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPhyNavigationURL as source;
