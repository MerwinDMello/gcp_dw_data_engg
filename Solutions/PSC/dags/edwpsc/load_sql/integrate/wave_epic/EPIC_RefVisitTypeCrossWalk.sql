
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_RefVisitTypeCrossWalk ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefVisitTypeCrossWalk (SourceAPrimaryKeyValue, DispEncTypeName, VisitTypeRequiredClaim, VisitTypeRequiredCopay, VisitTypePregnancyVisit, VisitTypeActiveFlag, VisitTypeOrthoVisit, VisitTypeObgynVisit, VisitTypeIsVisit, VisitTypeWebVisit, VisitTypePhysicalTherapyVisit)
SELECT source.SourceAPrimaryKeyValue, TRIM(source.DispEncTypeName), source.VisitTypeRequiredClaim, source.VisitTypeRequiredCopay, source.VisitTypePregnancyVisit, source.VisitTypeActiveFlag, source.VisitTypeOrthoVisit, source.VisitTypeObgynVisit, source.VisitTypeIsVisit, source.VisitTypeWebVisit, source.VisitTypePhysicalTherapyVisit
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_RefVisitTypeCrossWalk as source;
