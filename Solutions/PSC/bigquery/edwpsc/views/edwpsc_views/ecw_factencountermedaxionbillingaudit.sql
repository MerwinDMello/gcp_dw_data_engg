CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencountermedaxionbillingaudit`
AS SELECT
  `ecw_factencountermedaxionbillingaudit`.medaxionbillingauditkey,
  `ecw_factencountermedaxionbillingaudit`.medaxionregionname,
  `ecw_factencountermedaxionbillingaudit`.medaxionlocation,
  `ecw_factencountermedaxionbillingaudit`.servicedatekey,
  `ecw_factencountermedaxionbillingaudit`.servicedatetime,
  `ecw_factencountermedaxionbillingaudit`.casenumberfin,
  `ecw_factencountermedaxionbillingaudit`.patientmrn,
  `ecw_factencountermedaxionbillingaudit`.patientname,
  `ecw_factencountermedaxionbillingaudit`.providername,
  `ecw_factencountermedaxionbillingaudit`.providernpi,
  `ecw_factencountermedaxionbillingaudit`.reporttype,
  `ecw_factencountermedaxionbillingaudit`.eventreason,
  `ecw_factencountermedaxionbillingaudit`.firstfiledate,
  `ecw_factencountermedaxionbillingaudit`.lastfiledate,
  `ecw_factencountermedaxionbillingaudit`.dwlastupdatedatetime,
  `ecw_factencountermedaxionbillingaudit`.sourcesystemcode,
  `ecw_factencountermedaxionbillingaudit`.insertedby,
  `ecw_factencountermedaxionbillingaudit`.inserteddtm,
  `ecw_factencountermedaxionbillingaudit`.modifiedby,
  `ecw_factencountermedaxionbillingaudit`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factencountermedaxionbillingaudit`
;