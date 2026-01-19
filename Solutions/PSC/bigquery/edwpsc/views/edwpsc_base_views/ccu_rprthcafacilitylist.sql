CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_rprthcafacilitylist`
AS SELECT
  `ccu_rprthcafacilitylist`.contenttypeid,
  `ccu_rprthcafacilitylist`.facilityname,
  `ccu_rprthcafacilitylist`.designation,
  `ccu_rprthcafacilitylist`.poskey,
  `ccu_rprthcafacilitylist`.coid,
  `ccu_rprthcafacilitylist`.complianceassetid,
  `ccu_rprthcafacilitylist`.id,
  `ccu_rprthcafacilitylist`.contenttype,
  `ccu_rprthcafacilitylist`.modified,
  `ccu_rprthcafacilitylist`.created,
  `ccu_rprthcafacilitylist`.createdbyid,
  `ccu_rprthcafacilitylist`.modifiedbyid,
  `ccu_rprthcafacilitylist`.owshiddenversion,
  `ccu_rprthcafacilitylist`.version,
  `ccu_rprthcafacilitylist`.path,
  `ccu_rprthcafacilitylist`.dwlastupdatedatetime,
  `ccu_rprthcafacilitylist`.insertedby,
  `ccu_rprthcafacilitylist`.inserteddtm
  FROM
    edwpsc.`ccu_rprthcafacilitylist`
;