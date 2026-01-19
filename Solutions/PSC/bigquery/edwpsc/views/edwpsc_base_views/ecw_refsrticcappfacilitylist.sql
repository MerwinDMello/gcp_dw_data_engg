CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refsrticcappfacilitylist`
AS SELECT
  `ecw_refsrticcappfacilitylist`.facilitylistkey,
  `ecw_refsrticcappfacilitylist`.uniqueidentifier,
  `ecw_refsrticcappfacilitylist`.contractcontrolnumber,
  `ecw_refsrticcappfacilitylist`.assignedvendor,
  `ecw_refsrticcappfacilitylist`.inventorytype,
  `ecw_refsrticcappfacilitylist`.coid,
  `ecw_refsrticcappfacilitylist`.regionkey,
  `ecw_refsrticcappfacilitylist`.siteid,
  `ecw_refsrticcappfacilitylist`.facilityname,
  `ecw_refsrticcappfacilitylist`.poolexecutivename,
  `ecw_refsrticcappfacilitylist`.pooldirectorname,
  `ecw_refsrticcappfacilitylist`.poolmanagername,
  `ecw_refsrticcappfacilitylist`.effectivedate,
  `ecw_refsrticcappfacilitylist`.termeddate,
  `ecw_refsrticcappfacilitylist`.sourceaprimarykeyvalue,
  `ecw_refsrticcappfacilitylist`.deleteflag,
  `ecw_refsrticcappfacilitylist`.dwlastupdatedatetime,
  `ecw_refsrticcappfacilitylist`.sourcesystemcode,
  `ecw_refsrticcappfacilitylist`.insertedby,
  `ecw_refsrticcappfacilitylist`.inserteddtm,
  `ecw_refsrticcappfacilitylist`.modifiedby,
  `ecw_refsrticcappfacilitylist`.modifieddtm
  FROM
    edwpsc.`ecw_refsrticcappfacilitylist`
;