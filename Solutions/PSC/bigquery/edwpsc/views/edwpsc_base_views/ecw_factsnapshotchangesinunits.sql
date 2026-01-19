CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factsnapshotchangesinunits`
AS SELECT
  `ecw_factsnapshotchangesinunits`.unitskey,
  `ecw_factsnapshotchangesinunits`.monthid,
  `ecw_factsnapshotchangesinunits`.snapshotdate,
  `ecw_factsnapshotchangesinunits`.coid,
  `ecw_factsnapshotchangesinunits`.regionkey,
  `ecw_factsnapshotchangesinunits`.cptid,
  `ecw_factsnapshotchangesinunits`.differenceunitsvalue,
  `ecw_factsnapshotchangesinunits`.differencechargesvalue,
  `ecw_factsnapshotchangesinunits`.foundinpreviousmonth,
  `ecw_factsnapshotchangesinunits`.deletedline,
  `ecw_factsnapshotchangesinunits`.dwlastupdatedatetime,
  `ecw_factsnapshotchangesinunits`.claimlinechargekey,
  `ecw_factsnapshotchangesinunits`.claimkey,
  `ecw_factsnapshotchangesinunits`.cptchargesamt,
  `ecw_factsnapshotchangesinunits`.cptunits
  FROM
    edwpsc.`ecw_factsnapshotchangesinunits`
;