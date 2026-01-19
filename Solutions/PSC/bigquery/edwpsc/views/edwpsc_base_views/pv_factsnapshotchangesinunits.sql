CREATE OR REPLACE VIEW edwpsc_base_views.`pv_factsnapshotchangesinunits`
AS SELECT
  `pv_factsnapshotchangesinunits`.unitskey,
  `pv_factsnapshotchangesinunits`.monthid,
  `pv_factsnapshotchangesinunits`.snapshotdate,
  `pv_factsnapshotchangesinunits`.coid,
  `pv_factsnapshotchangesinunits`.regionkey,
  `pv_factsnapshotchangesinunits`.practiceid,
  `pv_factsnapshotchangesinunits`.cptid,
  `pv_factsnapshotchangesinunits`.differenceunitsvalue,
  `pv_factsnapshotchangesinunits`.differencechargesvalue,
  `pv_factsnapshotchangesinunits`.foundinpreviousmonth,
  `pv_factsnapshotchangesinunits`.deletedline,
  `pv_factsnapshotchangesinunits`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_factsnapshotchangesinunits`
;