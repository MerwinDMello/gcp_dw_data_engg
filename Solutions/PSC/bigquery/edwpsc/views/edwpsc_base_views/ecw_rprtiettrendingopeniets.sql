CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprtiettrendingopeniets`
AS SELECT
  `ecw_rprtiettrendingopeniets`.snapshotdate,
  `ecw_rprtiettrendingopeniets`.groupname,
  `ecw_rprtiettrendingopeniets`.divisionname,
  `ecw_rprtiettrendingopeniets`.marketname,
  `ecw_rprtiettrendingopeniets`.coidname,
  `ecw_rprtiettrendingopeniets`.coidlob,
  `ecw_rprtiettrendingopeniets`.totalerrorcount,
  `ecw_rprtiettrendingopeniets`.totalagedays,
  `ecw_rprtiettrendingopeniets`.errorcountover30days,
  `ecw_rprtiettrendingopeniets`.insertedby,
  `ecw_rprtiettrendingopeniets`.inserteddtm,
  `ecw_rprtiettrendingopeniets`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_rprtiettrendingopeniets`
;