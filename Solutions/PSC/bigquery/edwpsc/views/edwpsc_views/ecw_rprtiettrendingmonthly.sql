CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtiettrendingmonthly`
AS SELECT
  `ecw_rprtiettrendingmonthly`.snapshotdate,
  `ecw_rprtiettrendingmonthly`.group,
  `ecw_rprtiettrendingmonthly`.division,
  `ecw_rprtiettrendingmonthly`.market,
  `ecw_rprtiettrendingmonthly`.coid,
  `ecw_rprtiettrendingmonthly`.coidname,
  `ecw_rprtiettrendingmonthly`.practiceiets,
  `ecw_rprtiettrendingmonthly`.openpracticeiets,
  `ecw_rprtiettrendingmonthly`.opendaysofpracticeiets,
  `ecw_rprtiettrendingmonthly`.closedpracticeiets,
  `ecw_rprtiettrendingmonthly`.totalpracticeietsdaystoresolution,
  `ecw_rprtiettrendingmonthly`.practiceietsopen30plusdays,
  `ecw_rprtiettrendingmonthly`.newpracticeiets,
  `ecw_rprtiettrendingmonthly`.newopenpracticeiets,
  `ecw_rprtiettrendingmonthly`.newopendaysofpracticeiets,
  `ecw_rprtiettrendingmonthly`.newclosedpracticeiets,
  `ecw_rprtiettrendingmonthly`.newcloseddaystoresolution,
  `ecw_rprtiettrendingmonthly`.openpracticeietslastmonth,
  `ecw_rprtiettrendingmonthly`.openpracticeietslastmonthclosed30days,
  `ecw_rprtiettrendingmonthly`.practiceietsclosedmonth,
  `ecw_rprtiettrendingmonthly`.practiceietsclosedmonthdaystoresolution,
  `ecw_rprtiettrendingmonthly`.newclaims,
  `ecw_rprtiettrendingmonthly`.numberofweekdays,
  `ecw_rprtiettrendingmonthly`.renderingproviderspeciality,
  `ecw_rprtiettrendingmonthly`.insertedby,
  `ecw_rprtiettrendingmonthly`.inserteddtm,
  `ecw_rprtiettrendingmonthly`.lob,
  `ecw_rprtiettrendingmonthly`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprtiettrendingmonthly`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtiettrendingmonthly`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;