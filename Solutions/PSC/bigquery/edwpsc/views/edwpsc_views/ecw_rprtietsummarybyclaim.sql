CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtietsummarybyclaim`
AS SELECT
  `ecw_rprtietsummarybyclaim`.snapshotdate,
  `ecw_rprtietsummarybyclaim`.groupname,
  `ecw_rprtietsummarybyclaim`.divisionname,
  `ecw_rprtietsummarybyclaim`.marketname,
  `ecw_rprtietsummarybyclaim`.coidlob,
  `ecw_rprtietsummarybyclaim`.coidsublob,
  `ecw_rprtietsummarybyclaim`.coid,
  `ecw_rprtietsummarybyclaim`.coidname,
  `ecw_rprtietsummarybyclaim`.claimkey,
  `ecw_rprtietsummarybyclaim`.claimnumber,
  `ecw_rprtietsummarybyclaim`.ageindays,
  `ecw_rprtietsummarybyclaim`.errortype,
  `ecw_rprtietsummarybyclaim`.subcategoryname,
  `ecw_rprtietsummarybyclaim`.placeofservicecode,
  `ecw_rprtietsummarybyclaim`.totalbalance,
  `ecw_rprtietsummarybyclaim`.ieterrorcount,
  `ecw_rprtietsummarybyclaim`.claimunbilledstatuskey,
  `ecw_rprtietsummarybyclaim`.unbilledstatuscategory,
  `ecw_rprtietsummarybyclaim`.unbilledstatussubcategory,
  `ecw_rprtietsummarybyclaim`.servicingproviderid,
  `ecw_rprtietsummarybyclaim`.servicingprovidername,
  `ecw_rprtietsummarybyclaim`.servicingprovidernpi,
  `ecw_rprtietsummarybyclaim`.insertedby,
  `ecw_rprtietsummarybyclaim`.inserteddtm,
  `ecw_rprtietsummarybyclaim`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprtietsummarybyclaim`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtietsummarybyclaim`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;