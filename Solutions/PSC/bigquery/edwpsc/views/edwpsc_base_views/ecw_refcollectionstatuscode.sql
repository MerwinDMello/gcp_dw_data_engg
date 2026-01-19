CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refcollectionstatuscode`
AS SELECT
  `ecw_refcollectionstatuscode`.collectionstatuscode,
  `ecw_refcollectionstatuscode`.collectionstatusdesc
  FROM
    edwpsc.`ecw_refcollectionstatuscode`
;