CREATE OR REPLACE VIEW edwpsc_views.`ecw_refvirtualvisit`
AS SELECT
  `ecw_refvirtualvisit`.virtualvisittype,
  `ecw_refvirtualvisit`.virtualvisitdesc,
  `ecw_refvirtualvisit`.dwlastupdatedatetime,
  `ecw_refvirtualvisit`.sourcesystemcode,
  `ecw_refvirtualvisit`.insertedby,
  `ecw_refvirtualvisit`.inserteddtm,
  `ecw_refvirtualvisit`.modifiedby,
  `ecw_refvirtualvisit`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refvirtualvisit`
;