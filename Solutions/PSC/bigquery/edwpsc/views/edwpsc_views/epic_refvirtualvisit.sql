CREATE OR REPLACE VIEW edwpsc_views.`epic_refvirtualvisit`
AS SELECT
  `epic_refvirtualvisit`.virtualvisittype,
  `epic_refvirtualvisit`.virtualvisitdesc,
  `epic_refvirtualvisit`.dwlastupdatedatetime,
  `epic_refvirtualvisit`.sourcesystemcode,
  `epic_refvirtualvisit`.insertedby,
  `epic_refvirtualvisit`.inserteddtm,
  `epic_refvirtualvisit`.modifiedby,
  `epic_refvirtualvisit`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refvirtualvisit`
;