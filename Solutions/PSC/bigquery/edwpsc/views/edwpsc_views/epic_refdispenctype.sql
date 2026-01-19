CREATE OR REPLACE VIEW edwpsc_views.`epic_refdispenctype`
AS SELECT
  `epic_refdispenctype`.dispenctypekey,
  `epic_refdispenctype`.dispenctypename,
  `epic_refdispenctype`.dispenctypetitle,
  `epic_refdispenctype`.dispenctypeabbr,
  `epic_refdispenctype`.dispenctypeinternalid,
  `epic_refdispenctype`.dispenctypec,
  `epic_refdispenctype`.regionkey,
  `epic_refdispenctype`.sourceaprimarykey,
  `epic_refdispenctype`.dwlastupdatedatetime,
  `epic_refdispenctype`.sourcesystemcode,
  `epic_refdispenctype`.insertedby,
  `epic_refdispenctype`.inserteddtm,
  `epic_refdispenctype`.modifiedby,
  `epic_refdispenctype`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refdispenctype`
;