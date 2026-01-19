CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refprovidertaxonomycode`
AS SELECT
  `ecw_refprovidertaxonomycode`.taxonomycodekey,
  `ecw_refprovidertaxonomycode`.taxonomycode,
  `ecw_refprovidertaxonomycode`.specialtydesc,
  `ecw_refprovidertaxonomycode`.subspecialtydec,
  `ecw_refprovidertaxonomycode`.dwlastupdatedatetime,
  `ecw_refprovidertaxonomycode`.sourcesystemcode,
  `ecw_refprovidertaxonomycode`.insertedby,
  `ecw_refprovidertaxonomycode`.inserteddtm,
  `ecw_refprovidertaxonomycode`.modifiedby,
  `ecw_refprovidertaxonomycode`.modifieddtm,
  `ecw_refprovidertaxonomycode`.deleteflag,
  `ecw_refprovidertaxonomycode`.taxonomycodedesc
  FROM
    edwpsc.`ecw_refprovidertaxonomycode`
;