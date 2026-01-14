SELECT *
FROM UNNEST(['foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'])
AS element
WITH OFFSET AS offset
ORDER BY offset;