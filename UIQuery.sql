
WITH src
AS (
	select 1 as rn,	'A' as IPHash,    '1' as Cookie, null as UserID	union all
select 2,	'B', 	'1',	'DD'	union all
select 3,	'B', 	'2',  null		union all
select 4,	'C', 	'3',	'DD'	union all
select 5,	'E', 	'4',	'EE'	union all
select 6,	null,	'5',	'EE'
	)

	,stack
AS (
	--we're pivoting the table, makes life easier
     SELECT
       field ||
       CASE
       WHEN field = 'IPHash' THEN IPHash::string
       WHEN field = 'Cookie' THEN Cookie::string
       WHEN field = 'UserID' THEN UserID::string
       END AS value,
       nest(rn) AS rnarry
     FROM src UNNEST (['IPHash', 'Cookie', 'UserID'] field)
     GROUP BY ALL
   )	

	
	,transition
AS (
	-- here we create some arrays, joining the pivot to itself where rows are shared among the individual identifiers
	SELECT rn
		,array_distinct(array_concat(stack.rnarry, t.rnarry)) AS candidates
	FROM src
	JOIN stack ON CONTAINS (
			stack.rnarry
			,src.rn
			)
	JOIN stack t ON length(array_intersect(t.rnarry, stack.rnarry)) > 0
	)
	,dist
AS (
	--here, we explode the array to get closer to the target
	SELECT DISTINCT rn
		,candidates
	FROM transition unnest(candidates)
	)
	,
	--last, we renest, a unique nested array attribute materializes each "user"
SELECT rn
	,array_sort (nest(candidates))
AS userid FROM dist GROUP BY ALL ORDER BY rn
