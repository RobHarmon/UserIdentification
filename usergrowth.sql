
CREATE EXTERNAL TABLE ext_connection_event (
	event_time timestampntz
	,sending_user_id BIGINT
	,receiving_user_id BIGINT
	,event_type string
	,event_date pgdate
	) 
URL = 's3://somebucket/connection_events'
OBJECT_PATTERN= '*.parquet'
TYPE = (PARQUET);


create external table ext_user_snapshot (
user_id bigint,
country string,
age integer,
username string,
snapshot_date date)
URL = 's3: //somebucket/connection_events'
OBJECT_PATTERN = '*.parquet' 
TYPE = (PARQUET);

CREATE fact TABLE connection_event (
	event_time timestampntz
	,sending_user_id BIGINT
	,receiving_user_id BIGINT
	,sending_user_dwid BIGINT
	,receiving_user_dwid BIGINT
	,event_type string
	,event_date pgdate
	,source_file_name string
	,source_file_timestamp timestampntz
	) PRIMARY INDEX event_date;

CREATE dimension TABLE user (
	user_id BIGINT
	,user_dwid BIGINT
	,country string
	,year_of_birth
	,username string
	,snapshot_date DATE
	,dwhash BIGINT
	) PRIMARY INDEX user_dwid;

INSERT INTO user
WITH source AS (
		SELECT user_id
			,row_number() OVER (
				ORDER BY 1
				) + (
				SELECT max(user_dwid)
				FROM user
				) user_dwid
			,country
			,date_add('year', - age, date_trunc('year', current_timestamp)) AS year_of_birth
			,username
			,snapshot_date
			,city_hash(date_add('year', - age, date_trunc('year', current_timestamp)), username) AS dwhash
		FROM ext_user_snapshot
		WHERE source_file_name = ?
		)
	,destination AS (
		SELECT user_id
			,dwhash
		FROM user
		WHERE lead(snapshot_date) OVER (
				PARTITION BY user_id
				,ORDER BY snapshot_date
				) IS NULL
		)
SELECT source.*
FROM source
LEFT OUTER JOIN destination ON source.user_id = destination.user_id
WHERE source.dwhash != destination.dwhash
	OR destination.user_id IS NULL

SELECT *
FROM source
LEFT OUTER JOIN user ON user.user_id = source.user_id;

INSERT INTO connection_event
WITH userlookup AS (
		SELECT user_id
			,user_dwid
			,snapshot_date AS start_date
			,coalesce(lead(snapshot_date) OVER (
					PARTITION BY user_id
					,ORDER BY snapshot_date
					), dateadd('day', 20, current_timestamp) AS enddate FROM user)
		)
SELECT c.event_time
	,c.sending_user_id
	,c.receiving_user_id
	,su.userdwid
	,ru.userdwid
	,c.event_type
	,c.event_date
	,c.source_file_name
	,c.source_file_timestamp
FROM ext_connection_event c
LEFT OUTER JOIN userlookup su ON su.user_id = c.sending_user_id
	AND c.event_date BETWEEN su.start_date
		AND su.end_date
LEFT OUTER JOIN userlookup ru ON ru.user_id = c.sending_user_id
	AND c.event_date BETWEEN ru.start_date
		AND ru.end_date
WHERE c.source_file_name NOT IN (
		SELECT source_file_name
		FROM connection_event
		);
