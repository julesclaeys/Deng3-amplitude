--Run Country when new data input
CREATE OR REPLACE TASK RUN_S_AMPLITUDE_COUNTRY
warehouse = dataschool_wh
WHEN SYSTEM$STREAM_HAS_DATA('RAW_EVENTS_STREAM')
as
CREATE OR REPLACE TABLE S_AMPLITUDE_COUNTRY as
select distinct
"country" as country_name,
hash("country") as country_id
from amplitude_events;

-- This tasks uses the raw events stream to check for data, then rebuilding the table whenever new data is found
