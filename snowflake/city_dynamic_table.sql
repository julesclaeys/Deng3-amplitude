--City Dynamic Table
CREATE OR REPLACE DYNAMIC TABLE S_AMPLITUDE_CITY 
TARGET_LAG = DOWNSTREAM
WAREHOUSE = DATASCHOOL_WH
as 
select distinct
"city" as city_name,
hash("city") as city_id
from amplitude_events;

-- Example of how to build a dynamic table, careful they are costly
