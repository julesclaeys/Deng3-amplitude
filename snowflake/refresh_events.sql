CREATE OR REPLACE TASK RUN_EVENTS_REFRESH
warehouse = dataschool_wh
WHEN SYSTEM$STREAM_HAS_DATA('RAW_EVENTS_STREAM')
as
CALL REFRESH_S_AMPLITUDE_EVENTS();

-- This code creats a task to run our merge procedure
