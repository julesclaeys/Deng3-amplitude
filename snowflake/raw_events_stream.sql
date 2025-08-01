
create stream RAW_EVENTS_STREAM
    -- copy grants
    on table JC_DENG_3_STAGING.AMPLITUDE_EVENTS
    -- { at | before } { timestamp => <timestamp> | offset => <time_difference> | statement => <id> }
    -- append_only = true | false ]  [ insert_only = true ]
    -- comment = '<comment>'
;

-- This code creates the stream which checks for new data being loaded in our staging layer
