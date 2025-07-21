CREATE OR REPLACE PROCEDURE REFRESH_S_AMPLITUDE_EVENTS()
returns varchar
language sql
as
$$
MERGE INTO S_AMPLITUDE_EVENTS tgt
USING(
SELECT 
     eve."uuid"               AS event_id
    ,eve."session_id"         AS session_id
    ,eve."event_id"           AS session_event_order
    ,evel."id"                AS id
    ,eve."event_time"         AS event_time
    ,HASH(eve."user_properties") AS user_properties_id -- for future iterations change maybe?
    ,HASH(eve."event_properties") AS event_properties_id
FROM AMPLITUDE_EVENTS eve
LEFT JOIN AMPLITUDE_EVENTS_LISTS evel
    ON eve."event_type" = evel."name"
    where eve."_airbyte_extracted_at" > DATEADD( day, -1, CURRENT_DATE())) src
ON tgt.event_id = src.event_id
WHEN NOT MATCHED THEN INSERT ( 
EVENT_ID, SESSION_ID, SESSION_EVENT_ORDER, ID, EVENT_TIME, USER_PROPERTIES_ID, EVENT_PROPERTIES_ID
) VALUES (
src.EVENT_ID, 
src.SESSION_ID, 
src.SESSION_EVENT_ORDER, 
src.ID, 
src.EVENT_TIME, 
src.USER_PROPERTIES_ID, 
src.EVENT_PROPERTIES_ID
);
    
$$
;
