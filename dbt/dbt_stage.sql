-- This file contains all of the dbt staging scripts
-- They are built to have a CTE at the top to load the required source, renaming required fields and removing those we are not using in our planned intermediate layer


// 1) amplitude_events_lists

with 

source as (

    select * from {{ source('jc_deng_3_staging', 'amplitude_events_lists') }}

),

renamed as (

    select distinct
       -- "_airbyte_raw_id",
        "_airbyte_extracted_at" as airbyte_extracted_at, 
      --  "_airbyte_meta",
      --  "_airbyte_generation_id",
        "id" events_list_id,
        "name" events_list_name,
     --   "value",
    --     "hidden" hidden,
    --     "totals" totals,
    --     "deleted" deleted,
    --     "display",
    --     "autohidden" autohidden,
    --     "non_active",
    --     "flow_hidden",
    --     "in_waitroom",
    --     "totals_delta",
    --     "clusters_hidden",
    --     "timeline_hidden"

    from source

)

select * from renamed


  -- 2) Amplitude events lists


  with 

source as (

    select * from {{ source('jc_deng_3_staging', 'amplitude_events') }}

),

renamed as (

    select distinct
        -- "_airbyte_raw_id",
        "_airbyte_extracted_at" airbyte_extracted_at,
        -- "_airbyte_meta",
        -- "_airbyte_generation_id",
        -- "app",
        -- "dma",
        -- "adid",
        "city" city,
        -- "data",
        -- "idfa",
        -- "plan",
        "uuid" as event_id,
        -- "groups",
        -- "paying",
        "region" region,
        -- "_schema",
        "country" country,
        -- "library",
        "os_name" os_name,
        "user_id" user_id,
        "event_id" event_order,
        "language" language,
        "platform" platform,
        -- "data_type",
        "device_id" device_id,
        -- "source_id",
        -- "_insert_id",
        "event_time" event_time,
        "event_type" event_type,
        "ip_address" ip_address,
        "os_version" os_version,
        -- "partner_id",
        "session_id" session_id,
        -- "_insert_key",
        "device_type" device_type,
        -- "sample_rate",
        -- "amplitude_id",
        "device_brand" device_brand,
        "device_model" device_model,
        -- "location_lat",
        -- "location_lng",
        -- "version_name" ,
        "device_family" device_family,
        -- "start_version",
        -- "device_carrier",
        "processed_time" processed_time,
        "user_properties" user_properties,
        "event_properties" event_properties,
        -- "group_properties" ,
        -- "client_event_time",
        -- "client_upload_time",
        -- "server_upload_time",
        -- "user_creation_time",
        -- "device_manufacturer" device_manufacturer,
        -- "amplitude_event_type",
        -- "is_attribution_event",
        -- "server_received_time",
        -- "global_user_properties",
        -- "amplitude_attribution_ids"

    from source

)

select * from renamed
