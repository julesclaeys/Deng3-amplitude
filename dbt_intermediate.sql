-- This file contains all of our intermediate *(or silver)* layer from dbt
-- There are many tables hence why they are numbered. 
-- The main goal of this layer is to normalise the data, hashing where required, in the future I would do the hashing in another stage potentially reducing the processing power required. 

-- 1) int_amplitude_city 

WITH city AS (
 SELECT * FROM  {{ ref('stg_jc_deng_3_staging__amplitude_events') }} )
 
select distinct
city as city_name,
hash(city) as city_id
FROM city

-- 2) int_amplitude_country

with country as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)

select distinct
country as country_name,
hash(country) as country_id
from country

-- 3) int_amplitude_device_family_lookup

with family as (

  SELECT *  FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)

SELECT DISTINCT HASH(device_family) as device_family_id, 
device_family as device_family
FROM family


-- 4) int_amplitude_device_lookup

with device_l as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)
SELECT DISTINCT HASH(device_type) as device_type_id, 
COALESCE( device_type,device_family)as device_name,
HASH(device_family) as device_family_id
from device_l

-- 5) int_amplitude_device

with device as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)

SELECT DISTINCT HASH(os_name, device_type, device_family, os_version) as device_id,
HASH(os_name) os_id,
HASH(device_type) device_type_id,
os_version as os_version
FROM device


-- 6) int_amplitude_event_properties
-- This table included arrays

with prop as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
),

parse_evp_json_cte as (
    select
        hash(event_properties) as event_properties_id,
        (parse_json(event_properties)) as json
    from prop
)

select DISTINCT
    event_properties_id,
    json:"[Amplitude] Page URL"::string as page_url,
    json:"referrer"::string as referrer,-- like google
    json:"[Amplitude] Page Counter"::int as page_counter,
    json:"[Amplitude] Page Domain"::string as page_domain, --like til
    json:"[Amplitude] Page Path"::string as page_path, --like /how-we-help
    json:"[Amplitude] Page Title"::string as page_title, --the nice one
    json:"[Amplitude] Page Location"::string as page_location, --page_domain with page_path
    json:"referring_domain"::string as referring_domain, -- like google
    json:"[Amplitude] Element Text"::string as element_text, --like accept
    json:"video_url"::string as video_url, --like embed link
    -- json:"" as 
from parse_evp_json_cte as e

-- 7) int_amplitude_events_lists
with list as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events_lists') }}
)

select
    events_list_id ,
    events_list_name 
from list

-- 8) int_amplitude_events

with events as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
),

events_list as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events_lists') }}
)

SELECT 
     eve.event_id               AS event_id
    ,eve.session_id         AS session_id
    ,eve.event_order          AS event_order
    ,evel.events_list_id            
    ,eve.event_time         AS event_time
    ,HASH(eve.user_properties) AS user_properties_id -- for future iterations change maybe?
    ,HASH(eve.event_properties) AS event_properties_id
FROM events eve
LEFT JOIN events_list evel
    ON eve.event_type = evel.events_list_name

-- 9) int_ampltiude_location

with location as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)
select distinct
hash(ip_address) as location_id,
ip_address as ip_address,
hash(city) as city_id,
hash(country) as country_id,
hash(region) as region_id
from location


-- 10) int_amplitude_os_lookup

with os as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)

SELECT DISTINCT HASH(os_name) as os_id, 
os_name as os_name
from os

-- 11) int_amplitude_region
with region as (
    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)

select distinct
region as region_name,
hash(region) as region_id
from region

-- 12) int_amplitude_sessions

with sessions as (
    select * from {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
)

SELECT DISTINCT
    session_id AS session_id,
    user_id AS user_id,
    HASH(os_name, device_type, device_family, os_version) AS device_id,
    HASH(ip_address) AS location_id
FROM sessions

-- 13) int_amplitude_user_properties

with prop as (

    SELECT * FROM {{ ref('stg_jc_deng_3_staging__amplitude_events') }}
),

json as (Select
    hash(user_properties) as up_id,
    parse_json(user_properties) as parsed_json
from prop
)

select DISTINCT
    up_id,
    parsed_json:initial_utm_medium::STRING AS initial_utm_medium,
    parsed_json:initial_referring_domain::STRING AS initial_referring_domain,
    parsed_json:initial_utm_campaign::STRING AS initial_utm_campaign,
    parsed_json:referrer::STRING AS referrer,
    parsed_json:initial_utm_source::STRING AS initial_utm_source,
    parsed_json:initial_referrer::STRING AS initial_referrer,
    parsed_json:referring_domain::STRING AS referring_domain
from json





