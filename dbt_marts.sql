-- There is only one mart table created, in a real business context we would have more including aggregations

with events as 
        ( select * from {{ ref('int_amplitude_events') }}),
    sessions as 
        (select * from {{ ref('int_amplitude_sessions') }}),
    location as 
        (select * from {{ ref('int_amplitude_location') }}),
    city as 
        (select * from {{ ref('int_amplitude_city') }}),
    region as 
        (select * from {{ ref('int_amplitude_region') }}),
    country as 
        (select * from {{ ref('int_amplitude_country') }}),
    device as 
        (select * from {{ ref('int_amplitude_device') }}),
    os_lookup as 
        (select * from {{ ref('int_amplitude_os_lookup') }}),
    device_look as 
        (select * from {{ ref('int_amplitude_device_lookup') }}),
    device_family as 
        (select * from {{ ref('int_amplitude_device_family_lookup') }}),
    event_prop as   
        (select * from {{ ref('int_amplitude_event_properties') }}),
    user_prop as 
        (select * from {{ ref('int_amplitude_user_properties') }}),
    events_list as (select * from {{ ref('int_amplitude_events_list') }})

SELECT 
city.city_name
, country.country_name
, region.region_name
, dev_lok.device_name
, eve.event_id
, eve_prop.page_url
, eve_prop.page_title
, eve_prop.page_location
, os.os_name
, dev.os_version
, sess.session_id
, sess.user_id
, user_prop.initial_utm_campaign
, user_prop.initial_utm_medium
, user_prop.initial_utm_source
, user_prop.referrer
, user_prop.referring_domain
, list.events_list_name
FROM events eve
LEFT JOIN sessions sess
ON eve.session_id = sess.session_id
LEFT JOIN location loc
ON sess.location_id = loc.location_id
LEFT JOIN city city
ON loc.city_id = city.city_id
LEFT JOIN region as region
ON loc.region_id = region.region_id
LEFT JOIN country country
ON loc.country_id = country.country_id
LEFT JOIN device dev
ON sess.device_id = dev.device_id
LEFT JOIN os_lookup OS
ON dev.os_id = os.os_id
LEFT JOIN device_look dev_lok
ON dev.device_type_id = dev_lok.device_type_id
LEFT JOIN device_family fam
ON dev_lok.device_family_id = fam.device_family_id
LEFT JOIN event_prop eve_prop
ON eve.event_properties_id = eve_prop.event_properties_id
LEFT JOIN user_prop user_prop
ON eve.user_properties_id = user_prop.up_id
LEFT JOIN events_list list
ON eve.events_list_id = list.events_list_id
