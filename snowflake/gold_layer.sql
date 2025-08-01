-- This creates a task which runs after our run events refresh task, this will trigger the run for creation of our gold layer

CREATE OR REPLACE TASK RUN_G_AMPLITUDE_EVENTS
warehouse = dataschool_wh
AFTER RUN_EVENTS_REFRESH
CREATE OR REPLACE TABLE G_AMPLITUDE_EVENTS as
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
, list.
FROM S_AMPLITUDE_EVENTS eve
LEFT JOIN S_AMPLITUDE_SESSIONS sess
ON eve.session_id = sess.session_id
LEFT JOIN S_AMPLITUDE_LOCATION loc
ON sess.location_id = loc.location_id
LEFT JOIN S_AMPLITUDE_CITY city
ON loc.city_id = city.city_id
LEFT JOIN S_AMPLITUDE_REGION region
ON loc.region_id = region.region_id
LEFT JOIN S_AMPLITUDE_COUNTRY country
ON loc.country_id = country.country_id
LEFT JOIN S_AMPLITUDE_DEVICE dev
ON sess.device_id = dev.device_id
LEFT JOIN S_AMPLITUDE_OS_LOOKUP OS
ON dev.os_id = os.os_id
LEFT JOIN S_AMPLITUDE_DEVICE_LOOKUP dev_lok
ON dev.device_type_id = dev_lok.device_type_id
LEFT JOIN S_AMPLITUDE_DEVICE_FAMILY_LOOKUP fam
ON dev_lok.device_family_id = fam.device_family_id
LEFT JOIN S_AMPLITUDE_EVENT_PROPERTIES eve_prop
ON eve.event_properties_id = eve_prop.event_properties_id
LEFT JOIN S_AMPLITUDE_USER_PROPERTIES user_prop
ON eve.user_properties_id = user_prop.up_id;
