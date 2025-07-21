//Device Family Lookup
SELECT DISTINCT HASH("device_family") as device_family_id, 
"device_family" as device_family
FROM AMPLITUDE_EVENTS;

//Device_Lookup
SELECT DISTINCT HASH("device_type") as device_type_id, 
NVL( "device_type","device_family")as device_name,
HASH("device_family") as device_family_id
FROM AMPLITUDE_EVENTS;

//OS_lookup
SELECT DISTINCT HASH("os_name") as os_id, 
"os_name" as os_name
FROM AMPLITUDE_EVENTS;

//Device
SELECT DISTINCT HASH("os_name", "device_type", "device_family", "os_version") as device_id,
HASH("os_name") os_id,
HASH("device_type") device_type_id,
"os_version" as os_version
FROM AMPLITUDE_EVENTS;