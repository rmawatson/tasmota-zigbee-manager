
## Zigbee Manager for Tasmota

This extension is meant to implement the Tasmota discovery protocol when running Tasmota on a zigbree bridge, such as the Sonoff Zigbee Bridge Pro. It does this by advertising the the zigbee devices as standalone devices in the mqtt `/tasmota/discovery` topic, and emmiting zigbee d evice updates to their respective `/tele/<device_name/` `/stat/<device_name/` topics and, in the case of relays, listening on `/cmnd/<device_name/` topics

This is an alternative to Zigbee2MQTT and others such that it keeps all the zigbee communication on device, and translates this to mqtt messages that match any other Tasmotized wifi devices. 

To install this extension in Tasmota, paste the url `https://raw.githubusercontent.com/rmawatson/tasmota-zigbee-manager/refs/heads/main/extensions/` into the field at the bottom of `Tools->Extensions` and install from there.

Once installed, devices, schemas and settings can be managed from the **Zigbee Manager** page in the Tasmota web UI, see [Web UI](#web-ui), or with the [commands](#exposed-commands) in the console.

Primarily this was implemented to allow automatic discovery on Home Assistant with the existing Tasmota Integration, and has been used with a few PIR sensors, contact sensors, temperature/humidity sensors and Sonoff relays. (see schemas/ folder)

## Web UI

The extension adds a **Zigbee Manager** button to the main page of the Tasmota web UI. It opens three pages, **Devices**, **Schemas** and **Settings**, which cover what the `Zbm` commands below do. When a web password is set the pages need admin access.

Each page has **Main Menu** at the top, then a button for each page with the current one highlighted. After any action the page reloads with a message at the top, green when it worked, or red with the reason when it did not.

### Getting a device working

1. Pair the device with the bridge as usual, with Tasmota's **Zigbee Permit Join**.
2. Open **Zigbee Manager**. The device is listed within one poll period (5 seconds by default), or straight away after pressing **Poll devices**.
3. Press **Pull schemas** to download the schemas for your devices from this repository. The result is shown in the console when it finishes.
4. Type a name for the device and press **Add**. It is then advertised over MQTT discovery, and shows up in Home Assistant's Tasmota integration.

If the repository has no schema for the device, see [Creating a schema](#creating-a-schema), then upload it from the Schemas page.

### Devices

<img src="docs/images/devices.png" alt="Devices page" width="420">

Every device found on the bridge is listed, named devices first. Each shows its short address, manufacturer, model, key (`manufacturer:model`), the schema mapped to that key, link quality, battery level (`-` for mains powered devices) and when the bridge last heard from it, with its status in the top right.

- **Rename** (or **Set name** for an unnamed device) names the device with `ZbName`. Renaming a device that is already added adds it again, so its MQTT topics move to the new name.
- **Add** (`ZbmAddDevice`) is shown for every device that is not added. A name typed into the box is set first, so an unnamed device can be named and added in one go. Add also clears the errors left by an earlier attempt, so it can be pressed again once a missing schema has been added.
- **Reset** (`ZbmResetDevice`) clears the device's status. **Remove** (`ZbmRemoveDevice`) removes it, after asking.
- **Poll devices** (`ZbmPollDevices`) looks for devices joining or leaving the bridge now.
- **Pull schemas** (`ZbmPullSchemas`) downloads the schemas for every device from this repository, including devices that are already added, as their schemas may have been updated. The button shows *Pulling schemas...* until it has finished.

When a device is not added, a hint below its details says what to do. The statuses are

| Status | Meaning |
|---|---|
| Added | Working, its messages are processed and published over MQTT |
| Not added | Found on the bridge but not added yet, press Add |
| Device unnamed | It needs a name before it can be added |
| No mapping found | No schema is mapped to the device's key. Pull schemas, or upload a schema or add a mapping for it, then press Add |
| Schema not found | The key is mapped to a schema that is not in the registry |
| Schema compile failed | The schema's functions failed to compile, the reason is in the console |
| No default key available | The device has not reported its manufacturer and model yet, so it has no key |
| Device not found | The device is no longer paired with the bridge (only listed when *Remove devices that leave* is off) |
| Device was removed | Removed from the manager, press Reset to be able to add it again |

### Schemas

<img src="docs/images/schemas.png" alt="Schemas page" width="420">

Lists the schemas in the registry, each with the schemas it includes, its entities by category and the device keys mapped to it. **Remove** removes a schema (`ZbmRemoveSchema`), devices mapped to it stop working until another schema is mapped to their key. **Reset schemas**, at the bottom of the page, removes every schema and mapping (`ZbmResetSchemas`).

<img src="docs/images/schemas-upload.png" alt="Uploading a schema, and the mappings" width="420">

**Upload a schema** adds a schema pasted into the box, or loaded into it from a `.json` file. Nothing is stored until it has been checked, and it is rejected, with the reason, when

- it is not valid JSON, or its `version` is not the registry's
- it does not follow the schema layout, for example an unknown category, or an entity without any function
- one of its functions does not compile
- it includes a schema, or maps a key to a schema, that is neither in the registry nor in the same upload

A rejected schema stays in the box so it can be corrected. The upload shown above is rejected with *Schema rejected, acme_door includes battery_pct, which is not in the registry*. An accepted schema replaces any schema with the same name as a whole, and added devices using it are configured again so the changes reach Home Assistant.

**Mappings** lists the schema used for each device key. **Remove** removes a mapping (`ZbmRemoveMapping`), and the form below adds one (`ZbmAddMapping`), suggesting the keys of devices that do not have a mapping yet.

### Settings

<img src="docs/images/settings.png" alt="Settings page" width="420">

Changes the values otherwise set with `ZbmConfig`. **Reset settings** puts them back to their defaults (`ZbmResetConfig`).

| Setting | `ZbmConfig` key | Default | |
|---|---|---|---|
| Poll devices automatically | `auto_poll_devices` | on | Look for devices joining or leaving the bridge every poll period |
| Poll period (seconds) | `auto_poll_devices_period` | 5 | From 1 to 3600 |
| Add devices automatically | `auto_add_devices` | off | Add named devices that have a schema without pressing Add |
| Remove devices that leave | `auto_remove_devices` | on | Drop devices that are no longer paired with the bridge from the list |
| Name devices automatically | `auto_name_devices` | off | Name unnamed devices `manufacturer-model N` when they are added. Not recommended |
| Key devices by manufacturer:model | `auto_key_devices` | on | Needed to use the schemas in this repository |
| Log level | `log_level` | Info | None, Error, Info or Debug. Use Debug when looking into a problem |

## How it works

All devices must be named, and have a key. The key is autoamtically generated by default, so only naming with `ZbName` of the device is required. when 'Adding' a device the zigbee manager uses this key to look for a matching mapping in its registry. The mapping associates that device key with a schema which is also in the registry. The schema provides the config and callbacks for processing the zigbee messages. 

When a new zigbee message arrives, a matching device is looked up. If it is 'added', its schema is looked up. The message is then passed to `has_value` if it exists. If this returns false, no further processing is done. If this returns true `parse_value` is called to extract the value. This extracted value is then emmited to the respective mqtt stream. The target location varies based on whether this is sensor, relay, button, switch.

In the case of relays, a `cmnd/<device_name>` topic is subscribed to. Incoming messages are then passed to the `set_value` callback.
This is expected to write the value to the zigbee device (see `schemas/sonoff_minir2.json`)

`request_value` is called when the device/extension starts or when the device is first added. This to allow probing of the device to get its current status. For battery operated devices that are not actively listening this will end up doing nothing.

The `reset_value` callback is called straight after `parse_value`. It is intended to allow a reset value to be emitted to the mqtt
topic after the parsed value. In the case of a button, the default home assistant tasmota plugin didn't support the button field of the tasmota discovery message, and will show nothing in the UI.

Implementing the button as a sensor (see `sonoff_snzb-01p.json`) only one zigbee message is recieved (in the case of the Sonoff snzb-01p and probably others) when the button is pressed. This message always has the same `Power:2` value when the button is pressed. Home assistant ignores this as 'no change' for a sensor, and no event is generated within HA. To work aroudn this `reset_value` can be used to send a 0 straight after the value from `parse_value` has been emitted, creating a pulse that can be used trigger automations.

`format_category` is required for sensors to show up in home assistant without `format_category` the value for the sensor would be at the root of the json fragment sent to the `/tele/<device_name>/SENSOR` topic. This is ignored by the home assistant plugin, apart from a few select sensor fields. With the fragment below both `Pressed` and `LinkQuality` would fail to show in the UI

```
{
  "Time": "2025-11-25T20:10:09",
  "Pressed": 0,
  "LinkQuality": 147
}
```
setting `format_category` for the a sensor `Pressed` (see `sonoff_snzb-01p.json`) to `"format_category": "Button"` in the schema will show up in the home assistant UI as a sensor `Button Pressed` as in the json below 
```
{
  "Time": "2025-11-25T20:10:09",
  "Button": {
    "Pressed": 0
  },
  "Zigbee": {
    "LinkQuality": 147
  }
}
```
(This example has also had the LinkQuality sensor's `format_category` set to `Zigbee` (see `link_quality.json`) )

## Creating a schema

Assuming the device is connected to the zigbee bridge,

- Start with a basic schema fragment
    ```
    {
      "version": 1,
      "mappings": {},
       "schemas": {}
    }
    ```
- Find the device information with `ZbmDevices`
    ```
    01:42:23.460 ZBM: info > status : [<unnamed> (0x120E)]
    01:42:23.462 ZBM: info > status :    manufacturer: SONOFF
    01:42:23.464 ZBM: info > status :          model: ZBMINIR2
    01:42:23.466 ZBM: info > status :      shortaddr: 0x120E
    01:42:23.468 ZBM: info > status :       longaddr: 0x8404CBFEFFB6C67C
    01:42:23.469 ZBM: info > status :            mac: 8404CBFEFFB6
    01:42:23.471 ZBM: info > status :       lastseen: 2025-11-26T00:21:23
    01:42:23.472 ZBM: info > status :            lqi: 149
    01:42:23.474 ZBM: info > status :        battery: -1
    01:42:23.475 ZBM: info > status :            key: SONOFF:ZBMINIR2
    ```
- Name the device with `ZbName 0x120E,RELAY-01`.
- Make sure debug logging is set for Zbm and the tasmota console. use `WebLog 2` and `ZbmConfig log_level=3`
- Activate your device and watch the console (in the case of the relay press the button)
    ```
    01:47:26.533 ZBM: debug > device_manager : attributes_final,{"Power":1,"Endpoint":1,"LinkQuality":149},4622
    ```
    The zigbee packet is the second item `{"Power":1,"Endpoint":1,"LinkQuality":149}`

- Create the `relay` or `sensor` in your schema. The name for the schema is up to you. The catagories can either be `relays` or `sensors`. Although `buttons` and `switches` are also supported, they do not show anything in home assistant, but the mqtt messages for them work. the name of the relay can be whatever you want. Relays are `POWER1` ,`POWER2`, `POWER3`.. in the mqtt messages.
    ```
    {
        "version": 1,
        "mappings": {},
        "schemas": {
            "mysonoff_r2": {
                "relays":{
                    "MyRelay": {}
                }
            }
        }
    }
    ```
- Add any includes from preexisting schemas that contain features your devices exposes, (everythign seems to have LinkQuality as part of the zigbee message)
    ```
    {
        "version": 1,
        "mappings": {},
        "schemas": {
            "mysonoff_r2": {
                "include": ["link_quality"],
                "relays":{
                    "MyRelay": {}
                }
            }
        }
    }
    ```
- Create the callbacks for processing the messages from zigbee. 
All callbacks are passed the current device's `device_info`, along with
the attribute list for `has_value` and `parse_value` `reset_value`, the mqtt `value` for `set_value` or nothing for `request_value`.  All commands are passed a `context` object for their final argument. This exposes some helper functions for sending read and write requests to the zigbee device - `ctx.zb_write` `ctx.zb_read`
- In this case, setting and reading the the `Power` field is all that is required<br>
`has_value` should return a boolean value as to whether or not the value exists. If it always exists this is not required.<br/>
`"has_value": "/device_info,attr_list -> attr_list.contains('Power')"`<br/>
`parse_value` should return the extracted value, foramtted as required<br/>
`"parse_value": "/device_info,attr_list -> attr_list['Power'] ? 'ON' : 'OFF'"`<br/>
- For a sensor no futher callbacks are necessary - just a `format_category` in the case of HA. For a relay, `set_value` is needed to write a value from the cmnd topic's mqtt payload. It can use the provided `zb_write` helper function to write this to the relay device.
`"set_value": "/device_info,value,ctx -> ctx.zb_write(device_info,{'Power':value ? 1 : 0})"`<br/>
Note: it may require some experimentation in the console using tasmotas `ZbSend` to check your relay is working. the `zb_write` `zb_read` functions above are just wrappers around `ZbSend`
- Optionally you can add `request_value` to have Zbm request the latest relay status on startup for this device.
- Your schema should look look as below.
    ```
    {
        "version": 1,
        "schemas": {
            "mysonoff_r2": {
                "include": ["link_quality"],
                "relays": {
                    "MyRelay": {
                        "has_value": "/device_info,attr_list -> attr_list.contains('Power')",
                        "parse_value": "/device_info,attr_list -> attr_list['Power'] ? 'ON' : 'OFF'",
                        "set_value": "/device_info,value,ctx -> ctx.zb_write(device_info,{'Power':value ? 1 : 0})",
                        "request_value": "/device_info,ctx -> ctx.zb_read(device_info,{'Power':1})"
                    }
                }
            }
        }
    }
    ```
- Finally add a mapping to this schema. Assuming you want to use `auto_key_devices` to generate `manufacturer:model` keys for your device then using the information from `ZbmDevices` earlier
    ```
    01:42:23.462 ZBM: info > status :    manufacturer: SONOFF
    01:42:23.464 ZBM: info > status :          model: ZBMINIR2
    ```
    add the mapping to the schema
    ```
    {
        "version": 1,
        "mappings": {
            "SONOFF:ZBMINIR2": "mysonoff_r2"
        },        
        "schemas": {
            "mysonoff_r2": {
                "include": ["link_quality"],
                "relays": {
                    "MyRelay": {
                        "has_value": "/device_info,attr_list -> attr_list.contains('Power')",
                        "parse_value": "/device_info,attr_list -> attr_list['Power'] ? 'ON' : 'OFF'",
                        "set_value": "/device_info,value,ctx -> ctx.zb_write(device_info,{'Power':value ? 1 : 0})",
                        "request_value": "/device_info,ctx -> ctx.zb_read(device_info,{'Power':1})"
                    }
                }
            }
        }
    }
    ```
- The schema can now be added to the registry by pasting it into **Upload a schema** on the [Schemas page](#schemas), which checks it and tells you what is wrong if it is rejected, or with `ZbmAddSchema <paste_the_json>`
- Once the schema has been tested and confirmed working it can be added to the repository. Clone the repository, put the schema in the schema folder and run `scripts/update_manifest.py` to update the manfiest (used by `ZbmPullSchemas`). Submit a PR. For future use of your schema, `ZbPullSchemas` should be all that is required to setup your device.
  
## Exposed commands

All commands are either read only (ro), read write (rw) or, write only (wo). Unelss otherwise specified, arguments to the command can be passed as positionally `ZbmXXX arg`, as key value `ZbmXXX key=value` pairs, or as a json fragment `ZbmXXX {'key':'value,...}`.

> ### **ZbmDevices** (ro)
> 
> Lists information about devices available on the bridge

> ### **ZbmSchemas** (ro)
>
> Shows the current content of the registry (requires log_level >= 2 and WebLog >= 2) to see it in the console

> ### **ZbmConfig** (rw)
>
> Outputs the current config with no arguments, or accepts configuration variable to set. On/off values can be given as `1`/`0`, `on`/`off`, `true`/`false` or `yes`/`no`. The same values can be changed on the [Settings page](#settings).
>
> `auto_poll_devices`<br/>
> Enable/disable auto polling of devices. This will periodically check for available devices, resolve states that no longer apply and remove devices that are. `ZbmPollDevices` will run the same process manually a single time `default=true`
> 
> `auto_poll_devices_period`<br/>
> Polling period for auto_poll_devices in seconds `default=5`
> 
> `auto_add_devices`
> Enable/disable automatically attempting to add a device. Until a device is added, no zigbee messages will be processed for that device, and no mqtt messages will be sent. To automatically add a device, it must have a valid name, have a key set (or auto_key_devices=true), and have a mapping present in the registry with an associated schema for this device type. `default=false`
> 
> `auto_remove_devices`<br/>
> Enable/disable removal of devices that are no longer bound to the tasmota zigbee bridge. If the device has failed or has been added, it will be marked as removed, and will require manual removal. `default=true`
> 
> `auto_name_devices`<br/>
> Enable/disable to automaticlly name a device (not recommended). This generates a name of the form `manufacturer-model N` `default=false`
> 
> `auto_key_devices`<br/>
> Enable/disable generating a key for schema lookup of the form `manufacturer:model`. It is recommended to use this as a custom key would not match any existing schema. For a custom schema, this is however available so for example 2 identical devices could be mapped to 2 different schemas (for whatever reason) `default=true`
>  
> `log_level=2`
> The current log_level of the zbm extension - use 3 when debugging any issue.

> ## ZbmPollDevices (ro)
> Runs the same processing that is run when `auto_poll_devices=true`

> ## ZbmAddSchema (wo)
> Add a schema to the registry. The schema should be of the form
> ```
> {
>    "version": 1,
>    "mappings": {},
>    "schemas": {
>        "schema_name": {
>            "states": {
>                "SensorName": {
>                    "parse_value": "<berry function>"
>                }
>            }
>        }
>    }
>}
>```
> Note: mappings and schemas are both optional, so you can add a mapping with this, or a schema, or both.
>
> Adding a schema or mapping that is already in the registry updates it, the values from the newly added schema replace the stored ones (an `include` list is replaced as a whole).

> ## ZbmResetSchemas (ro)
> Flushes all schemas from the registry and leaves it in a default state

> ## ZbmRemoveSchema (wo)
> Remove a specific schema form the registry

> ## ZbmResetConfig (ro)
> Resets the config to a default value

> ## ZbmResetService (ro)
> Removes all devices found since started and resets their status. Added devices would no longer be added after this is called.

> ## ZbmAddDevice (wo)
> Attemps to add a device. Once a device is added it zigbee payloads for that device will be processed, an mqtt discovery topic will be emitted. 'Added' is the working state of a device. Any additional states (as seen using ZbmDevices) is most likely an error. To sucessfully add a device it needs to have a valid name (or auto_name_devices=true), a valid key (or auto_key_devices=true), and the registry must have both a mapping that will be matches for this devices key, that points to a and a valid schema for this device

> ## ZbmRemoveDevice (wo)
> Removes the device either by `devicename=the_device_name` or `deviceid=the_device_shortaddr`

> ## ZbmResetDevice (wo)
> Clears any status flags on the device. Used when you want to attemp readding after a schema compilation failed, and after fixing the schema.
> 
> ## ZbmAddMapping (wo)
> Adds a mapping using key=value. This is the same the mappings added with ZbmAddSchema (and the same mapping could also be done this way)

> ## ZbmRemoveMapping (wo)
> Removes a mapping by key name

> ## ZbmPullSchemas
> For all devices attached to the zigbee bridge, attempts to download valid schemas and mappings from the github repository for them based on the device key (or generated key if `auto_key_devices=true`). For devices that have schemas in the repository, assuming they are all named with `ZbName` already, with default settings this would be all that is required to make them discoverable (and in the case of home assistant they would show up as devices in the tasmota integration)
>
> The download runs in the background, one file at a time, so the command returns straight away with `{"ZbmPullSchemas":{"Status":"Started","Keys":[...]}}`. Schemas are downloaded for every device, including devices that are already added, as the repository may have newer versions, together with the schemas they include. Each downloaded schema replaces the one in the registry as a whole as soon as it arrives (a schema you added yourself under the same name as one in the repository is overwritten), and devices that are already added are configured again so the changes take effect. Failed requests are retried. Once finished the result is logged and published to `stat/<topic>/RESULT`, either `{"ZbmPullSchemas":{"Status":"Done","Schemas":[...],"NotFound":[...]}}` or `{"ZbmPullSchemas":{"Status":"Failed","Error":"...","Schemas":[...]}}`, where `Schemas` lists the schemas replaced before the failure.


## To do/Notes
commands (see `sonoff_minir2.json`) are not currently implemented. Home assisant doesn't really have no code/yaml way to call them, but it would still be nice to listen to the topics to configure things such as TurboMode on the sonoff_minir2.

the zha repository contains alot of already found information devices, for example the minir2, the TurboMode feature's details are described `https://github.com/zigpy/zha-device-handlers/blob/dev/zhaquirks/sonoff/zbminir2.py`
