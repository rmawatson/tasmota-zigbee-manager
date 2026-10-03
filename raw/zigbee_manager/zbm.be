# MIT License - Copyright (c) 2025 rmawatson@hotmail.com - See LICENSE file for details
#-  -#

import persist
import zigbee
import json
import string
import global
import mqtt
import introspect
import undefined
import re

var zbm_state = nil
var zbm_mqtt_bridge = nil
var zbm_schema_registry = nil
var zbm_service = nil
var zbm_schema_puller = nil
var zbm_web_ui = nil

def _join(li, sep, fn)
    var result = ""
    if fn == nil
        fn = / v -> v
    end
    if size(li)
        for v : li
            result += f"{fn(v)}{sep}"
        end
        return result[0..-(size(sep)+1)]
    end
    return result
end

def _copy(value,shallow)

    def _apply(val)
        if isinstance(val, list)
            var new_list = []
            for item : val
                new_list.push(shallow ? item :_apply(item))
            end
            return new_list
        elif isinstance(val, map)
            var new_map = {}
            for key : val.keys()
                new_map[key] = _apply(shallow ? val[key] : _apply(val[key]))
            end
            return new_map
        end
        return val
    end
    return _apply(value)
end

def to_list(iter)
    var result = []
    for item : iter
        result.push(item)
    end
    return result
end

def list_remove(li, value, ignore_error)
    var index = li.find(value)
    if index == nil
        if ignore_error
            return li
        end
        raise "index_error", f"invalid index {value}"
    end
    li.pop(index)
    return li
end

def lstrip(string_)
  if size(string_) == 0 || (string.count(string_," ") + string.count(string_,"\t"))== size(string_)
    return ""
  end

  var strbegin = 0
  for i : 0..size(string_)-1 
    strbegin = i
    if string_[strbegin] != ' ' && string_[strbegin] != '\t'
      break
    end
  end
  return string_[strbegin..]
end

def rstrip(string_)
  if size(string_) == 0 || (string.count(string_," ") + string.count(string_,"\t"))== size(string_)
    return ""
  end
  var strend = size(string_)-1
  for y : 1..strend
    strend = size(string_)-y
    if string_[strend] != ' '&& string_[strend] != '\t'
      break
    end
  end
  return string_[0..strend]
end

def strip(string_)
  return rstrip(lstrip(string_))
end

class ZbmStruct
    var info

    def init(info)
        self.info = info
    end

    def tostring()
        return f"{self.info}"
    end

    def member(name)
        if !self.info.contains(name)
            return undefined
        end
        return self.info[name]
    end

    def setmember(name, value)
        if !self.info.contains(name)
            raise "attribute_error", f"attribute {name} not found on {classname(self)} instance"
        end
        self.info[name] = value
    end

end

class ZbmLogger
    static var levels = {0:"None",1:"Error",2:"Info",3:"Debug"}

    var prefix 
    def init(prefix)
        self.prefix = prefix
    end

    def log_message(severity,message)
        if severity <= (zbm_state != nil ? zbm_state.log_level : 3)
            if self.prefix
                message = f"{self.prefix} : {message}"
            end
            log(f"ZBM: {string.tolower(self.levels[severity])} > {message}",1)
            
        end
    end

    def debug(message)
        self.log_message(3,message)
    end
    
    def info(message)
        self.log_message(2,message)
    end

    def error(message)
        self.log_message(1,message)
    end
end

def zb_send(payload)
    ZbmLogger("zb_send").debug(f"sending value {json.dump(payload)}")
    tasmota.cmd(f"ZbSend {json.dump(payload)}")
end

def zb_write(device_info,payload)
    zb_send({"Device":device_info.deviceid,"Send":payload})
end

def zb_read(device_info,payload)
    zb_send({"Device":device_info.deviceid,"Read":payload})
end



def zb_timestr(time_type)
    if time_type == nil
        time_type = "local"
    end

    var rtc_timestamp = tasmota.rtc()
    if !rtc_timestamp.contains(time_type)
        raise "invalid_time_key",f"time of type {time_type} does not exist."
    end
    return tasmota.strftime("%Y-%m-%dT%H:%M:%S",tasmota.rtc()[time_type])
end

class zb_handler_error end
class zb_handler_invalid end

def zb_invoke_handler(callback_name,components,device_info,*args)
    if components.contains(callback_name) && components[callback_name].valid
        def _make_call_fn(fn)
            return def (_,*args) call(fn,args) end
        end

        var context = ZbmStruct({
            "zb_write":_make_call_fn(zb_write),
            "zb_read":_make_call_fn(zb_read)})        
        try
            return call(components[callback_name].fn,device_info,args + [context])
        except .. as e,m
            ZbmLogger(f"invoke_handler {callback_name}").debug(f"{e},{m}")
            components[callback_name].valid = false
            return zb_handler_error
        end
    end
    return zb_handler_invalid
end

def zb_retain(retain_type)
    var value = tasmota.cmd(retain_type)[retain_type]
    return type(value) == string ? (string.toupper(value) == "OFF" ? false : true) : !!value
end

def zb_state_retain()
    return zb_retain("StateRetain")
end

def zb_sensor_retain()
    return zb_retain("SensorRetain")
end


class ZbmNotify : ZbmStruct
    var handlers

    def init(info)
        super(self).init(info)
        self.handlers = {}
    end

    def on_changed(name, fn)
        if self.info.contains(name) || name == "*"
            if !self.handlers.contains(name)
                self.handlers[name] = []
            end
            self.handlers[name].push(fn)
        end      
    end

    def setmember(name, value)
        if !self.info.contains(name)
            super(self).setmember(name, value) 
            return
        end

        var prev_value = self.info[name]

        if prev_value == value
            return
        end

        super(self).setmember(name, value)

        if self.handlers.contains(name)
            for handler : self.handlers[name]
                handler(value, prev_value)
            end
        end 

        if self.handlers.contains("*")
            for handler : self.handlers["*"]
                handler(value, prev_value)
            end
        end
    end
end

class ZbmDeviceStatus

    static var Available = 0
    static var Added = 1
    static var Unnamed = 2
    static var MappingNotFound = 4
    static var SchemaNotFound = 8
    static var SchemaCompileFailed = 16
    static var NotFound = 32
    static var NoDefaultKey = 64
    static var Removed = 128
    static var ErrorFlag = _class.Unnamed | _class.MappingNotFound | _class.SchemaCompileFailed | _class.NoDefaultKey
    static var StatusFlag = _class.Added | _class.NotFound

    static var description = {
        _class.Added:"Added",
        _class.Unnamed:"Device unnamed",
        _class.MappingNotFound:"No mapping found",
        _class.SchemaNotFound:"Schema not found",
        _class.SchemaCompileFailed:"Schema compile failed",
        _class.NotFound:"Device not found",
        _class.NoDefaultKey:"No default key available",
        _class.Removed:"Device was removed"
    }

    static def descriptions(status)
        var result = []
        if status != 0
            for i : 0..size(_class.description)-1
                var value = 1 << i
                if value & status
                    result.push(_class.description[value])
                end
            end
        end
        return result
    end
end

class ZbmDeviceInfo: ZbmNotify

    def init(arg,status)
        
        if arg == nil
            super(self).init({})
        elif isinstance(arg,map)
            super(self).init(arg)        
        else
            super(self).init({
                "shortaddr": arg.shortaddr,
                "longaddr": arg.longaddr.tohex(),
                "macaddr": arg.longaddr.tohex()[0..11],
                "deviceid": "0x" + string.toupper(f"{arg.shortaddr:.4x}"),
                "name": arg.name,
                "reachable": arg.reachable,
                "hidden": arg.hidden,
                "router": arg.router,
                "model": arg.model,
                "manufacturer": arg.manufacturer,
                "lastseen": arg.lastseen,
                "lqi": arg.lqi,
                "battery": arg.battery,
                "battery_lastseen": arg.battery_lastseen,
                "ieeeaddr": arg.info()["IEEEAddr"],
                "key": nil,
                "status": status
            })
        end
    end

    def update(other)
        for info_key : other.info.keys()
            if other.info[info_key] == nil
                continue
            end 
            self.setmember(info_key,other.info[info_key])
        end
    end
    
end

class ZbmCompiledSchema

    var name
    var schema    
    var includes
    var valid

    def init(name,schema,includes,valid)
        self.name = name
        self.schema = schema
        self.includes = includes
        self.valid = (valid == nil || valid == true) ? true : false
    end
end

class ZbmCompiledFunction

    var fn
    var valid

    def init(fn,valid)
        self.fn = fn
        self.valid = (valid == nil || valid == true) ? true : false 
    end
end

class ZbmSchemaItem : ZbmStruct
    def init(debug_name,value,flags,transform)
        super(self).init({"debug_name":debug_name,"value":value,"flags":flags == nil ? 0 : flags,"transform":transform})
    end
end

class ZbmSchemaProcessorInfo

    static var OPTIONAL = 0
    static var REQUIRED = 1
    static var UNIQUE = 2
    static var COMPILABLE = 4    

    static var FN_TYPE_LAMBDA = 1
    static var FN_TYPE_ANON_DEF = 2
    static var FN_TYPE_NAMED_DEF = 3

    static var categories = ["states","sensors","switches","relays","commands"]
    static var fn_components = ["has_value","parse_value","request_value","set_value","reset_value"]

    static def valid_expr(expr, name)
        # Validate value matches regex expression
        return def (value)
            if type(value) != "string"
                raise "validation_error", f"invalid {name} value '{value}', expected string"
            end
            if !re.match(expr, value)
                raise "validation_error", f"invalid {name} string '{value}'"
            end
            return true
        end
    end
    

    static def fn_info(value)
        var matches

        if matches := re.match("^\\s*/\\s*([a-zA-Z0-0,_ ]*)(->.+)\\s*$",value)
            return ZbmStruct({"all":matches[0], "type":_class.FN_TYPE_LAMBDA,"args":matches[1],"rest":matches[2],"name":nil})
        elif matches :=re.match("^\\s*def\\s*\\(([a-zA-Z0-0,_ ]*)\\)(.*?end)\\s*$",value)
            return ZbmStruct({"all":matches[0], "type":_class.FN_TYPE_ANON_DEF,"args":matches[1],"rest":matches[2],"name":nil})
        elif matches :=re.match("^\\s*def\\s*([a-zA-Z0-9_]+)\\s*\\(([a-zA-Z0-0,_ ]*)\\)(.+*end)\\s*$",value)
            return ZbmStruct({"all":matches[0], "type":_class.FN_TYPE_NAMED_DEF,"args":matches[2],"rest":matches[3],"name":matches[1]})
        end

        return false
    end

    static def valid_fn(value)
        if type(value) != "string"
            raise "validation_error", f"invalid type '{type(value)}' when validation function, expected string"
        end
        return _class.fn_info(value) == false ? false: true
    end

    static var value_in = /values -> /value -> (values.find(value) != nil)
    static var valid_name = /name -> _class.valid_expr("^[a-zA-Z0-9 _-]+$", name)
    static var valid_mapping = /name -> _class.valid_expr("^[:a-zA-Z0-9 _-]+$", name)

    static def compile_fn(fn_string,schema_item)
        var info
        try
            if (info := _class.fn_info(fn_string)) == false
                raise "validation_error expected"
            elif [_class.FN_TYPE_LAMBDA,_class.FN_TYPE_ANON_DEF].find(info.type) != nil
                fn_string = f"return {info.all}"
            else
                fn_string = f"{info.all} return {info.name}"
            end
            return ZbmCompiledFunction(compile(fn_string)())
        except .. as e,m
            raise "schema_compile_error",
                f"failed to compile function '{fn_string}' for {schema_item.debug_name} - {m} "
        end
    end

    static var schema_category_layout = {
        ZbmSchemaItem("catageory", _class.value_in(_class.categories), _class.OPTIONAL): {
            ZbmSchemaItem("element_name", _class.valid_name("element_name"), _class.OPTIONAL): {
                ZbmSchemaItem("functions", _class.value_in(_class.fn_components), _class.REQUIRED): ZbmSchemaItem("function_item",_class.valid_fn,_class.COMPILABLE,_class.compile_fn),
                ZbmSchemaItem("others", _class.value_in(["format_category"]), _class.OPTIONAL): ZbmSchemaItem("function_item",_class.valid_name( "name")),
            }
        },
        ZbmSchemaItem("config", "config", _class.OPTIONAL): {ZbmSchemaItem("config_item", _class.valid_name("config_key"), _class.OPTIONAL):nil},
        ZbmSchemaItem("include", "include", _class.OPTIONAL): ZbmSchemaItem("list", [ZbmSchemaItem(_class.valid_name("include_item"), _class.valid_name( "name"), _class.OPTIONAL)])
    }

    static var schema_layout = {
        ZbmSchemaItem("version", "version", _class.OPTIONAL):/value -> type(value) == "int" || type(value) == "real",
        ZbmSchemaItem("mappings", "mappings", _class.OPTIONAL): ZbmSchemaItem("mapping", {
            ZbmSchemaItem("mapping_item", _class.valid_mapping("mapping_key"), _class.OPTIONAL):  ZbmSchemaItem("mapping_target",_class.valid_name("mapping_target"))
        }, _class.UNIQUE),
        ZbmSchemaItem("schemas", "schemas", _class.OPTIONAL): 
            {ZbmSchemaItem("schema", _class.valid_name( "schema_name"), _class.OPTIONAL): _class.schema_category_layout}
            
    }
end

class ZbmSchemaProcessor

    static def process_schema_value(payload_el,schema_el,level,raise_error)

        var schema_item = nil
        if isinstance(schema_el, ZbmSchemaItem)
            schema_item = schema_el
            schema_el = schema_el.value
        end

        var make_result = /value,schema_el,schema_item -> ZbmStruct({"value":value,"schema_el":schema_el,"schema_item":schema_item})
        if schema_el == nil
            return make_result(true,schema_el,schema_item)
        elif ["int", "real", "string", "bool"].find(type(schema_el)) != nil
            if payload_el != schema_el
                if raise_error
                    raise "type_error", f"expected {schema_el} got {payload_el} at {level}"
                end
                return make_result(false,schema_el,schema_item)
            end
            return make_result(true,schema_el,schema_item)
        elif type(schema_el) == "function"
            if !schema_el(payload_el,schema_item)
                if raise_error
                    raise "validation_error", f"verification function failed for '{payload_el}' at {level}"
                end
                return make_result(false,schema_el,schema_item)
            end
            return make_result(true,schema_el,schema_item)
        end

        return make_result(false,schema_el,schema_item)
    end

    static def process_schema_element(payload_el, schema_el, level, storage,compile_fns)

        var _info = ZbmSchemaProcessorInfo    
        var next_storage = nil
        var result = nil
        var schema_item = nil

        var presult = _class.process_schema_value(payload_el,schema_el,level,true)
        schema_el = presult.schema_el
        schema_item = presult.schema_item

        if presult.value
            result = payload_el
        elif isinstance(schema_el,list)
            if !isinstance(payload_el,list)
                raise "type_error", f"expected list got '{type(payload_el)}'' with value '{payload_el}' at {level}"
            end
                
            if storage != nil
                if isinstance(schema_el[0],list)
                    next_storage =  []
                elif isinstance(schema_el[0],map)
                    next_storage =  {}
                end
                # a list is merged as a single value, the payload processed last replaces
                # it. appending would repeat every entry each time a schema is re-added
                storage.clear()
            end

            for pl_val : payload_el
                var value = _class.process_schema_element(pl_val, schema_el[0], level + 1, next_storage,compile_fns)
                if storage != nil
                    if storage && schema_item && (schema_item.flags & _info.UNIQUE)
                        if storage.find(value) != nil
                            raise "constraint_error",f"value '{value}' violates UNIQUE constraint at {level}"
                        end
                    end
                    storage.push(value)
                end
            end
            result = storage
        elif isinstance(schema_el,map)
            if !isinstance(payload_el,map)
                raise "type_error", f"expected dictionary got '{type(payload_el)}' with value '{payload_el}' at {level}"
            end
            
            var required_keys = []
            for schema_key : schema_el.keys()
                if isinstance(schema_key, ZbmSchemaItem)
                    if schema_key.flags & _info.REQUIRED
                        required_keys.push(schema_key)
                    end
                end
            end

            for payload_key : payload_el.keys()
                var key_matched = nil
                for schema_key : schema_el.keys()
                    presult = _class.process_schema_value(payload_key,schema_key,level,false)
                    if presult.value
                        schema_item = presult.schema_item
                        key_matched = schema_key
                        break
                    end
                end                    
                
                if key_matched != nil
                    list_remove(required_keys, key_matched, true)

                    
                    var schema_value = schema_el[key_matched]
                    
                    if isinstance(schema_value, ZbmSchemaItem)
                        schema_value = schema_value.value
                    end

                    if storage != nil
                        if isinstance(schema_value,list)
                            next_storage = storage.contains(payload_key) ? storage[payload_key] : []
                        elif isinstance(schema_value,map)
                            next_storage = storage.contains(payload_key) ? storage[payload_key] : {}
                        end
                    end

                    var value = _class.process_schema_element(payload_el[payload_key], schema_el[key_matched], level + 1, next_storage,compile_fns)
                    var transformed_key = payload_key
                    if schema_item && schema_item.transform != nil
                        #var fn = schema_item.transform
                        transformed_key = call(schema_item.transform,transformed_key,schema_item)
                        end                    
        
                    if storage != nil 
                        if schema_item && schema_item.flags & _info.UNIQUE && storage.contains(transformed_key)
                            raise "constraint_error", f"key '{transformed_key}' violates UNIQUE constraint at {level}"
                        end

                        storage[transformed_key] = value
                    end
                else
                    raise "schema_error",f"unknown key '{payload_key}' at {level}" 
                end
            end

            if size(required_keys) > 0
                var missing = _join(required_keys, ', ', / v -> str(v.value))
                raise "validation_error", f"missing required keys '{missing}' at level {level}"
            end
            result = storage
        else
            raise "type_error", f"invalid schema type {type(schema_el)=} {isinstance(schema_el,map)=} {isinstance(schema_el,list)=} at level {level}"
        end
        if schema_item && schema_item.transform != nil
            var compile_flag = schema_item.flags & _info.COMPILABLE
            if compile_flag && compile_fns || !compile_flag
                var fn = schema_item.transform
                result = fn(result,schema_item)
                end
        end

        return result
    end

    static def process_schemas(payload_list, schema_verifier, merge,compile_fns)
        if !isinstance(payload_list,list)
            payload_list = [payload_list]
        end

        var result ={}
        for payload_item : payload_list
            
            _class.process_schema_element(
                payload_item, 
                schema_verifier, 
                0, 
                merge ? result  : nil,
                compile_fns
            )
        end
        return !merge ? true : result 
    end

end

class ZbmSchemaRegistry : ZbmNotify
    
    #var registry
    var compiled_cache
    var log

    static var default_registry = {
        "version": 1.0,
        "mappings":{},
        "schemas": {}
    }

    def init()
        #sensors merged and sent to tele/<device-name>/SENSOR"
        #states merged and sent to tele/<device-name>/STATE"
        self.log = ZbmLogger("schema_registry")

        super(self).init({"registry":_copy(self.default_registry)})

        self.load_registry()
        self.compiled_cache = {}

    end

    def unload()

    end

    def save_registry()
        persist.zbm_registry = self.registry
        persist.save(true)
        self.log.debug("saved registry")
    end

    def load_registry()
        if persist.has("zbm_registry") && persist.zbm_registry != nil
            self.registry = _copy(persist.zbm_registry,true)
            self.remove_duplicate_includes()
            self.log.debug("loaded persisted registry")
        end
    end

    # registries saved by earlier versions can list the same include several times,
    # as every merge appended the include list to the one already stored
    def remove_duplicate_includes()
        for schema : self.registry.find("schemas",{})
            if isinstance(schema,map) && isinstance(schema.find("include"),list)
                var includes = []
                for include_name : schema["include"]
                    if includes.find(include_name) == nil
                        includes.push(include_name)
                    end
                end
                schema["include"] = includes
            end
        end
    end

    def reset()
        self.registry = _copy(self.default_registry)
        self.compiled_cache = {}
        persist.zbm_registry = nil
        persist.save(true)
    end

    def compile_schema(schema_name)
        try
            var schema_info = self.find_schemas(schema_name,true)

            var compiled_schema = ZbmSchemaProcessor.process_schemas(
                schema_info.schemas,
                ZbmSchemaProcessorInfo.schema_category_layout,
                true,
                true)

            self.compiled_cache[schema_name] = ZbmCompiledSchema(
                schema_name,
                compiled_schema,
                schema_info.includes,
                true)
        except .. as e,m
            self.log.debug(f"schema compile failed '{schema_name}' > {e} {m}")
            # set the valid flag to false, so constant recompiles of the schema are not tried
            self.compiled_cache[schema_name] = ZbmCompiledSchema(schema_name,nil,[],false)
        end
        self.log.debug(f"schema compiled succeeded '{schema_name}'")
    end   

    def find_schemas(schema_name,resolve_includes)

        var schemas = {}
        var search_list = [schema_name]
        var includes = {}
        if !self.registry.contains("schemas")
            raise "schema_error", f"unable to find schema {schema_name}, registry appears empty."
        end

        while search_list.size()
            var search_name = search_list[0]
            search_list.pop(0)

            if schemas.contains(search_name)
                continue
            end

            if !self.registry["schemas"].contains(search_name)
                raise "schema_error", f"unable to find schema {search_name}"
            end

            var found_schema = self.registry["schemas"][search_name]
            schemas[search_name] = found_schema
            if resolve_includes
                if found_schema.contains("include")
                    for include_name : found_schema["include"]
                        includes[include_name] = nil
                        search_list.push(include_name)
                    end
                end
            end

        end

        return ZbmStruct({"schemas":to_list(schemas.iter()),"includes":to_list(includes.keys())})
    end



    def set_registry(reset_value)
        self.registry = reset_value == nil ? _copy(self.default_registry) : _copy(reset_value,true)
        self.compiled_cache = {}
        self.save_registry()
    end

    def check_version(schema_json)
        if schema_json.contains("version") && (real(schema_json["version"]) != real(self.registry["version"]))
            raise "schema_error",f"mismatched schema version, registry is {self.registry['version']}, schema is {schema_json['version']}"
        end
    end

    # raises if schema_json could not be added to the registry
    def validate_schema(schema_json)
        self.check_version(schema_json)
        ZbmSchemaProcessor.process_schemas(
            schema_json,
            ZbmSchemaProcessorInfo.schema_layout,
            false,
            false)
    end

    def add_schema(schema_json)
        self.add_schemas([schema_json])
    end

    def add_schemas(schema_jsons)

        for schema_json : schema_jsons
            self.check_version(schema_json)
        end

        # the registry is merged first, so the schemas being added replace the values
        # already stored for them rather than the stored values winning
        var processed_schema = ZbmSchemaProcessor.process_schemas(
            [self.registry] + schema_jsons,
            ZbmSchemaProcessorInfo.schema_layout,
            true,
            false)

        self.set_registry(processed_schema)

    end

    # adds the schemas and mappings in schema_json, unlike add_schema a schema that is
    # already in the registry is replaced as a whole, so anything removed from it goes too
    def replace_schemas(schema_json)

        self.validate_schema(schema_json)

        var replaced = schema_json.find("schemas",{})
        var kept = {}
        for schema_name : self.registry["schemas"].keys()
            if !replaced.contains(schema_name)
                kept[schema_name] = self.registry["schemas"][schema_name]
            end
        end

        var processed_schema = ZbmSchemaProcessor.process_schemas(
            [{"version":self.registry["version"],"mappings":self.registry["mappings"],"schemas":kept},schema_json],
            ZbmSchemaProcessorInfo.schema_layout,
            true,
            false)

        self.set_registry(processed_schema)
    end

    def remove_schema(schema_name)
        if !self.registry["schemas"].contains(schema_name)
            return false
        end
        
        self.registry["schemas"].remove(schema_name)
        self.set_registry(self.registry)
        self.log.debug(f"removed {schema_name}")
        return true
    end

    def schema(schema_name)
        if self.compiled_cache.contains(schema_name)
            self.log.debug(f"found cached compiled_schema for {schema_name}")
            return self.compiled_cache[schema_name]
        end

        if !self.registry["schemas"].contains(schema_name)
            self.log.error(f"missing expected schema {schema_name}")            
            raise  "schema_error",f"missing expected schema {schema_name}"
        end
        
        self.compile_schema(schema_name)
        return self.compiled_cache[schema_name]
    end

    def add_mapping(key,schema_name)

        if !self.registry["schemas"].contains(schema_name)
            ZbmLogger().error(f"schema {schema_name} is not a valid schema.")
            return false
        end
        if self.registry["mappings"].contains(key)
            ZbmLogger().error(f"mapping already exists for {key}")
            return false
        end

        self.registry["mappings"][key] = schema_name
        self.set_registry(self.registry)
        self.log.debug(f"removed mapping {key} for {schema_name}")
        return true
    end

    def remove_mapping(key)
        if !self.registry["mappings"].contains(key)
            return false
        end
        self.registry["mappings"].remove(key)
        self.set_registry(self.registry)
        return true
    end

    def mapping(key)
        return self.registry["mappings"].contains(key) ? self.registry["mappings"][key] : nil
    end
    
end

class ZbmMqttBridge

    var topic_subscriptions
    var topic_cache
    var log 
    

    def init()
        self.log = ZbmLogger("mqtt_bridge")
        self.topic_cache = {}
        self.topic_subscriptions = []
        self.load_cache()
    end 

    def save_cache()
        persist.zbm_bridge_topic_cache = self.topic_cache
        persist.save(true)
    end

    def load_cache()
        if persist.has("zbm_bridge_topic_cache")
            self.topic_cache = _copy(persist.topic_cache,true)
            self.log.info("loaded persisted topic cache")
        end
    end
    
    def unload()
        for topic : self.topic_subscriptions
            mqtt.unsubscribe(topic)
        end
    end

    def cached_topic_data(topic)
        if !self.topic_cache.contains(topic)
            self.topic_cache[topic] = {}
        end
        return self.topic_cache[topic]
    end

    def dispatch_value(device_info,category_name,dispatch_datas)

        var topic_data = {}

        for dispatch_data : dispatch_datas
            
            var components = dispatch_data.components
            var entity_name = dispatch_data.entity_name
            var entity_index = dispatch_data.entity_index
            var value = dispatch_data.value

            if category_name == "relays"
                var state_topic = f"tele/{device_info.name}/STATE"

                var publish_value = (type(value) == "int" || type(value) == "bool") ? (value ? "ON" : "OFF") : value
                mqtt.publish(f"stat/{device_info.name}/POWER{entity_index+1}",publish_value)
                mqtt.publish(f"stat/{device_info.name}/RESULT",json.dump({f"POWER{entity_index+1}":publish_value}))

            elif category_name == "switches"

                var topic = f"tele/{device_info.name}/SENSOR"
                var cached_payload = self.cached_topic_data(topic)

                cached_payload[entity_name] = (type(value) == "int" || type(value) == "bool") ? (value ? "ON" : "OFF") : value
                cached_payload["Time"] = zb_timestr()

                if !topic_data.contains(topic)
                    topic_data[topic] = {"payload":cached_payload,"retain":zb_sensor_retain()}
                end    
            elif category_name == "states"

                var topic = f"tele/{device_info.name}/STATE"
                var cached_payload = self.cached_topic_data(topic)

                cached_payload[entity_name] = value
                cached_payload["Time"] = zb_timestr()

                if !topic_data.contains(topic)
                    topic_data[topic] = {"payload":cached_payload,"retain":zb_state_retain()}
                end 
            elif category_name == "sensors"
                var topic = f"tele/{device_info.name}/SENSOR"
                var cached_payload = self.cached_topic_data(topic)

                cached_payload["Time"] = zb_timestr()

                if components.contains("format_category")
                    var format_category = components["format_category"]
                    if !cached_payload.contains(format_category)
                        cached_payload[format_category] = {entity_name:value}
                    else
                        cached_payload[format_category][entity_name] = value
                    end
                else
                    cached_payload[entity_name] = value
                end

                if !topic_data.contains(topic)
                    topic_data[topic] = {"payload":cached_payload,"retain":zb_sensor_retain()}
                end
            end
        end

        for topic : topic_data.keys()
            mqtt.publish(
                topic,
                json.dump(topic_data[topic]["payload"]),
                topic_data[topic]["retain"]
            )
        end
    end

    def relay_command_handler(device_info,entity_index,relay_components,relay_name,payload)
        self.log.debug(f"recieved command {payload} for {relay_name}")

        var payload_value = payload
        if type(payload) == "string"
            if string.toupper(payload) == "ON"
                payload_value = 1
            elif string.toupper(payload) == "OFF"
                payload_value = 0
            else
                self.log.error(f"unparseable payload value {payload} for relay")
            end
        elif ["int","bool"].find(type(payload)) != nil
            payload_value = payload_value ? 1 : 0
        else
            self.log.error(f"unparseable payload value {payload} for relay")
        end

        zb_invoke_handler("set_value",relay_components,device_info,payload_value)

        # self.log.debug("publishing early response")
        # mqtt.publish(f"stat/{device_info.name}/POWER{entity_index+1}",string.toupper(payload))
        # mqtt.publish(f"stat/{device_info.name}/RESULT",json.dump({f"POWER{entity_index+1}":string.toupper(payload)}))        
    end 

    def configure_device(device_info,compiled_schema)

        var state_map = tasmota.cmd("State")
        var status_map = tasmota.cmd("Status")["Status"]

        var relays = []
        var switches = []
        var switch_names = []
        var buttons = []

        var battery = 0
        var deepsleep = 0 

        if compiled_schema.schema.contains("config")
            if compiled_schema.schema["config"].contains("battery")
                battery = compiled_schema.schema["config"]["battery"] ? 1 : 0
            end
            if compiled_schema.schema["config"].contains("deepsleep")
                deepsleep = compiled_schema.schema["config"]["deepsleep"] ? 1 : 0
            end
        end

        if compiled_schema.schema.contains("relays")

            var relay_state_keys = []
            var index = 1 
            for relay_name :  compiled_schema.schema["relays"].keys()
                var relay_components = compiled_schema.schema["relays"][relay_name] 
                relays.push(1)
                
                if relay_components.contains("set_value")
                    var topic = f"cmnd/{device_info.name}/Power{index}"
                    # a device is configured again when mqtt reconnects or its schema is
                    # pulled again. drop the listener added last time, otherwise every
                    # command would be sent to the device once per configuration
                    if self.topic_subscriptions.find(topic) != nil
                        mqtt.unsubscribe(topic)
                    else
                        self.topic_subscriptions.push(topic)
                    end
                    mqtt.subscribe(topic,
                        /topic,index,payload_s,payload_b -> 
                            self.relay_command_handler(device_info,index,relay_components,relay_name,payload_s) 
                    )
                else
                    self.log.error(f"relay '{relay_name}' missing expected set_value handler")
                    compiled_schema.valid =  false
                end
                index +=1
            end
            
        end

        if compiled_schema.schema.contains("switches")
            for switch_name : compiled_schema.schema["switches"].keys()
                switches.push(1)
                switch_names.push(switch_name)
            end
        end

        self.log.debug(f"dispatching discovery config topic for {device_info.name}")
        mqtt.publish(
            f"tasmota/discovery/{device_info.macaddr}/config",
            json.dump({
                "ip": state_map["IPAddress"],
                "dn": device_info.name,
                "fn": [device_info.name],
                "hn": state_map["Hostname"],
                "mac": device_info.macaddr,
                "md": device_info.model,
                "ty": 0,
                "if": 0,
                "cam": 0,
                "ofln": "Offline",
                "onln": "Online",
                "state": ["OFF", "ON", "TOGGLE", "HOLD"],
                "sw": "1.0",
                "t": device_info.name,
                "ft": "%prefix%/%topic%/",
                "tp": ["cmnd", "stat", "tele"],
                "rl": relays,
                "swc": switches,
                "swn": switch_names,
                "btn": buttons,
                "so": {
                    "4": 0,
                    "11": 0,
                    "13": 0,
                    "17": 0,
                    "20": 0,
                    "30": 0,
                    "68": 0,
                    "73": 1,
                    "82": 0,
                    "114": 0,
                    "117": 0,
                },
                "lk": 0,
                "lt_st": 0,
                "bat": battery,
                "dslp": deepsleep,
                "sho": [],
                "sht": [],
                "ver": 1,
                }
            ),
            true
        )
        
        self.log.debug(f"dispatching discovery sensor topic for {device_info.name}")
        if compiled_schema.schema.contains("sensors")
            var payload = {"sn":{"Time":zb_timestr()},"ver":1}
            var sensors_payload = payload["sn"]
            for sensor_name : compiled_schema.schema["sensors"].keys()
                var sensor_detail = compiled_schema.schema["sensors"][sensor_name]

                if sensor_detail.contains("format_category")
                    var format_category = sensor_detail["format_category"]
                    if !sensors_payload.contains(format_category)
                        sensors_payload[format_category] = {sensor_name:nil}
                    else
                        sensors_payload[format_category][sensor_name] = nil
                    end
                else
                    sensors_payload[sensor_name] = nil
                end
            end

            mqtt.publish(
                f"tasmota/discovery/{device_info.macaddr}/sensors",
                json.dump(payload),
                true
            )
        end
    end

    def request_device_values(device_info,compiled_schema)
        for category_name : ZbmSchemaProcessorInfo.categories
            if !compiled_schema.schema.contains(category_name)
                continue
            end
            
            var entity_list = compiled_schema.schema[category_name]
            for entity_name : entity_list.keys()
                
                var components = entity_list[entity_name]
                var result

                if [zb_handler_invalid,zb_handler_error].find(result := zb_invoke_handler("request_value",components,device_info)) != nil
                    if result == zb_handler_error 
                        self.log.debug(f"an error occurred executing request_value handler for {device_info.shortaddr}:{compiled_schema.name}:{category_name}:{entity_name}")
                    end
                end
            end
        end   
    end

    def dispatch_online_state(device_info,value)
        self.log.debug(f"dispatching online state {value} for {device_info.name}")
        mqtt.publish(f"tele/{device_info.name}/LWT",value ? "Online" : "Offline",true)
    end
end


class ZbmState : ZbmNotify

    var log
    static var config_defaults = {
        "auto_poll_devices":true,
        "auto_poll_devices_period":5,
        "auto_add_devices":false,
        "auto_remove_devices":true,
        "auto_name_devices":false,
        "auto_key_devices":true,
        "log_level":2
    }

    def init()

        var info = {
            "zigbee_started":zigbee.started(),
            "mqtt_connected":mqtt.connected(),
        }

        for config_key : self.config_defaults.keys()
            info[config_key] = self.config_defaults[config_key]
        end

        self.log = ZbmLogger("state")

        super(self).init(info)
        self.load_state()
        self.on_changed("*",/value -> self.value_changed(value))
        tasmota.add_driver(self)
        end 

    def reset()
        for key : ZbmState.config_defaults
            zbm_state.info[key] = ZbmState.config_defaults[key]
        end
        persist.zbm_state  = nil
        persist.save(true)
    end

    def value_changed(value)
        self.save_state()
    end

    def save_state()
        var saved_state = {}
        for key : self.config_defaults.keys()
            saved_state[key] = self.info[key]
        end
        persist.zbm_state = saved_state
        persist.save(true)
        self.log.debug("saved state")
    end

    def load_state()
        if persist.has("zbm_state") && persist.zbm_state != nil
            for key : persist.zbm_state.keys()

                self.info[key] = persist.zbm_state[key]
            end            
            self.log.debug(f"loaded persisted state {self.info}")
        end
    end

    def unload()
        tasmota.remove_driver(self)
    end

    def every_second()
        self.zigbee_started = zigbee.started()
        self.mqtt_connected = mqtt.connected()
    end

end

class ZbmService

    var device_infos
    var poll_count
    var log


    def init()
        self.log = ZbmLogger("device_manager")
        self.device_infos = {}
        self.load_devices()
        self.poll_count = 0
        zigbee.add_handler(self)
        zbm_state.on_changed("auto_poll_devices",/value -> self.on_auto_poll_devices_changed(value))
        zbm_state.on_changed("zigbee_started",/value -> self.on_zigbee_status_changed(value))
        zbm_state.on_changed("mqtt_connected",/value -> self.on_mqtt_status_changed(value))
        zbm_schema_registry.on_changed("registry",/value -> self.on_registry_changed(value))

        self.on_mqtt_status_changed(zbm_state.mqtt_connected)        
        self.on_auto_poll_devices_changed(zbm_state.auto_poll_devices)
        self.log.info("initialized")
    end

    def unload()
        zigbee.remove_handler(self)
        tasmota.remove_driver(self)
        self.log.info("uninitialized")
    end

    def save_devices()
        var saved_devices = {}
        for key : self.device_infos.keys()

            var device_info = self.device_infos[key]
            if device_info.status != 0
                saved_devices[key] = device_info.info
            end
            
        end
        self.log.debug(f"saved devices.")
        persist.zbm_device_infos = saved_devices
        persist.save(true)
    end

    def load_devices()
        if persist.has("zbm_device_infos") && persist.zbm_device_infos != nil
            for key : persist.zbm_device_infos.keys()
                var shortaddr = persist.zbm_device_infos[key]["shortaddr"]
                var device_info = ZbmDeviceInfo(persist.zbm_device_infos[key])
                self.device_infos[shortaddr] = device_info
                self.log.debug(f"loaded persisted device {self.device_infos[shortaddr]}")
            end
        end
    end

    def reset()
        self.device_infos = {}
        persist.zbm_device_infos = nil
        persist.save(true)
    end
    
    def save_before_restart()

        for device_info : self.device_infos.iter()
            zbm_mqtt_bridge.dispatch_online_state(device_info,false)
        end
        self.save_devices()
    end

    def on_auto_poll_devices_changed(should_poll)
        self.log.debug(f"auto_poll_devices status {should_poll}")
        should_poll ? tasmota.add_driver(self) : tasmota.remove_driver(self)
    end

    def on_zigbee_status_changed(available)
        self.on_mqtt_status_changed(available)
        # for device_info : self.device_infos.iter()
        #     if device_info.status != ZbmDeviceStatus.Added
        #         self.log.debug(f"Skipping {device_info.name}, not added")
        #         continue
        #     end
        #     zbm_mqtt_bridge.dispatch_online_state(device_info,available)
        #     zbm_mqtt_bridge.request_device_values(device_info)
        # end
        # self.log.debug(f"zigbee status changed to {available}")
    end

    def on_mqtt_status_changed(available)
        if available
            for device_info : self.device_infos.iter()
                if device_info.status != ZbmDeviceStatus.Added
                    self.log.debug(f"Skipping {device_info.name}, not added")
                    continue
                end
                if !self.configure_device(device_info)
                    self.log.debug(f"unable to confoigure device {device_info.name}")
                end
            end
        end
        self.log.debug(f"mqtt status changed to {available}")
    end

    def on_registry_changed(registry)

        #clear bad mapping flags, let them try again

        var schema_flags = ZbmDeviceStatus.MappingNotFound | 
                           ZbmDeviceStatus.SchemaNotFound |
                           ZbmDeviceStatus.SchemaCompileFailed
    
        for device_info : self.device_infos.iter()
            if device_info.status & schema_flags
                device_info.status &= ~schema_flags
            end
        end
        self.log.debug(f"registry updated, removed schema error flags")
    end

    def add_available_device(zb_device)

        if !self.has_device(zb_device.shortaddr)
            self.device_infos[zb_device.shortaddr] = ZbmDeviceInfo(zb_device,0)
            self.log.debug(f"available device {self.device_infos[zb_device.shortaddr].deviceid}")
        else
            self.update_device(ZbmDeviceInfo(zb_device))
        end
        return self.device_infos[zb_device.shortaddr]
    end

    def update_available_devices()

        var available_devices = []
        for zb_device : zigbee
            available_devices.push(zb_device.shortaddr)
            var device_info = self.add_available_device(zb_device)
            if zbm_state.auto_add_devices
                self.add_device(device_info)
            end
        end

        var remove_devices = []
        for device_info : self.device_infos.iter()
            if available_devices.find(device_info.shortaddr) == nil
                device_info.status |= ZbmDeviceStatus.NotFound
                if zbm_state.auto_remove_devices
                    remove_devices.push(device_info)
                end
            end
        end

        for device_info : remove_devices
            self.remove_device(device_info,true)
        end
    end

    def every_second()
        if zbm_state.auto_poll_devices
            if (self.poll_count := (self.poll_count+1) % zbm_state.auto_poll_devices_period) != 0
                return
            end
            self.update_available_devices()
        end
    end

    def auto_name_device(device_info)
        if device_info.name != nil && strip(device_info.name) != ""
            return false
        end
 
        if (device_info.manufacturer != nil && device_info.manufacturer == "") ||
            (device_info.model != nil && device_info.model == "")
            return false
        end

        var pending_name = f"{device_info.manufacturer}-{device_info.model}"
        var pending_index = 1

        for existing_dev_info : self.device_infos.iter()
            if string.startswith(existing_dev_info.name,pending_name)
                var digit_part = int(existing_dev_info.name.split(" ")[-1])
                if digit_part > pending_index
                    pending_index = digit_part+1
                end
            end
        end
        pending_name = f"{pending_name} {pending_index}"
        device_info.name = pending_name
        tasmota.cmd(f"ZbName {device_info.deviceid},{pending_name}")
        return pending_name
    end

    def device_mapping(device_info)
        var schema_name

        if (schema_name := zbm_schema_registry.mapping(device_info.key)) == nil
            self.log.debug(f"mapping not found '{device_info.key}'")
            device_info.status |= ZbmDeviceStatus.MappingNotFound
            return nil
        end
        return schema_name
    end

    def device_schema(device_info,schema_name)
        var compiled_schema
        try
            compiled_schema = zbm_schema_registry.schema(schema_name)
        except "schema_error"
            self.log.debug(f"invalid schema - schema '{schema_name}' does not exist")
            device_info.status |= ZbmDeviceStatus.SchemaNotFound
            return nil
        end

        if !compiled_schema.valid
            self.log.debug(f"invalid schema - schema '{schema_name}' failed to compile")
            device_info.status |= ZbmDeviceStatus.SchemaCompileFailed
            return nil
        end
        return compiled_schema
    end

    def update_device(device_info)
        if introspect.toptr(self.device_infos[device_info.shortaddr]) == introspect.toptr(device_info)
            return device_info
        end

        self.device_infos[device_info.shortaddr].update(device_info)
        return self.device_infos[device_info.shortaddr]
    end

    def remove_device(device_info)
        if !self.device_infos.contains(device_info.shortaddr)
            return nil    
        end

        
        if device_info.status & ZbmDeviceStatus.Added
            zbm_mqtt_bridge.dispatch_online_state(device_info,false)
        end

        if device_info.status & ZbmDeviceStatus.NotFound
            self.device_infos.remove(device_info.shortaddr)
            self.log.info(f"removed not found device name:{device_info.name} shortaddr:{device_info.deviceid}")

        else
            device_info.status = ZbmDeviceStatus.Removed
            self.log.info(f"removed device name:{device_info.name} shortaddr:{device_info.deviceid}")
        end
        self.save_devices() 
        return true
    end

    def reset_device(device_info)
        if !self.device_infos.contains(device_info.shortaddr)
            return nil    
        end
        device_info.status = 0
        self.log.info(f"reset device name:{device_info.name} shortaddr:{device_info.deviceid}")
        self.save_devices()        
        return true
    end

    def has_device(shortaddr)
        return self.device_infos.contains(shortaddr)
    end

    def update_device_status(device_info)
        if device_info.status & ZbmDeviceStatus.Unnamed
            if ["",nil].find(device_info.name) == nil
                device_info.status &= ~ZbmDeviceStatus.Unnamed
            end
        end
        if device_info.status & ZbmDeviceStatus.NoDefaultKey
            if ["",nil].find(strip(device_info.manufacturer)) == nil &&
                ["",nil].find(strip(device_info.model)) == nil
                device_info.status &= ~ZbmDeviceStatus.NoDefaultKey
            end
        end
    end

    def add_device(device_info)

        self.update_device_status(device_info)
        
        if device_info.status != ZbmDeviceStatus.Available
            return nil
        end

        if device_info.key == nil
            if zbm_state.auto_key_devices
                if ["",nil].find(strip(device_info.manufacturer)) != nil &&
                        ["",nil].find(strip(device_info.model)) != nil
                    self.log.debug(f"unable to assign default key manufacturer and/or model are empty")
                    device_info.status |= ZbmDeviceStatus.NoDefaultKey
                    return nil
                end
                self.log.debug(f"assiging default key '{device_info.manufacturer}:{device_info.model}'")
                device_info.key = f"{device_info.manufacturer}:{device_info.model}"
            else
                self.log.debug(f"unable to add device, key required, auto_key_devices=false")                
                return nil
            end
        end

        if ["",nil].find(device_info.name) != nil
            if zbm_state.auto_name_devices
                if !self.auto_name_device(device_info)
                    self.log.debug(f"unable to add device - name required and auto name failed (missing info) - use 'ZbName shortaddr, name'")
                    device_info.status |= ZbmDeviceStatus.Unnamed
                end
                return nil
            else
                self.log.debug(f"unable to add device, name required, auto_name_devices=false")
                device_info.status |= ZbmDeviceStatus.Unnamed
                return nil
            end
        end

        return self.configure_device(device_info)
    end

    def configure_device(device_info)

        var schema_name
        var compiled_schema

        if (schema_name := self.device_mapping(device_info)) == nil
            return nil
        end

        if (compiled_schema := self.device_schema(device_info,schema_name)) == nil
            return nil            
        end

        device_info.status |= ZbmDeviceStatus.Added 
        self.log.info(f"configured device name:{device_info.name} shortaddr:{device_info.shortaddr}")
        self.save_devices()
        if zbm_state.mqtt_connected
            zbm_mqtt_bridge.configure_device(device_info,compiled_schema)
            zbm_mqtt_bridge.request_device_values(device_info,compiled_schema)
            tasmota.set_timer(4,/ -> zbm_mqtt_bridge.dispatch_online_state(device_info,zbm_state.zigbee_started))
        else
            self.log.info(f"device not configured mqtt offline name:{device_info.name} shortaddr:{device_info.shortaddr}")
        end


        #tasmota.set_timer(5,/ -> zbm_mqtt_bridge.dispatch_online_state(device_info,true))
        #tasmota.set_timer(2,/ -> zbm_mqtt_bridge.request_device_values(device_info,compiled_schema))
        return self.device_infos[device_info.shortaddr]

    end

    def attributes_final(event_type, frame, raw_attr_list, idx)

        self.log.debug(f"{event_type},{raw_attr_list},{idx}")

        var zb_device  = zigbee[idx]
        var device_info = self.add_available_device(zb_device)

        if zbm_state.auto_add_devices
            self.add_device(device_info)
        end

        if device_info.status & ZbmDeviceStatus.ErrorFlag || !(device_info.status & ZbmDeviceStatus.Added)
            return
        end

        var schema_name
        var compiled_schema

        if (schema_name := self.device_mapping(device_info)) == nil
            return
        end

        if (compiled_schema := self.device_schema(device_info,schema_name)) == nil
            return
        end

        self.log.debug(f"Got schema for {device_info.shortaddr}")

        var attr_list = json.load(str(raw_attr_list))

        #given the zigbee payload try each of the has_value (optional if value is gaurenteed) and parse_value to extract the value for each entity (sensor/switch/relay/command/..)
        for category_name : ZbmSchemaProcessorInfo.categories
            if !compiled_schema.schema.contains(category_name)
                continue
            end
            var dispatch_data = []
            var reset_dispatch_data = []

            var entity_list = compiled_schema.schema[category_name]
            var entity_index = 0


            for entity_name : entity_list.keys()

                var components = entity_list[entity_name]
                var log_debug_error = /handler_name ->
                    self.log.debug(f"an error occurred executing {handler_name} handler for {device_info.shortaddr}:{schema_name}:{category_name}:{entity_name}")

                #instead if disabling the functions, the whole schema should probably be made invalid if the callables aren't runnable?
                var result
                if [zb_handler_invalid,zb_handler_error].find(result := zb_invoke_handler("has_value",components,device_info,attr_list)) != nil
                    if result == zb_handler_error 
                        log_debug_error("has_value")
                        continue
                    end
                elif !result 
                    continue 
                end

                var value
                if [zb_handler_invalid,zb_handler_error].find(value := zb_invoke_handler("parse_value",components,device_info,attr_list)) != nil
                    if value == zb_handler_error 
                        log_debug_error("parse_value")
                    end
                    continue
                else
                    self.log.debug(f"new value for {device_info.shortaddr}:{schema_name}:{category_name}:{entity_name}={value}")
                    dispatch_data.push(ZbmStruct({"components":components,"entity_name":entity_name,"entity_index":entity_index,"value":value}))                    
                end
                
                var reset_value #temporary solution to have a push button masqurade as a sensor and 'toggle' its value
                if [zb_handler_invalid,zb_handler_error].find(reset_value := zb_invoke_handler("reset_value",components,device_info,attr_list)) == nil
                    self.log.debug(f"new reset for {device_info.shortaddr}:{schema_name}:{category_name}:{entity_name}={reset_value}")
                    reset_dispatch_data.push(ZbmStruct({"components":components,"entity_name":entity_name,"entity_index":entity_index,"value":reset_value}))  
                end

                entity_index +=1
            end
            zbm_mqtt_bridge.dispatch_value(device_info,category_name,dispatch_data)
            zbm_mqtt_bridge.dispatch_value(device_info,category_name,reset_dispatch_data)
        end
    end

end

class ZbmTransformed : ZbmStruct
    def init(name,fn)
        super(self).init({"name":name,"fn":fn})
    end
end

class ZbmOptional : ZbmStruct
    def init(name)
        super(self).init({"name":name})
    end
end

def valid_integer(value)
    if ["real","int"].find(type(value)) != nil
        return int(value)
    elif re.match("^(?:\\d+(?:\\.\\d+)|0x[a-fA-F0-9]+)?$",value) == nil
        raise "validation_error",f"expected integer"
    end
    return int(value)
end


def no_space(value)
    if string.find(value," ") >= 0 || string.find(value,"\t") >= 0
        raise "validation_error",f"expected string without spaces, got '{value}'"
    end
    return int(value)
end

def args_from_payload(payload,payload_json,arg_spec)
    var arguments = []
    var argument_names = []
    var num_optional = 0
    var num_arguments = 0
    for arg : arg_spec
        var arg_info = ZbmStruct({"name":nil,"value":nil,"optional":nil,"transform":nil})
        while true
            if classof(arg) == ZbmOptional
                arg_info.optional = true
                arg = arg.name
                continue
            end
            if classof(arg) == ZbmTransformed
                arg_info.transform = arg.fn
                arg = arg.name
                continue
            end
            arg_info.name = arg
            argument_names.push(arg)
            break
        end
        arguments.push(arg_info)
        num_optional += int(!!arg_info.optional)
        num_arguments += 1
    end

    def validate_arity(arg_size,args_min,args_max)
        if arg_size < args_min ||  arg_size > args_max
            var format_count = args_min == args_max ? f"{args_min}" :
                f"between {args_min} and {args_max}"
            raise "argument_error",f"invalid number of arguments, expected {format_count} got {arg_size}."
        end
    end

    def process_argument(argument,value)
        if argument.transform != nil
            try
                value = call(argument.transform,value)
            except .. as e,m
                raise "argument_error",f"transform failed with '{value}' for argument '{argument.name}' - {e}, {m}."
            end
        end
        argument.value = value    
    end

    var argument_map = {}
    for i : 0..size(arguments)-1
        argument_map[argument_names[i]] = arguments[i]
    end

    def process_keyed_argument(key,value)
        if !argument_names.find(key) == nil
            raise "argument_error",f"unknown argument '{key}' in json fragment"
        end
        process_argument(argument_map[key],value)
    end

    if payload_json != nil && type(payload_json) != "string"
        validate_arity(payload_json.size(),num_arguments-num_optional,num_arguments)
        for key : payload_json.keys()
            process_keyed_argument(key,payload_json[key])
        end
    elif payload != nil && size(payload)
        var split_payload = string.split(payload,",")
        validate_arity(split_payload.size(),num_arguments-num_optional,num_arguments)
        var is_assignment
        for i : 0..split_payload.size()-1

            #assignment argument 
            var matches = re.match(f"^((?:[a-zA-Z0-9_().]|-)+)=((?:[a-zA-Z0-9_().]|-)+)$",split_payload[i])
            if matches != nil
                if is_assignment == false
                    raise "argument_error",f"positional argument already found. invalid assigned argument '{split_payload[i]}'"
                end
                if size(matches) != 3
                    raise "argument_error",f"assigned values must be formatted key=value, got '{split_payload[i]}'"
                end 
                is_assignment = true
                process_keyed_argument(matches[1],matches[2])
            else
                if is_assignment == true
                    raise "argument_error",f"assigned argument already found. invalid positional argument '{split_payload[i]}'"
                end
                process_argument(arguments[i],split_payload[i])
            end
        end
    else
        validate_arity(0,num_arguments-num_optional,num_arguments)
    end

    for i : 0..arguments.size()-1
        var argument = arguments[i]

        if !argument.optional && argument.value == nil
            raise "argument_error",f"required argument {i} missing, '{argument.name}'"
        end
    end
    return arguments
end

def log_cmnd(cmnd_name,log_type,message)
    if log_type == "error"
        ZbmLogger(cmnd_name).error(f'{cmnd_name} error {message}')
        
    elif log_type == "info"
        ZbmLogger(cmnd_name).info(f'{cmnd_name} info {message}')
    end

    return nil
end

def log_cmnd_error(cmnd_name,message)
    var result = log_cmnd(cmnd_name,"error",message)
    tasmota.resp_cmnd_error()
    return result
end
def log_cmnd_info(cmnd_name,message)
    return log_cmnd(cmnd_name,"info",message)
end

def parse_args(cmnd_name,payload,payload_json,arg_spec)

    try
        return args_from_payload(payload,payload_json,arg_spec)
    except .. as e,m
        log_cmnd_error(cmnd_name,f"failed with {e}, {m}")
        tasmota.resp_cmnd_error()
        return nil
    end
end


def find_device(cmnd_name, idx, payload, payload_json,arg_spec)

        
    var parsed_args 
    if (parsed_args := parse_args(cmnd_name,payload,payload_json,arg_spec)) == nil
        return ZbmStruct({"device":nil,"identifier":nil})
    end

    var deviceid = parsed_args[0].value
    var devicename = parsed_args[1].value    

    if deviceid == nil && devicename == nil
        log_cmnd_error(cmnd_name,f"argument_error, either 'devicename' or 'deviceid' required")
        return ZbmStruct({"device":nil,"identifier":nil})
    end

    zbm_service.update_available_devices()

    var identifier
    var found_device

    for device_info : zbm_service.device_infos.iter()
        if deviceid != nil 
            identifier = deviceid
            if device_info.shortaddr ==  int(deviceid)
                found_device = device_info
                break
            end
        elif devicename != nil
            identifier = devicename
            if !(device_info.status & ZbmDeviceStatus.Unnamed) && device_info.name == devicename
                found_device = device_info
                break
            end
        end
    end
    return ZbmStruct({"device":found_device,"identifier":identifier})
end

def zbm_add_device(cmnd_name, idx, payload, payload_json)

    var arg_spec = [ZbmOptional(ZbmTransformed("deviceid",valid_integer)),
                    ZbmOptional("devicename"),
                    ZbmOptional("devicekey")]

    var found_info = find_device(cmnd_name, idx, payload, payload_json,arg_spec)
    if found_info.device == nil
        return log_cmnd_error(cmnd_name,f"devicename or deviceid {found_info.identifier} not found")
    end

    if zbm_service.add_device(found_info.device) == nil
        return log_cmnd_error(cmnd_name,f"{found_info.device.name} cannot be added - {ZbmDeviceStatus.descriptions(found_info.device.status)}")     
    end
    
    tasmota.resp_cmnd_done()
end

def zbm_remove_device(cmnd_name, idx, payload, payload_json)

    var arg_spec = [ZbmOptional(ZbmTransformed("deviceid",valid_integer)),
                    ZbmOptional("devicename")]

    var found_info = find_device(cmnd_name, idx, payload, payload_json,arg_spec)
    if found_info.device == nil
        return log_cmnd_error(cmnd_name,f"devicename or deviceid {found_info.identifier} not found")
    end

    if zbm_service.remove_device(found_info.device) == nil
        return log_cmnd_error(cmnd_name,f"{found_info.device.name} cannot be removed.")     
    end
    
    tasmota.resp_cmnd_done()
end

def zbm_reset_device(cmnd_name, idx, payload, payload_json)

    var arg_spec = [ZbmOptional(ZbmTransformed("deviceid",valid_integer)),
                    ZbmOptional("devicename")]


    if re.match("^[Aa][Ll][Ll]$",payload)
        for device : zbm_service
            if zbm_service.reset_device(device.device) == nil
                return log_cmnd_error(cmnd_name,f"{device.device.name} cannot be reset.")     
            end     
        end   
    else
        var found_info = find_device(cmnd_name, idx, payload, payload_json,arg_spec)
        if found_info.device == nil
            return log_cmnd_error(cmnd_name,f"devicename or deviceid {found_info.identifier} not found")
        end

        if zbm_service.reset_device(found_info.device) == nil
            return log_cmnd_error(cmnd_name,f"{found_info.device.name} cannot be reset.")     
        end
    end
    
    tasmota.resp_cmnd_done()
end

def zbm_add_schema(cmnd_name, idx, payload, payload_json)

    var schema_json
    if payload_json != nil 
        schema_json = payload_json
    elif payload != nil
        schema_json = json.load(schema_json)
    end

    if schema_json == nil
        log_cmnd_error(cmnd_name,'expected a json fragment {"version": 1.0,"mappings":..,"schemas":..}')
        return tasmota.resp_cmnd_error()
    end

    try
        zbm_schema_registry.add_schema(schema_json)
    except .. as e,m
        return log_cmnd_error(cmnd_name,f"{e,m}")
    end

    tasmota.resp_cmnd_done()
end

def zbm_add_mapping(cmnd_name, idx, payload, payload_json)  

    var parsed_args 
    if (parsed_args:= parse_args(cmnd_name,payload,payload_json,["key","schema"])) == nil
        return tasmota.resp_cmnd_error()
    end

    var key = parsed_args[0].value
    var schema_name = parsed_args[1].value

    for part : [key,schema_name]
        if string.startswith(part," ")
            log_cmnd_info(cmnd_name,"value {part} has a leading space, possible error.")
        end
    end

    if !zbm_schema_registry.add_mapping(key,schema_name)
        return tasmota.resp_cmnd_error()
    end

    tasmota.resp_cmnd({"ZbmAddMapping":f"{key}:{schema_name}"})
end

def zbm_remove_mapping(cmnd_name, idx, payload, payload_json)
    var parsed_args 
    if (parsed_args:= parse_args(cmnd_name,payload,payload_json,["key"])) == nil
        return tasmota.resp_cmnd_error()
    end

    var key = parsed_args[0].value
    if !zbm_schema_registry.remove_mapping(key)
        return tasmota.resp_cmnd_error()
    end
    tasmota.resp_cmnd({"ZbmRemovemapping":f"{key}"})
end



def zbm_device(cmnd_name, idx, payload, payload_json)
    var parsed_args 
    if (parsed_args:= parse_args(cmnd_name,payload,payload_json,["deviceid"])) == nil
        return tasmota.resp_cmnd_error()
    end

    var status = {"ZbmStatus":[]}
    for device_info : zbm_service.device_infos.iter()
        if device_info.deviceid == parsed_args[0].value
            
            var desc = ZbmDeviceStatus.descriptions(device_info.status)
            var device_status = {"devicename":device_info.name,
                                "deviceid":device_info.deviceid,
                                "status":desc}

            status["ZbmStatus"].push(device_status)
            var devicename = (device_info.name != "" && device_info.name != nil) ? device_info.name : "<unnamed>"
            ZbmLogger("status").info(f"[{devicename} (0x{device_info.shortaddr:.4X})]")
            ZbmLogger("status").info(f"   manufacturer: {device_info.manufacturer}")
            ZbmLogger("status").info(f"         model: {device_info.model}")
            ZbmLogger("status").info(f"     shortaddr: 0x{device_info.shortaddr:.4X}")
            ZbmLogger("status").info(f"      longaddr: 0x{device_info.longaddr}")
            ZbmLogger("status").info(f"           mac: {device_info.macaddr}")
            var format_time = tasmota.strftime('%Y-%m-%dT%H:%M:%S',device_info.lastseen)
            ZbmLogger("status").info(f"      lastseen: {format_time}")
            ZbmLogger("status").info(f"           lqi: {device_info.lqi}")
            ZbmLogger("status").info(f"       battery: {device_info.battery}")
            ZbmLogger("status").info(f"           key: {device_info.key}")
            ZbmLogger("status").info(f"        status: {desc}")
            return tasmota.resp_cmnd(status)
            
        end
    end
    return tasmota.resp_cmnd_error()
end

def zbm_devices(cmnd_name, idx, payload, payload_json)

    var status = {"ZbmStatus":[]}
    for device_info : zbm_service.device_infos.iter()
        var desc = ZbmDeviceStatus.descriptions(device_info.status)
        var device_status = {"devicename":device_info.name,
                             "deviceid":device_info.deviceid,
                             "status":desc}

        status["ZbmStatus"].push(device_status)
        var devicename = (device_info.name != "" && device_info.name != nil) ? device_info.name : "<unnamed>"
        ZbmLogger("status").info(f"[{devicename} (0x{device_info.shortaddr:.4X})] {desc}")
    end
    tasmota.resp_cmnd(status)
end

def zbm_config(cmmd_name,idx,payload,payload_json)

    if !size(payload) && payload_json == nil
        var config = {"ZbmConfig":{}}
        for config_key : ZbmState.config_defaults.keys()
            config["ZbmConfig"][config_key] = zbm_state.info[config_key] 
            log_cmnd_info(cmmd_name,f"{config_key} = {zbm_state.info[config_key]}")
        end
        return tasmota.resp_cmnd(config)
    end


    def transform_config_arg(argv,ttype)
        if type(argv) == ttype
            return argv
        end
        if ttype == "bool"
            if type(argv) == "string"
                if re.match("^([Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|1)$",argv)
                    return true
                elif re.match("^([Ff][Aa][Ll][Ss][Ee]|[Nn][Oo]|1)$",argv)
                    return false
                end
            end
            return bool(argv)
        elif ttype == "int"
            return int(argv)
        elif ttype == "real"
            return real(argv)
        elif ttype == "string"
            return string(argv)
        end
    end

    var arg_list = []
    for arg_name : ZbmState.config_defaults.keys()
        var target_type = type(ZbmState.config_defaults[arg_name])
        arg_list.push(ZbmOptional(ZbmTransformed(arg_name,/arg -> transform_config_arg(arg,target_type))))
    end

    var parsed_args 
    if (parsed_args := parse_args(cmmd_name,payload,payload_json,arg_list))  == nil
        return
    end

    for arg : parsed_args
        if arg.value == nil
            continue
        end
        log_cmnd_info(cmmd_name,f"setting {arg.name} to {arg.value}")
        zbm_state.setmember(arg.name,arg.value)
    end

    tasmota.resp_cmnd_done()
end

def zbm_schemas(cmnd_name,idx,payload,payload_json)
    ZbmLogger("schmea").info(json.dump(zbm_schema_registry.registry,"format"))
    tasmota.resp_cmnd_done()
end

def zbm_reset_schema(cmnd_name,idx,payload,payload_json)
    zbm_schema_registry.reset()
    ZbmLogger("registry").info("registry has been reset")    
    tasmota.resp_cmnd_done()    
end

def zbm_remove_schema(cmnd_name,idx,payload,payload_json)
    var parsed_args 
    if (parsed_args:= parse_args(cmnd_name,payload,payload_json,["schema_name"])) == nil
        return
    end
    var schema_name = parsed_args[0].value
    if !zbm_schema_registry.remove_schema(schema_name)
        ZbmLogger("registry").info(f"invalid schema name '{schema_name}'")
        return tasmota.resp_cmnd_error()    
    end
    tasmota.resp_cmnd_done()    
end


def zbm_poll_devices(cmnd_name,idx,payload,payload_json)
    zbm_service.update_available_devices()
    ZbmLogger("service").info("devices have been polled")
end

def zbm_reset_config(cmnd_name,idx,payload,payload_json)
    zbm_state.reset()
    ZbmLogger("config").info("config has been reset")    
    tasmota.resp_cmnd_done()    
end

def zbm_reset_service(cmnd_name,idx,payload,payload_json)
    zbm_service.reset()
    ZbmLogger("service").info("service has been reset")    
    tasmota.resp_cmnd_done()    
end

class ZbmSchemaPuller
    # downloads the schemas for a list of device keys, and the schemas they include,
    # from the repository. each step runs from its own timer and makes at most one
    # request, so no single call gets near the time berry code is allowed to run for
    # (USE_BERRY_TIMEOUT, 4s by default) and tasmota keeps servicing mqtt, the web ui
    # and zigbee while the files download. the schemas are always downloaded, even when
    # already in the registry, as the repository may have newer versions. each file
    # replaces the schemas it defines as soon as it has been downloaded, then the added
    # devices using them are configured again so the changes take effect.

    static var url_base = "https://raw.githubusercontent.com/rmawatson/tasmota-zigbee-manager/refs/heads/main/schema"
    static var timer_id = "zbm_pull_schemas"
    static var step_delay = 100
    static var retry_delay = 2000
    static var max_attempts = 3

    var log
    var keys
    var manifests
    var pending
    var requested
    var found
    var downloaded
    var reconfigure
    var step
    var attempts
    var running

    def init(keys)
        self.log = ZbmLogger("pull_schemas")
        self.keys = []
        for key : keys
            if self.keys.find(key) == nil
                self.keys.push(key)
            end
        end
        self.manifests = []
        self.pending = []
        self.requested = {}
        self.found = []
        self.downloaded = []
        self.reconfigure = []
        self.attempts = 0
        self.running = false
    end

    def start()
        self.running = true
        self.log.info(f"pulling schemas for {self.keys}")
        self.next(/-> self.read_index())
    end

    def cancel()
        self.running = false
        tasmota.remove_timer(self.timer_id)
    end

    def next(step)
        self.attempts = 0
        self.schedule(step,self.step_delay)
    end

    def schedule(step,delay)
        self.step = step
        tasmota.set_timer(delay,/-> self.run_step(),self.timer_id)
    end

    def run_step()
        if !self.running
            return
        end
        try
            self.step()
        except "webrequest_error","timeout_error" as e,m
            # failed requests are retried. so is the step if a slow request ran
            # into berry's time limit, it is started over from a new timer
            self.attempts += 1
            if self.attempts < self.max_attempts
                self.log.info(f"{m}, retrying")
                self.schedule(self.step,self.retry_delay * self.attempts)
            else
                self.finish(f"{e}, {m}")
            end
        except .. as e,m
            self.finish(f"{e}, {m}")
        end
    end

    def fetch(file_name)
        var url = f"{self.url_base}/{file_name}"
        self.log.debug(f"requesting {url}")

        var client = webclient()
        var status
        var content
        try
            client.set_follow_redirects(true)
            client.begin(url)
            status = client.GET()
            if status == 200
                content = client.get_string()
            end
        except .. as e,m
            client.close()
            raise "webrequest_error",f"unable to get {file_name} - {e} {m}"
        end
        client.close()

        if status != 200
            # only connection failures and server errors are worth retrying
            var error = (status < 0 || status >= 500) ? "webrequest_error" : "schema_error"
            raise error,f"unable to get {file_name} - http status {status}"
        end

        var data = content != nil ? json.load(content) : nil
        if !isinstance(data,map)
            raise "webrequest_error",f"unable to parse {file_name}"
        end
        return data
    end

    def request(schema_name)
        if !self.requested.contains(schema_name)
            self.requested[schema_name] = true
            self.pending.push(schema_name)
        end
    end

    def read_index()
        var manifests = self.fetch("index.json").find("manifests")
        if !isinstance(manifests,list)
            raise "schema_error","index.json does not list any manifests"
        end
        self.manifests = manifests
        self.next(/-> self.search_manifest())
    end

    # finds the schemas mapped to the keys, one manifest per step
    def search_manifest()
        if !size(self.keys) || !size(self.manifests)
            if size(self.pending)
                self.next(/-> self.download_schema())
            else
                self.finish()
            end
            return
        end

        var manifest = self.fetch(self.manifests[0])
        self.manifests.remove(0)

        var entries = manifest.find("schemas",{})
        for schema_name : entries.keys()
            if !isinstance(entries[schema_name],map)
                continue
            end
            for mapping : entries[schema_name].find("mappings",[])
                var key_index = self.keys.find(mapping)
                if key_index != nil
                    self.keys.remove(key_index)
                    self.found.push(mapping)
                    self.log.info(f"found schema {schema_name} for {mapping}")
                    self.request(schema_name)
                end
            end
        end
        self.next(/-> self.search_manifest())
    end

    # downloads one schema file per step and replaces the schemas it defines in the
    # registry, queueing the schemas it includes
    def download_schema()
        if !size(self.pending)
            for device_info : zbm_service.device_infos.iter()
                if (device_info.status & ZbmDeviceStatus.Added) && self.found.find(device_info.key) != nil
                    self.reconfigure.push(device_info)
                end
            end
            self.next(/-> self.reconfigure_device())
            return
        end

        var schema_name = self.pending[0]
        var schema_json = self.fetch(f"{schema_name}.json")
        zbm_schema_registry.replace_schemas(schema_json)
        self.log.info(f"replaced schema {schema_name}")

        self.pending.remove(0)
        self.downloaded.push(schema_name)
        for schema : schema_json.find("schemas",{})
            for include_name : schema.find("include",[])
                self.request(include_name)
            end
        end
        self.next(/-> self.download_schema())
    end

    # configures one added device per step with its replaced schema, so new or
    # removed entities show up over mqtt without restarting
    def reconfigure_device()
        if !size(self.reconfigure)
            self.finish()
            return
        end
        var device_info = self.reconfigure[0]
        self.reconfigure.remove(0)
        if zbm_service.has_device(device_info.shortaddr) && (device_info.status & ZbmDeviceStatus.Added)
            self.log.info(f"configuring {device_info.name} with the updated schema")
            zbm_service.configure_device(device_info)
        end
        self.next(/-> self.reconfigure_device())
    end

    def finish(error)
        self.cancel()
        if zbm_schema_puller == self
            zbm_schema_puller = nil
        end

        var result = {"Status":error == nil ? "Done" : "Failed"}
        if error == nil
            self.log.info(f"pulled schemas {self.downloaded}")
            result["Schemas"] = self.downloaded
        else
            self.log.error(f"unable to pull schemas - {error}")
            result["Error"] = error
            result["Schemas"] = self.downloaded
        end
        if size(self.keys)
            self.log.info(f"no schema found for {self.keys}")
            result["NotFound"] = self.keys
        end
        tasmota.publish_result(json.dump({"ZbmPullSchemas":result}),"RESULT")
    end
end

def zbm_pull_schmeas(cmnd_name,idx,payload,payload_json)

    if zbm_schema_puller != nil
        return log_cmnd_error(cmnd_name,"a schema pull is already in progress")
    end

    # every device is included, also those already added, as the schemas in the
    # repository may have been updated since they were pulled
    var request_list = []
    for device_info : zbm_service.device_infos.iter()

        if ["",nil].find(device_info.key) != nil && 
            (["",nil].find(device_info.manufacturer) != nil || 
                ["",nil].find(device_info.model) != nil)
            continue
        end

        if device_info.key == nil
            if zbm_state.auto_key_devices
                device_info.key = f"{device_info.manufacturer}:{device_info.model}"
            else
                continue
            end
        end
        request_list.push(device_info.key)
    end

    if !size(request_list)
        log_cmnd_info(cmnd_name,"no devices with a key to pull schemas for")
        return tasmota.resp_cmnd_done()
    end

    # the download continues in the background, the result is published once it is done
    zbm_schema_puller = ZbmSchemaPuller(request_list)
    zbm_schema_puller.start()
    tasmota.resp_cmnd(json.dump({cmnd_name:{"Status":"Started","Keys":zbm_schema_puller.keys}}))
end

class ZbmWebUI
    # a "Zigbee Manager" page, opened from a button on the main page, showing the
    # devices known to the manager with buttons to add, reset, remove and rename them

    static var url = "/zbm"
    static var problem_flags = ZbmDeviceStatus.Unnamed | ZbmDeviceStatus.MappingNotFound | ZbmDeviceStatus.SchemaNotFound |
                               ZbmDeviceStatus.SchemaCompileFailed | ZbmDeviceStatus.NotFound | ZbmDeviceStatus.NoDefaultKey
    static var style = "<style>"
        ".zd{background:var(--c_bg);border-radius:0.3em;margin:5px 0;padding:5px 6px;}"
        ".zh{display:flex;justify-content:space-between;align-items:baseline;gap:8px;}"
        ".zs{font-size:0.85em;text-align:right;}"
        ".zi{width:100%;font-size:0.85em;border-spacing:0;margin-top:3px;}"
        ".zi td{padding:0 2px;vertical-align:top;overflow-wrap:anywhere;}"
        ".zi td:first-child{width:32%;opacity:0.7;}"
        ".zn{font-size:0.8em;margin:4px 0 0;}"
        ".zr{display:flex;gap:5px;margin-top:5px;padding:0;}"
        ".zr input{flex:1;min-width:0;width:auto;}"
        ".zb{flex:1;width:auto;line-height:1.8rem;font-size:0.9rem;padding:0 8px;}"
        ".zr input+.zb{flex:none;}"
        ".zm{border-radius:0.3em;margin:5px 0;color:var(--c_btntxt);text-align:center;}"
        ".zt{display:flex;gap:5px;padding:0;}"
        ".zt button{flex:1;width:auto;}"
        "button:disabled{opacity:0.5;cursor:default;}"
        "</style>"

    var routes_added
    var message

    def init()
        import webserver
        self.routes_added = false
        tasmota.add_driver(self)
        # tasmota only sends web_add_handler once, when the web server starts. if the
        # extension is started later, from the extensions page, the server is already
        # running and the page is added straight away
        if webserver.state() != webserver.HTTP_OFF
            self.web_add_handler()
        end
    end

    def unload()
        import webserver
        tasmota.remove_driver(self)
        if self.routes_added
            self.routes_added = false
            try
                webserver.remove_route(self.url,webserver.HTTP_GET)
                webserver.remove_route(self.url,webserver.HTTP_POST)
            except .. as e,m
                ZbmLogger("web_ui").debug(f"unable to remove the page - {e} {m}")
            end
        end
    end

    def web_add_handler()
        import webserver
        if self.routes_added
            return
        end
        webserver.on(self.url,/-> self.page_devices(),webserver.HTTP_GET)
        webserver.on(self.url,/-> self.page_action(),webserver.HTTP_POST)
        self.routes_added = true
    end

    def web_add_main_button()
        import webserver
        webserver.content_send("<p></p><form id=but_zbm style='display:block;' action='zbm' method='get'><button>Zigbee Manager</button></form>")
    end

    static def text(value)
        import webserver
        if value == nil || value == ""
            return "-"
        end
        return webserver.html_escape(str(value))
    end

    static def device_label(device_info)
        var name = ["",nil].find(device_info.name) == nil ? device_info.name : "unnamed device"
        return f"{name} ({device_info.deviceid})"
    end

    static def last_seen(lastseen)
        if lastseen == nil || lastseen <= 0
            return "never"
        end
        var seconds = tasmota.rtc()["utc"] - lastseen
        if seconds < 0
            return tasmota.strftime("%Y-%m-%d %H:%M:%S",lastseen)
        elif seconds < 60
            return f"{seconds}s ago"
        elif seconds < 3600
            return f"{seconds / 60} min ago"
        elif seconds < 86400
            return f"{seconds / 3600} h ago"
        end
        return f"{seconds / 86400} days ago"
    end

    # devices with a name first, in name order, then the unnamed ones
    static def sorted_devices()
        def sort_key(device_info)
            var name = device_info.name
            return (["",nil].find(name) == nil ? "0" + string.tolower(name) : "1") + device_info.deviceid
        end
        var devices = []
        for device_info : zbm_service.device_infos.iter()
            var key = sort_key(device_info)
            var index = 0
            while index < size(devices) && sort_key(devices[index]) <= key
                index += 1
            end
            devices.insert(index,device_info)
        end
        return devices
    end

    def find_device(deviceid)
        for device_info : zbm_service.device_infos.iter()
            if device_info.deviceid == deviceid
                return device_info
            end
        end
        return nil
    end

    def page_devices()
        import webserver
        if !webserver.check_privileged_access() return nil end

        webserver.content_start("Zigbee Manager")
        webserver.content_send_style()
        webserver.content_send(self.style)
        webserver.content_send("<div style='padding:0 5px;text-align:center;'><h3><hr>Zigbee Manager<hr></h3></div>")

        if self.message != nil
            var background = self.message[1] ? "var(--c_btnsv)" : "var(--c_btnrst)"
            webserver.content_send(f"<div class='zm' style='background:{background};'>{webserver.html_escape(self.message[0])}</div>")
            self.message = nil
        end

        var pull_button = "<button name='act' value='pull'>Pull schemas</button>"
        if zbm_schema_puller != nil
            pull_button = "<button type='button' disabled>Pulling schemas...</button>"
        end
        webserver.content_send("<form method='post' action='zbm' class='zt'><button name='act' value='poll'>Poll devices</button>" + pull_button + "</form><p></p>")

        var devices = self.sorted_devices()
        webserver.content_send(f"<fieldset><legend><b>&nbsp;Devices ({size(devices)})&nbsp;</b></legend>")
        if !size(devices)
            webserver.content_send("<p><small><i>No zigbee devices found yet.</i></small></p>")
        end
        for device_info : devices
            self.send_device(device_info)
        end
        webserver.content_send("</fieldset>")

        webserver.content_button(webserver.BUTTON_MAIN)
        webserver.content_stop()
    end

    def send_device(device_info)
        import webserver
        var status = device_info.status
        var named = ["",nil].find(device_info.name) == nil
        var name = named ? webserver.html_escape(device_info.name) : ""
        var title = named ? name : "<i>unnamed</i>"
        var deviceid = webserver.html_escape(device_info.deviceid)

        var status_text = "Not added"
        var status_style = ""
        if status == ZbmDeviceStatus.Added
            status_text = "Added"
            status_style = " style='color:var(--c_btnsv);'"
        elif status != 0
            status_text = _join(ZbmDeviceStatus.descriptions(status),", ")
            if status & self.problem_flags
                status_style = " style='color:var(--c_txtwrn);'"
            end
        end

        var schema_name = device_info.key != nil ? zbm_schema_registry.mapping(device_info.key) : nil
        var battery = (device_info.battery != nil && device_info.battery >= 0) ? f"{device_info.battery}%" : nil

        webserver.content_send(f"<div class='zd'><div class='zh'><b>{title}</b><span class='zs'{status_style}>{webserver.html_escape(status_text)}</span></div>")
        webserver.content_send("<table class='zi'>")
        for row : [["Device",device_info.deviceid],
                   ["Manufacturer",device_info.manufacturer],
                   ["Model",device_info.model],
                   ["Key",device_info.key],
                   ["Schema",schema_name],
                   ["Link quality",device_info.lqi],
                   ["Battery",battery],
                   ["Last seen",self.last_seen(device_info.lastseen)]]
            webserver.content_send(f"<tr><td>{row[0]}</td><td>{self.text(row[1])}</td></tr>")
        end
        webserver.content_send("</table>")

        var hint
        if !(status & ZbmDeviceStatus.Added)
            if !named
                hint = "Name the device to be able to add it."
            elif status & (ZbmDeviceStatus.MappingNotFound | ZbmDeviceStatus.SchemaNotFound)
                hint = "There is no schema for this device's key. Pull schemas, or add a schema for it, then add the device again."
            elif status & ZbmDeviceStatus.SchemaCompileFailed
                hint = "The schema for this device failed to compile, see the console."
            elif status & ZbmDeviceStatus.NoDefaultKey
                hint = "The device has not reported its manufacturer and model yet, so it has no key."
            elif status & ZbmDeviceStatus.Removed
                hint = "Reset the device to be able to add it again."
            end
        end
        if hint != nil
            webserver.content_send(f"<p class='zn'>{hint}</p>")
        end

        var rename_label = named ? "Rename" : "Set name"
        webserver.content_send(f"<form method='post' action='zbm'><input type='hidden' name='dev' value='{deviceid}'>")
        webserver.content_send(f"<div class='zr'><input name='name' value='{name}' maxlength='32' placeholder='Device name'><button class='zb' name='act' value='rename'>{rename_label}</button></div>")
        webserver.content_send("<div class='zr'>")
        if named && !(status & (ZbmDeviceStatus.Added | ZbmDeviceStatus.Removed))
            webserver.content_send("<button class='zb bgrn' name='act' value='add'>Add</button>")
        end
        if status != 0
            webserver.content_send("<button class='zb' name='act' value='reset'>Reset</button>")
        end
        if !(status & ZbmDeviceStatus.Removed)
            webserver.content_send("<button class='zb bred' name='act' value='remove' onclick='return confirm(\"Remove this device?\")'>Remove</button>")
        end
        webserver.content_send("</div></form></div>")
    end

    def page_action()
        import webserver
        if !webserver.check_privileged_access() return nil end

        try
            self.message = self.run_action(webserver.arg("act"),webserver.arg("dev"),webserver.arg("name"))
        except .. as e,m
            self.message = [f"{e}, {m}",false]
        end
        webserver.redirect(self.url)
    end

    # returns [message, success]
    def run_action(action,deviceid,name)
        if action == "poll"
            zbm_service.update_available_devices()
            return ["Devices polled",true]
        elif action == "pull"
            return self.pull_schemas()
        end

        var device_info = self.find_device(deviceid)
        if device_info == nil
            return [f"Device {deviceid} not found",false]
        end
        var label = self.device_label(device_info)

        if action == "add"
            # adding from the page is a retry, the schema errors left by an earlier attempt
            # are cleared as the registry may have changed since
            device_info.status &= ~(ZbmDeviceStatus.MappingNotFound | ZbmDeviceStatus.SchemaNotFound | ZbmDeviceStatus.SchemaCompileFailed)
            if zbm_service.add_device(device_info) != nil
                return [f"Added {label}",true]
            end
            var reasons = ZbmDeviceStatus.descriptions(device_info.status)
            return [f"Unable to add {label} - {size(reasons) ? _join(reasons, ', ') :: 'see the console'}",false]
        elif action == "reset"
            zbm_service.reset_device(device_info)
            return [f"Reset {label}",true]
        elif action == "remove"
            zbm_service.remove_device(device_info)
            return [f"Removed {label}",true]
        elif action == "rename"
            return self.rename_device(device_info,name)
        end
        return [f"Unknown action '{action}'",false]
    end

    def rename_device(device_info,name)
        name = strip(name != nil ? name : "")
        if !size(name) || size(name) > 32 || !re.match("^[a-zA-Z0-9 ._-]+$",name)
            return ["A name can have up to 32 letters, digits, spaces, '.', '_' or '-'",false]
        end
        var label = self.device_label(device_info)
        if name == device_info.name
            return [f"{label} already has that name",true]
        end

        var was_added = (device_info.status & ZbmDeviceStatus.Added) != 0
        tasmota.cmd(f"ZbName {device_info.deviceid},{name}")
        zbm_service.update_available_devices()
        if device_info.name != name
            return [f"Unable to rename {label}",false]
        end
        if !was_added
            return [f"Renamed {label} to {name}",true]
        end

        # an added device publishes to topics named after it, so it is added again
        # to move them to the new name
        zbm_service.reset_device(device_info)
        if zbm_service.add_device(device_info) == nil
            return [f"Renamed {label} to {name}, but it could not be added again",false]
        end
        return [f"Renamed {label} to {name} and added it again under the new name",true]
    end

    def pull_schemas()
        var response = tasmota.cmd("ZbmPullSchemas")
        var result = isinstance(response,map) ? response.find("ZbmPullSchemas") : nil
        if isinstance(result,map) && result.find("Status") == "Started"
            return ["Pulling schemas, the result is shown in the console",true]
        elif result == "Done"
            return ["No devices with a key to pull schemas for",true]
        end
        return ["Unable to start pulling schemas, see the console",false]
    end
end

class ZbmExtension

    def init()
        zbm_state = ZbmState()
        zbm_mqtt_bridge = ZbmMqttBridge()
        zbm_schema_registry = ZbmSchemaRegistry()
        zbm_service = ZbmService()

        tasmota.add_cmd('ZbmDevice',zbm_device)
        tasmota.add_cmd('ZbmDevices',zbm_devices)
        tasmota.add_cmd('ZbmSchemas',zbm_schemas)
        tasmota.add_cmd('ZbmConfig',zbm_config)
        tasmota.add_cmd('ZbmPollDevices',zbm_poll_devices)
        tasmota.add_cmd('ZbmAddSchema',zbm_add_schema)
        tasmota.add_cmd('ZbmResetSchemas',zbm_reset_schema)
        tasmota.add_cmd('ZbmRemoveSchema',zbm_remove_schema)
        tasmota.add_cmd('ZbmResetConfig',zbm_reset_config)
        tasmota.add_cmd('ZbmResetService',zbm_reset_service)
        tasmota.add_cmd('ZbmAddDevice',zbm_add_device)
        tasmota.add_cmd('ZbmRemoveDevice',zbm_remove_device)
        tasmota.add_cmd('ZbmResetDevice',zbm_reset_device)
        tasmota.add_cmd('ZbmAddMapping',zbm_add_mapping)
        tasmota.add_cmd('ZbmRemovemapping',zbm_remove_mapping)
        tasmota.add_cmd('ZbmPullSchemas',zbm_pull_schmeas)

        try
            zbm_web_ui = ZbmWebUI()
        except .. as e,m
            ZbmLogger("web_ui").error(f"web page not available - {e} {m}")
        end
        print("loaded zbm")
    end

    def unload()

        if zbm_schema_puller != nil
            zbm_schema_puller.cancel()
            zbm_schema_puller = nil
        end
        if zbm_web_ui != nil
            zbm_web_ui.unload()
            zbm_web_ui = nil
        end
        zbm_service.unload()
        zbm_schema_registry.unload()
        zbm_mqtt_bridge.unload()
        zbm_state.unload()
        
        zbm_service = nil
        zbm_schema_registry = nil
        zbm_mqtt_bridge = nil
        zbm_state = nil
        tasmota.remove_cmd('ZbmDevice')
        tasmota.remove_cmd('ZbmDevices')
        tasmota.remove_cmd('ZbmSchemas')
        tasmota.remove_cmd('ZbmConfig')
        tasmota.remove_cmd('ZbmPollDevices')
        tasmota.remove_cmd('ZbmAddSchema')
        tasmota.remove_cmd('ZbmResetSchemas')
        tasmota.remove_cmd('ZbmRemoveSchema')
        tasmota.remove_cmd('ZbmResetConfig')
        tasmota.remove_cmd('ZbmResetService')
        tasmota.remove_cmd('ZbmAddDevice')
        tasmota.remove_cmd('ZbmRemoveDevice')
        tasmota.remove_cmd('ZbmResetDevice')
        tasmota.remove_cmd('ZbmAddMapping')
        tasmota.remove_cmd('ZbmRemovemapping')
        tasmota.remove_cmd('ZbmPullSchemas')
        print("unloaded zbm")
    end
end

return ZbmExtension()
