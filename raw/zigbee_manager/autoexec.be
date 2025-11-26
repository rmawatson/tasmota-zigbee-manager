
do
  import introspect
  var zbm_ext = introspect.module('zbm.be', true)
  tasmota.add_extension(zbm_ext)
end

# to remove:
#       tasmota.unload_extension('Zigbee Manager')
