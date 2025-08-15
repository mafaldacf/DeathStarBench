local _M = {}
local k8s_suffix = os.getenv("fqdn_suffix")
if (k8s_suffix == nil) then
  k8s_suffix = ""
end

function _M.ReadMovieId()
  local bridge_tracer = require "opentracing_bridge_tracer"
  local GenericObjectPool = require "GenericObjectPool"
  local MovieIdServiceClient = require 'media_service_MovieIdService'
  local ttypes = require("media_service_ttypes")
  local Cast = ttypes.Cast
  local ngx = ngx
  local cjson = require("cjson")

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  local tracer = bridge_tracer.new_from_global()
  local parent_span_context = tracer:binary_extract(ngx.var.opentracing_binary_context)
  local span = tracer:start_span("ReadMovieId", {["references"] = {{"child_of", parent_span_context}}})
  local carrier = {}
  tracer:text_map_inject(span:context(), carrier)

  local args = ngx.req.get_uri_args()
  if (args.title == nil) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  local title = args.title
  
  local client = GenericObjectPool:connection(MovieIdServiceClient, "movie-id-service" .. k8s_suffix , 9090)
  local ok, res = pcall(client.ReadMovieId, client, req_id, title)

  if not ok then
    local msg = (res.message or res.msg or require("cjson").encode(res)) or tostring(res)
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("ERROR reading movie-id: " .. msg)
    ngx.log(ngx.ERR, "error reading movie-id: " .. msg)
    GenericObjectPool:returnConnection(client)
    return ngx.exit(ngx.status)
  end

  local movie_id = res
  ngx.say(string.format("successfully read movie (title=%s): movie_id=%s", title, movie_id))
  GenericObjectPool:returnConnection(client)

end

return _M
