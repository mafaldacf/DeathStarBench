local _M = {}
local k8s_suffix = os.getenv("fqdn_suffix")
if (k8s_suffix == nil) then
  k8s_suffix = ""
end

function _M.ReadMovieInfo()
  local bridge_tracer = require "opentracing_bridge_tracer"
  local GenericObjectPool = require "GenericObjectPool"
  local MovieInfoServiceClient = require 'media_service_MovieInfoService'
  local ttypes = require("media_service_ttypes")
  local Cast = ttypes.Cast
  local ngx = ngx
  local cjson = require("cjson")

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  local tracer = bridge_tracer.new_from_global()
  local parent_span_context = tracer:binary_extract(ngx.var.opentracing_binary_context)
  local span = tracer:start_span("ReadMovieInfo", {["references"] = {{"child_of", parent_span_context}}})
  local carrier = {}
  tracer:text_map_inject(span:context(), carrier)

  local args = ngx.req.get_uri_args()
  if (args.movie_id == nil) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  local movie_id = args.movie_id
  
  local client = GenericObjectPool:connection(MovieInfoServiceClient, "movie-info-service" .. k8s_suffix , 9090)
  local movie_info = client:ReadMovieInfo(req_id, movie_id)
  ngx.say(string.format("successfully read movie (movie_id=%s): title=%s, avg_rating=%g, plot_id=%s", movie_id, movie_info.title, movie_info.avg_rating, tostring(movie_info.plot_id)))
  GenericObjectPool:returnConnection(client)

end

return _M
