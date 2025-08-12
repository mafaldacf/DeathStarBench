local _M = {}
local k8s_suffix = os.getenv("fqdn_suffix")
if (k8s_suffix == nil) then
  k8s_suffix = ""
end

function _M.ReadPage()
  local bridge_tracer = require "opentracing_bridge_tracer"
  local GenericObjectPool = require "GenericObjectPool"
  local PageServiceClient = require 'media_service_PageService'
  local ttypes = require("media_service_ttypes")
  local Cast = ttypes.Cast
  local ngx = ngx
  local cjson = require("cjson")

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  local tracer = bridge_tracer.new_from_global()
  local parent_span_context = tracer:binary_extract(ngx.var.opentracing_binary_context)
  local span = tracer:start_span("ReadPage", {["references"] = {{"child_of", parent_span_context}}})
  local carrier = {}
  tracer:text_map_inject(span:context(), carrier)

  local args = ngx.req.get_uri_args()
  if (args.movie_id == nil or args.review_start == nil or args.review_stop == nil) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  local movie_id = args.movie_id
  local review_start = tonumber(args.review_start)
  local review_stop = tonumber(args.review_stop)

  local client = GenericObjectPool:connection(PageServiceClient, "page-service" .. k8s_suffix , 9090)
  local page = client:ReadPage(req_id, movie_id, review_start, review_stop)

  local cjson = require "cjson.safe"
  cjson.encode_empty_table_as_object(false)

  local mi = page and page.movie_info
  local reviews = page and (page.reviews or (mi and mi.reviews))

  ngx.say(string.format("successfully read page (movie_id=%s):", tostring(movie_id)))

  if type(reviews) == "table" and #reviews > 0 then
    for i, review in ipairs(reviews) do
      ngx.say(string.format("\tReview #%d: text=%s", i, tostring(review.text or "")))
    end
  else
    ngx.say("\t(no reviews)")
  end
  
  GenericObjectPool:returnConnection(client)

end

return _M
