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

  ngx.req.read_body()
  local data = ngx.req.get_body_data()

  if not data then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Empty body")
    ngx.log(ngx.ERR, "Empty body")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  local args = ngx.req.get_post_args()
  local movie_id = args.movie_id
  local review_start = tonumber(args.review_start)
  local review_stop = tonumber(args.review_stop)

  local client = GenericObjectPool:connection(PageServiceClient, "page-service" .. k8s_suffix , 9090)
  local page = client:ReadPage(req_id, movie_id, review_start, review_stop)
  
  ngx.say("successfully read page (movie_id=" .. tostring(movie_id) .. "): "
  .. "movie_id=" .. tostring(page.movie_info.movie_id)
  .. ", title=" .. tostring(page.movie_info.title)
  .. ", reviews:\n")

  if page.movie_info.reviews then
    for i, review in ipairs(page.movie_info.reviews) do
      ngx.say(string.format(
        "  Review #%d: review_id=%s, user_id=%s, rating=%d, text=%s, timestamp=%s",
        i,
        tostring(review.review_id),
        tostring(review.user_id),
        tonumber(review.rating) or 0,
        tostring(review.text or ""),
        tostring(review.timestamp)
      ))
    end
  else
    ngx.say("  (no reviews)")
  end
  
  GenericObjectPool:returnConnection(client)

end

return _M
