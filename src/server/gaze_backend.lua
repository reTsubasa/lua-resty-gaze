local redis = require("resty.redis")
local json = require("cjson.safe")

local method = ngx.req.get_method
local uri_args = ngx.req.get_uri_args
local get_headers = ngx.req.get_headers
local get_body_data = ngx.req.get_body_data
local exit = ngx.exit
local say = ngx.say
local date = ngx.today
local len = string.len
local fmt = string.format
local find = string.find
local sub = string.sub
local log = ngx.log
local ERR = ngx.ERR
local DEBUG  = ngx.DEBUG

-- key prefix of tmp data in redis list
local tmp_key_prefix = "_tmp"

-- per_defined tokens that valid to send the request to this api
local tokens = {"rCSA4rwmRszJrU5goRfhKvhUDz9n+aSQ"}

-- SETTINGS OF DATA HOUSEKEEPER
-- interval of data housekeeper timer 
local hk_interval = 5
local hk_max_loop_num = 100

local _M = {_VERSION = "0.0.1"}

local mt = {__index = _M}

-- module layer cache for redis connect handler
local red 
-- init the redis connnect handler
local function redis_hdl()
    if not red then
        red = redis:new()
        red:set_timeouts(1000, 1000, 1000)
        local ok, err = red:connect("127.0.0.1", 6379)
        if not ok then
            ngx.log(ngx.ERR,"redis connect failed",err)
            return
        end
    end
    return red
end


local function gen_cache_key(opts)
    if opts.tmp then
        return fmt("%s_%s", data(),tmp_key_prefix)
    end

    return date()
end


-- a simple header token valid function
local function valid_token(token)
    for _, k in ipairs(tokens) do
        if k == token then
            return true
        end
    end
    return nil
end


local function pop_data(key)
    local red = redis_hdl()
    if not red then
        return nil
    end
    local data = red:rpop(key)
    if data then
        return json.decode(data)
    end
end


local function summation(total,once)
    local records = total.data or {}
    if not once or type(once) ~= "table" then
        return
    end

    for k, v in pairs(once) do
        if records[k] then
            records[k] = records[k] + v
        else
            records[k] = v
        end
    end
end



-- timer main function
local function housekeeper(_,)
    local tmp_key = gen_cache_key({tmp = true})

    -- get redis handler
    local red = redis_hdl()
    if not red then
        log(ERR,"get redis connection handler failed")
    end

    -- fetch today's data
    local sum_data = {}
    local sum_key  = gen_cache_key()
    local value,err = red:get(sum_key)
    if err then
        log(ERR,"get key from redis error: ",err)
        return
    end

    sum_data = json.decode(value)

    -- loop the queue if get the data
    local loop_cnt = 1
    local once_data = pop_data(tmp_key)
    while (loop_cnt <= hk_max_loop_num) and once_data  do
        summation(sum_data,once_data)
        once_data = pop_data(tmp_key)
        loop_cnt = loop_cnt + 1
    end

    -- set back new sum_data to the redis
    local ok,err = red:set(sum_key,cjson.encode(sum_data))
    if not ok then
        log(ERR,"set back to redis failed: ",err)
    end


end


-- a timer that comsume the tmp data ,and calu the sum value
-- last set back to the db
function _M.timer_incr()
    ngx.timer.every(hk_interval, housekeeper)
end

-- a API endpoint.That receive the data and insert into the db
-- if succ then return http code,else,return other codes
function _M.receiver()
    ngx.req.read_body()
    if method ~= "POST" then
        return exit(406)
    end

    -- header token check
    local ok = valid_token(get_headers["auth"])
    if not ok then
        return exit(405)
    end

    -- get body data
    local data = get_body_data()
    if not data or len(data) == 0 then
        return say("ok")
    end

    -- lpush into redis
    local key = gen_cache_key({tmp = true})
    local red = redis_hdl()
    local ok ,err = red:lpush(key,data)
    if not ok then
        return ngx.exit(502)
    end
    return say("ok")
end


-- a API,return the data after the housekeeper worked
function _M.get_quest()
    local input_date = uri_args["date"]
    if input_date then
        if len(input_date) ~= 8 then
            return say("日期格式不正确，仅支持?date='YYYYMMDD'类型")
        end
        input_date = fmt("%s-%s-%s", sub(input_date,1,4),sub(input_date,5,6),sub(input_date,7,8))
    else
        input_date = date()
    end

    local red = redis_hdl()
    if not red then
        return exit(502)
    end
    local res,err = red:get(input_date)
    if err then
        return say("获取后端数据错误",err)
    end
    return res

end
return _M
