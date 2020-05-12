local utils = require "pl.utils"
local json = require("cjson.safe")
local http = require("resty.http")

local ngx_shared = ngx.shared
local pairs = pairs
local ngx = ngx
local error = error
local setmetatable = setmetatable
local tonumber = tonumber
local sub = string.sub
local insert = table.insert
local today = ngx.today

local clear_tab
do
    local ok
    ok, clear_tab = pcall(require, "table.clear")
    if not ok then
        clear_tab = function(tab)
            for k in pairs(tab) do
                tab[k] = nil
            end
        end
    end
end

local _M = {_VERSION = "0.0.1"}

local mt = {__index = _M}

-- local cache of counters increments
local increments = {}
-- boolean flags of per worker sync timers
local timer_started = {}
-- sync backend interval
local backend_interval = 10
-- shm cache the data
local gaze = ngx.shared.gaze
if not gaze then
    error('shared dict "gaze" not defined', 2)
end
-- max number of data per single post request
local single_post_max_num = 10

-- local time callback ttl time
local local_time_cb_ttl = 600
-- cache the last callback epoch time
local local_sec_last_cb
-- cache the yesterday data
local local_yesterday_data_cache

-- set the backend post endpoint url
local backend_endpoint = ""
-- set backend auth token
local backend_token = "rCSA4rwmRszJrU5goRfhKvhUDz9n+aSQ"

-- init http request handler
local httpc = http.new()
httpc:set_timeout(500)

-- worker sync_interval
local worker_sync_interval = 60

-- execute the shell script to get the data of yesterday
local function local_time_callback()
    local cmd = [[date -d"1 day ago" +"%Y-%m-%d"]]
    local _, _, str = utils.executeex(cmd)
    if str then
        return sub(str, 1, 10)
    end
end

-- return the format YYYY-MM-DD of yesterday
-- with a LRU cache
-- do a local time callback will make the I/O block due the local shell script call
-- use the LRU cache value with callback ttl

local function get_yesterday(now)
    if not local_sec_last_cb then
        local_sec_last_cb = now
    end
    if not local_yesterday_data_cache then
        local_yesterday_data_cache = local_time_callback()
        local_sec_last_cb = now
        return local_yesterday_data_cache
    end
    if now - local_sec_last_cb > local_time_cb_ttl then
        local_yesterday_data_cache = local_time_callback()
        local_sec_last_cb = now
        return local_yesterday_data_cache
    else
        return local_yesterday_data_cache
    end
end

local function http_send(data)
    local res, err =
        httpc:request_uri(
        backend_endpoint,
        {
            method = "POST",
            body = data,
            headers = {
                ["Content-Type"] = "application/json",
                ["auth"] = backend_token
            },
            keepalive_timeout = 60000,
            keepalive_pool = 10
        }
    )
    if not res then
        ngx.log(ngx.DEBUG, err)
    end
    if not res.status or tonumber(res.status) ~= 200 then
        ngx.log(ngx.DEBUG, (res.status or "code error"))
        return
    else
        return true
    end
end

-- send the data
-- return true for succeed or queue not have the data
-- return nil,when send data to backend failed
local function pop_data(key)
    local len = gaze:llen(key)
    if tonumber(len) == 0 or not len then
        return true
    end

    for i = 1, len do
        local send_tmp = {}
        local val, err = gaze:rpop(key)
        if err then
            ngx.log(ngx.ERR, err)
        end
        if val then
            insert(send_tmp, json.decode(val))
            if #send_tmp >= single_post_max_num then
                local ok = http_send(json.encode(send_tmp))
                if ok then
                    -- clean the send_tmp and do next loop
                    clear_tab(send_tmp)
                else
                    -- rpush back the data and try next round
                    for i = #send_tmp, 1, -1 do
                        gaze:rpush(key, json.encode(send_tmp[i]))
                    end
                    ngx.log(ngx.ERR, "send to backend failed")
                    return nil, "send to backend failed"
                end
            end
        end
    end
    return true
end

-- pop the data from SHM queue and POST to the backend server
local function push_backend()
    -- check yesterday's queue is empty or not exist
    -- if still have the data ,post older data first
    local now = tonumber(ngx.now())

    -- get the yesterday's date as the queue's key
    local yesterday = get_yesterday(now)

    local succ, err = pop_data(yesterday)
    if not succ then
        print("send data to backend failed. key:", yesterday, "err: ", err)
        return
    end

    succ, err = pop_data(now)
    if not succ then
        print("send data to backend failed. key:", now, "err: ", err)
        return
    end
end

-- should run at phase:init_by_worker*
-- start a timer that pop the data in the shm
-- and post this data to the backend server
function _M.sync_backend()
    ngx.timer.every(backend_interval, push_backend)
end


-- left push the every worker's data into shm gaze,a queue hold all data waiting for 
-- poped and send to the backend
local function sync(_, self)
    local date = today()

    local ok,err = gaze:lpush(data,json.encode(self.increments))
    if not ok then
        ngx.log(ngx.ERR,"push worker data to shm failed",err)
        return 
    end

    clear_tab(self.increments)
    return true
end

-- new function for workers
function _M.new()
    local id = ngx.worker.id()

    -- if not ngx_shared[shdict_name] then
    --     error('shared dict "' .. (shdict_name or "nil") .. '" not defined', 2)
    -- end

    if not increments[id] then
        increments[id] = {}
    end

    local self =
        setmetatable(
        {
            dict = gaze,
            increments = increments[id]
        },
        mt
    )

    if not timer_started[id] then
        ngx.log(ngx.DEBUG, "start timer  on worker ", id)
        ngx.timer.every(worker_sync_interval, sync, self)
        timer_started[id] = true
    end

    return self
end


-- should run at other phase,perfer at  phase: log_by_lua*
function _M:incr(key, step)
    step = step or 1
    local v = self.increments[key]
    if v then
        step = step + v
    end

    self.increments[key] = step
    return true
end

return _M
