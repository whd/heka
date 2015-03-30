-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Input to read entries from the systemd journal

Config:
    matches = "[]"
    embedded JSON array of matches to apply. See
    http://www.freedesktop.org/software/systemd/man/journalctl.html for a
    description of matches. By default, all journal entries that the user
    running Heka has access to are matched.

    use_fields = true
    whether to map systemd fields to top-level Heka fields
    e.g. _HOSTNAME -> Fields[Hostname] etc.

    process_module = nil
    module to load (if any) to further transform messages before injecting
    (similar to a decoder)

    process_module_entry_point = nil
    method to call handing off the current table for further decoding. It is
    expected that this method call inject_message()

    offset_method = "manual"
    The method used to determine at which offset to begin consuming messages.
    The valid values are:

    - *manual* Heka will track the offset and resume from where it last left off (default).
    - *newest* Heka will start reading from the most recent available offset.
    - *oldest* Heka will start reading from the oldest available offset.

    offset_file = nil (required)
    File to store the checkpoint in. Must be unique. Currently the sandbox API
    does not access to Logger information so this file must be specified
    explicitly.

*Example Heka Configuration*

.. code-block:: ini

    [SystemdInput]
    type = "SandboxInput"
    filename = "lua_inputs/systemd.lua"

        [SystemdInput.config]
        matches = '["_SYSTEMD_UNIT=fxa-auth.service"]'

--]]

require "io"
require "os"
require "string"
require "cjson"
local d = require "debug"
local debug = d.debug

local sj = require "systemd.journal"

-- TODO support disjunction
local matches = cjson.decode(read_config("matches") or "[]")

local offset_method = read_config("offset_method") or "manual"
local cursor
local checkpoint_file = read_config("offset_file") or error("must specify offset_file")

-- This is a cursory attempt at making a more "heka-ish"
-- message. process_module_entry_point can be used to for arbitrary mappings
-- and transformations.
local fields_map = {
    MESSAGE = "Payload"
    , MESSAGE_ID = {
        "Uuid",
        function(x) format_uuid(x) end
    }
    , _HOSTNAME = "Hostname"
    , _SOURCE_REALTIME_TIMESTAMP = {
        "Timestamp",
        function (i) return tonumber(i) * 1e3 end
    }
    , _PID = "PID"
    , PRIORITY = "Severity"
    , _SYSTEMD_UNIT = "Logger"
    , _SYSTEMD_USER_UNIT = "Logger"
    , SYSLOG_IDENTIFIER = "Logger"
}

local function format_uuid(str)
    s = ""
    for i in string.gmatch(str, "(..)") do
        s = s .. string.char(tonumber(i, 16))
    end
    return s
end

-- see
-- http://www.freedesktop.org/software/systemd/man/systemd.journal-fields.html
-- for a list of fields. Currently "any-wins" semantics when multiple journal
-- fields map to a single heka one since tables are traversed in arbitrary
-- order.
local function map_fields(tbl)
    for key, value in pairs(fields_map) do
        local newkey, method
        if type(value) == "table" then
            newkey, method = unpack(value)
        else
            newkey = value
        end

        local v = tbl.Fields[key]
        if v then
            if method then
                tbl[newkey] = method(v)
            else
                tbl[newkey] = v
            end
            tbl.Fields[key] = nil
        end
    end
    inject_message(tbl)
end

local mod = read_config("process_module")
local decoder = read_config("process_module_entry_point")
if mod and not decoder then
    error("must provide process_module_entry_point for module " .. mod)
elseif mod then
    mod = require(mod)
    decoder = mod[decoder] or error("can't find method " .. " in module " .. mod)
elseif read_config("use_fields") then
    decoder = map_fields
end

function process_message()
    local j = assert(sj.open())
    j:set_data_threshold(0)

    for i, match in ipairs(matches) do
        debug("adding match " .. match)
        assert(j:add_match(match))
    end

    local fh = io.open(checkpoint_file, "r")
    if fh then
        -- the systemd cursor is variable length, so we write out the cursor
        -- with a newline to avoid having to truncate the checkpoint file
        cursor = fh:read("*line")
        debug("cursor: " .. tostring(cursor))
        fh:close()
        fh = assert(io.open(checkpoint_file, "r+"))
    else
        fh = assert(io.open(checkpoint_file, "w+"))
    end
    fh:setvbuf("no")
    if cursor then
        if not j:seek_cursor(cursor) then
            error("failed to seek to cursor" .. cursor)
        end
        -- Note that [seek_cursor] does not actually make any entry the new
        -- current entry, this needs to be done in a separate step with a
        -- subsequent sd_journal_next(3) invocation (or a similar call)
        j:next()

        -- maybe issue a warning instead if this fails
        assert(j:test_cursor(cursor))
    else
        j:seek_tail()
        j:next()
    end

    local ready = j:next()

    while true do
        debug("main loop iteration")
        while not ready do
            if j:wait(1) ~= sj.WAKEUP.NOP then
                ready = j:next()
            end
        end

        local cursor = j:get_cursor()

        local msg = {
            Type = "heka.systemd",
            Timestamp = os.time() * 1e9,
            Fields = j:to_table()
        }
        if decoder then
            local r, err = decoder(msg)
            if r ~= 0 then
                debug(read_config("process_module_entry_point") .. " failed: " .. err)
            end
        else
            inject_message(msg)
        end

        local offset, err = fh:seek("set")
        if err then error(err) end
        assert(offset == 0)
        fh:write(cursor .. "\n")
        fh:flush()

        ready = j:next()
    end
    fh:close()
    return 0
end
