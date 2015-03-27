-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Input to read entries from the systemd journal

Config:
    matches = "[]"
    embedded JSON array of matches to apply. see
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

local sj = require "systemd.journal"

-- TODO support disjunction
local matches = cjson.decode(read_config("matches") or "[]")

local mod = read_config("process_module")
local decoder = read_config("process_module_entry_point")
if mod and not decoder then
    error("must provide process_module_entry_point for module " .. mod)
elseif mod then
    mod = require(mod)
    decoder = mod[decoder] or error("can't find method " .. " in module " .. mod)
end

local offset_method = read_config("offset_method") or "manual"
local cursor
local checkpoint_file = read_config("offset_file") or error("must specify offset_file")

-- TODO severity, conditional output based on config.
-- To see debug output, add configuration like:
--     [PayloadEncoder]
--     [LogOutput]
--     message_matcher = "Type == 'debug'"
--     encoder = "PayloadEncoder"
local function debug(msg)
    local msg = {
        Timestamp = os.time(),
        Type = "debug",
        Payload = tostring(msg)
    }
    inject_message(msg)
end

local function format_uuid(str)
    s = ""
    for i in string.gmatch(str, "(..)") do
        s = s .. string.char(tonumber(i, 16))
    end
    return s
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
            Timestamp = os.time() * 1e9,
            Fields = j:to_table()
        }
        if use_fields then
            -- Uuid = format_uuid('9d1aaa27d60140bd96365438aad20286', 16),

            -- see
            -- http://www.freedesktop.org/software/systemd/man/systemd.journal-fields.html
            -- for a list of fields

            -- local a, f =  j:enumerate_data()
            -- while a do
            --     if f then
            --         local i = f:find("=")
            --         local key = f:sub(0, i - 1)
            --         local value = f:sub(i + 1)
            --         if key == "MESSAGE" then
            --             msg.Payload = value
            --         elseif key == "_HOSTNAME" then
            --             msg.Hostname = value
            --         elseif key == "_PID" then
            --             msg.Pid = value
            --         elseif key == "_SOURCE_REALTIME_TIMESTAMP" then
            --             msg.Timestamp = tonumber(value) * 1e3
            --         elseif key == "PRIORITY" then
            --             msg.Severity = tonumber(value)
            --         else
            --             msg.Fields[key] = value
            --         end
            --     end
            --     a, f = j:enumerate_data()
            -- end
        end

        if decoder then decoder(msg) else inject_message(msg) end

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
