-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- TODO support severity, info/warn/critical etc.
-- To see debug output, add configuration like:
--     [PayloadEncoder]
--     [LogOutput]
--     message_matcher = "Type == 'heka.debug'"
--     encoder = "PayloadEncoder"

--[[
API
^^^
**debug(payload, type, ns)**

    Emit a debug message.

    *Arguments*
        - payload (any type that supports tostring)
            Value to use in Payload field, converted with tostring. Tables are
            specially printed by walking the table and printing its key/value
            pairs (or for arrays, simple the values).
        - type (string or nil)
            String to use in the `Type` field. Defaults to "heka.debug".
        - ns (number or nil)
            Nanosecond timestamp to use for any strftime field interpolation
            into the above fields. Current system time will be used if nil.

--]]

local os = require "os"
local tostring = tostring
local inject_message = inject_message

-- FIXME mention that this may conflict with filter-specific configuration.
-- And also that it overrides the lua debug library (not available from the
-- sandbox anyway).
local dbg = read_config("debug")

local M = {}
setfenv(1, M) -- Remove external access to contain everything in the module.

--[[ Public Interface --]]

function debug(msg, typ, ns)
    if dbg then
        local msg = {
            Timestamp = ns or os.time(),
            Type = typ or "heka.debug",
            Payload = tostring(msg)
        }
        inject_message(msg)
    end
end

return M
