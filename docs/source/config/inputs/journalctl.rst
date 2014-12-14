JournalCtlInput
===============

Read from the systemd journal via the journalctl command.

A note on implementation:

In order to avoid writing a custom stream parser for the systemd
`export format <http://www.freedesktop.org/wiki/Software/systemd/export/>`_
this implementation uses systemd's
`JSON format <http://www.freedesktop.org/wiki/Software/systemd/json/>`_.
This is somewhat inefficient and an export format parser would be preferred,
but by using JSON with TokenParser the implementation is greatly simplified.

Config:

- bin (string):
    Path to the journalctl binary; defaults to "journalctl".
- decoder (string):
    Name of the decoder instance to send messages to. If omitted messages will
    be injected directly into Heka's message router.
- matches ([]string):
    Matches to filter on, see man JOURNALCTL(1)
- retries (RetryOptions, optional):
    A sub-section that specifies the settings to be used for restart behavior.
    See :ref:`configuring_restarting`. In the current implementation, it is
    recommended to set max_retries to something other than -1, since the plugin
    attempts to recover from some errors gracefully (bad cursor, process
    failures), but considers other errors as unrecoverable (bad matches).

Example:

.. code-block:: ini

    [JournalCtlInput]
    matches = ["_SYSTEMD_UNIT=fxa-auth-server.service"]

        [JournalCtlInput.Retries]
        max_retries = 10
