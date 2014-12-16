/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Wesley Dawson (whd@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

// Based on pipeline/stream_parser.go
// See https://wiki.freedesktop.org/www/Software/systemd/export/ for a
// description of this format.

package systemd

import (
	"bytes"
	"encoding/binary"
	"github.com/mozilla-services/heka/message"
	"io"
	"strings"
)

// StreamParser interface to read a split a stream into records
type JournalCtlStreamParser interface {
	// Parses out the next journal key value pair.
	// Returns the number of bytes read from the stream, and the key and value
	// as strings. A value of true for final signifies that the returned key
	// and value strings should be ignored and the current journal entry
	// committed.
	Parse(reader io.Reader) (bytesRead int, key string, value string, final bool, err error)

	// Retrieves the remainder of the parse buffer.  This is the
	// only way to fetch the last record in a stream that specifies a start of
	// line  delimiter or contains a partial last line.  It should only be
	// called when at the EOF and no additional data will be appended to
	// the stream.
	GetRemainingData() []byte

	// Sets the internal buffer to at least 'size' bytes.
	SetMinimumBufferSize(size int)
}

// Internal buffer management for the StreamParser
type journalCtlStreamParserBuffer struct {
	buf       []byte
	readPos   int
	scanPos   int
	needData  bool
	err       string
	parseMode int
}

func newJournalCtlStreamParserBuffer() (s *journalCtlStreamParserBuffer) {
	s = new(journalCtlStreamParserBuffer)
	s.buf = make([]byte, 1024*8)
	s.needData = true
	return
}

func (s *journalCtlStreamParserBuffer) GetRemainingData() (record []byte) {
	if s.readPos-s.scanPos > 0 {
		record = s.buf[s.scanPos:s.readPos]
	}
	s.scanPos = 0
	s.readPos = 0
	return
}

func (s *journalCtlStreamParserBuffer) SetMinimumBufferSize(size int) {
	if cap(s.buf) < size {
		newSlice := make([]byte, size)
		copy(newSlice, s.buf)
		s.buf = newSlice
	}
	return
}

func (s *journalCtlStreamParserBuffer) read(reader io.Reader) (n int, err error) {
	if cap(s.buf)-s.readPos <= 1024*4 {
		if s.scanPos == 0 { // line will not fit in the current buffer
			newSize := cap(s.buf) * 2
			if newSize > message.MAX_RECORD_SIZE {
				if cap(s.buf) == message.MAX_RECORD_SIZE {
					if s.readPos == cap(s.buf) {
						s.scanPos = 0
						s.readPos = 0
						return cap(s.buf), io.ErrShortBuffer
					} else {
						newSize = 0 // don't allocate any more memory, just read into what is left
					}
				} else {
					newSize = message.MAX_RECORD_SIZE
				}
			}
			if newSize > 0 {
				s.SetMinimumBufferSize(newSize)
			}
		} else { // reclaim the space at the beginning of the buffer
			copy(s.buf, s.buf[s.scanPos:s.readPos])
			s.readPos, s.scanPos = s.readPos-s.scanPos, 0
		}
	}
	n, err = reader.Read(s.buf[s.readPos:])
	return
}

// Byte delimited line parser
type JournalCtlParser struct {
	*journalCtlStreamParserBuffer
	delimiter byte
}

func NewJournalCtlParser() (t *JournalCtlParser) {
	t = new(JournalCtlParser)
	t.journalCtlStreamParserBuffer = newJournalCtlStreamParserBuffer()
	t.delimiter = '\n'
	return
}

func (t *JournalCtlParser) Parse(reader io.Reader) (bytesRead int, key string, value string, final bool, err error) {
	var record []byte

	if t.needData {
		if bytesRead, err = t.read(reader); err != nil {
			if err == io.ErrShortBuffer {
				record = t.buf
				// return truncated message and allow input plugin to decide what to do with it
			}
			return
		}
	}
	t.readPos += bytesRead

	bytesRead, record = t.findRecord(t.buf[t.scanPos:t.readPos])

	s := string(record)
	if s == "\n" {
		// the final key value pair for the current journal entry has been read
		final = true
	} else {
		split := strings.SplitN(s, "=", 2)
		if len(split) > 1 {
			// this is a simple KEY=VALUE line
			key, value = split[0], strings.TrimSuffix(split[1], "\n")
		} else {
			// binary value, parse the first 8 bytes of the buffer as a unit64
			// and use that as the length of the payload
			t.scanPos += bytesRead
			key = strings.TrimSuffix(split[0], "\n")
			b := t.buf[t.scanPos : t.scanPos+8]
			length := int64(binary.LittleEndian.Uint64(b))
			t.scanPos += 8
			bytesRead, record = int(length), t.buf[t.scanPos:t.scanPos+int(length)]
			value = string(record)
			t.scanPos += 1 // consume the newline
		}
	}
	t.scanPos += bytesRead
	if len(record) == 0 {
		t.needData = true
	} else {
		if t.readPos == t.scanPos {
			t.readPos = 0
			t.scanPos = 0
			t.needData = true
		} else {
			t.needData = false
		}
	}
	return
}

func (t *JournalCtlParser) findRecord(buf []byte) (bytesRead int, record []byte) {
	n := bytes.IndexByte(buf, t.delimiter)
	if n == -1 {
		return
	}
	bytesRead = n + 1 // include the delimiter for backwards compatibility
	record = buf[:bytesRead]
	return
}
