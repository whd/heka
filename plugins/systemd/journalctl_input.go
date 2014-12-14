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
#***** END LICENSE BLOCK *****/

// FIXME atomic.AddInt64(&pi.processMessageFailures, 1)
package systemd

import (
	"bufio"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type JournalCtlInputConfig struct {
	// Path of journalctl executable
	Bin string `toml:"bin"`

	// matches (see man JOURNALCTL(1))
	Matches []string `toml:"matches"`

	// Name of configured decoder instance.
	Decoder string
}

// Heka Input plugin that runs external programs and processes their
// output as a stream into Message objects to be passed into
// the Router for delivery to matching Filter or Output plugins.
type JournalCtlInput struct {
	processMessageCount    int64
	processMessageFailures int64

	ProcessName string
	ir          InputRunner
	decoderName string
	cmd         *exec.Cmd

	stdoutChan chan string
	stderrChan chan string
	stdout     *io.PipeReader
	stderr     *io.PipeReader

	stopChan chan bool
	parser   StreamParser

	pConfig *PipelineConfig

	hostname           string
	heka_pid           int32
	tickInterval       uint
	checkpointFile     *os.File
	checkpointFilename string

	cursor string

	// workaround for a possible duplicate first message with --after-cursor, see
	// http://cgit.freedesktop.org/systemd/systemd/commit/?id=8ee8e53648bf45854d92b60e1e70c17a0cec3c3d
	// for the upstream fix
	first bool

	// internal state tracking that persists across restart attempts
	drop_cursor bool
	bad_matches bool

	once sync.Once
}

// ConfigStruct implements the HasConfigStruct interface and sets
// defaults.
func (pi *JournalCtlInput) ConfigStruct() interface{} {
	return &JournalCtlInputConfig{
		Bin:     "journalctl",
		Matches: []string{},
	}
}

// utilities for writing checkpoint
func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

func (pi *JournalCtlInput) writeCheckpoint(cursor string) (err error) {
	if pi.checkpointFile == nil {
		if pi.checkpointFile, err = os.OpenFile(pi.checkpointFilename,
			os.O_WRONLY|os.O_SYNC|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			return
		}
	}
	pi.checkpointFile.Seek(0, 0)
	pi.checkpointFile.Truncate(0)
	_, err = pi.checkpointFile.WriteString(cursor)
	return
}

func readCheckpoint(filename string) (cursor string, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}

	b, err := ioutil.ReadAll(file)
	if err != nil {
		return
	}
	cursor = string(b)
	return
}

func (pi *JournalCtlInput) SetPipelineConfig(pConfig *PipelineConfig) {
	pi.pConfig = pConfig
}

// Init implements the Plugin interface.
func (pi *JournalCtlInput) Init(config interface{}) (err error) {
	conf := config.(*JournalCtlInputConfig)
	drop := pi.drop_cursor
	if pi.bad_matches {
		return fmt.Errorf("bad match in %s", conf.Matches)
	}

	pi.stdoutChan = make(chan string)
	pi.stderrChan = make(chan string)
	pi.stopChan = make(chan bool)
	pi.first = true
	pi.drop_cursor = false
	pi.bad_matches = false

	pi.checkpointFilename = pi.pConfig.Globals.PrependBaseDir(filepath.Join("journalctl",
		fmt.Sprintf("%s.cursor", pi.ProcessName)))

	if fileExists(pi.checkpointFilename) {
		if pi.cursor, err = readCheckpoint(pi.checkpointFilename); err != nil {
			return fmt.Errorf("readCheckpoint %s", err)
		}
	} else {
		if err = os.MkdirAll(filepath.Dir(pi.checkpointFilename), 0766); err != nil {
			return
		}
	}

	if drop {
		pi.ir.LogMessage(fmt.Sprintf("dropping bad cursor \"%s\"", pi.cursor))
		pi.cursor = ""
	}

	args := []string{"-o", "json", "--no-pager", "--all", "--follow"}
	if pi.cursor != "" {
		args = append(args, []string{"--after-cursor", pi.cursor}...)
	}

	args = append(args, conf.Matches...)
	pi.cmd = exec.Command(conf.Bin, args...)

	pi.stdout, pi.cmd.Stdout = io.Pipe()
	pi.stderr, pi.cmd.Stderr = io.Pipe()

	pi.decoderName = conf.Decoder
	tp := NewTokenParser()
	pi.parser = tp

	pi.heka_pid = int32(os.Getpid())

	return nil
}

func (pi *JournalCtlInput) SetName(name string) {
	pi.ProcessName = name
}

func (pi *JournalCtlInput) Run(ir InputRunner, h PluginHelper) error {
	// So we can access our InputRunner outside of the Run function.
	pi.ir = ir
	pi.hostname = h.Hostname()
	pConfig := h.PipelineConfig()

	var (
		pack    *PipelinePack
		dRunner DecoderRunner
		data    string
	)
	ok := true

	// Try to get the configured decoder.
	hasDecoder := pi.decoderName != ""
	if hasDecoder {
		decoderFullName := fmt.Sprintf("%s-%s", ir.Name(), pi.decoderName)
		if dRunner, ok = h.DecoderRunner(pi.decoderName, decoderFullName); !ok {
			return fmt.Errorf("Decoder not found: %s", pi.decoderName)
		}
	}

	// Start the output parser and start running commands.
	go pi.RunCmd()

	packSupply := ir.InChan()
	// Wait for and route populated PipelinePacks.
	for ok {
		select {
		case data, ok = <-pi.stdoutChan:
			if !ok {
				break
			}
			atomic.AddInt64(&pi.processMessageCount, 1)
			pack = <-packSupply
			cursor := pi.writeToPack(data, pack, "stdout")

			if pi.first && pi.cursor == cursor {
				pi.ir.LogMessage(fmt.Sprintf("ignoring duplicate first message at cursor %s", cursor))
				pi.first = false
				continue
			}

			if hasDecoder {
				dRunner.InChan() <- pack
			} else {
				pConfig.Router().InChan() <- pack
			}

			if err := pi.writeCheckpoint(cursor); err != nil {
				return err
			}

		case data = <-pi.stderrChan:
			pi.ir.LogError(fmt.Errorf("%s", data))
			// Try to do some journalctl-specific cleanup.
			if strings.HasPrefix(data, "Failed to seek to cursor") {
				// If the cursor is bad, unset it so that a restart does not try to
				// use the same cursor. This behavior resembles LogstreamerInput's
				// when presented with an invalid checkpoint.
				pi.drop_cursor = true
			} else if strings.HasPrefix(data, "Failed to add match") {
				// FIXME if matches are bad, journalctl will probably never
				// succeed and this should be a fatal condition. Having the
				// notion of both recoverable and unrecoverable errors doesn't
				// map into heka's restarting interface very well. However, as
				// long as max_retries is bounded, this fatal condition will
				// eventually cause heka to stop, whereas a bad cursor or other
				// arbitrary process failures should be recoverable from, so
				// the simplest solution would be to set max_retries
				// appropriately, but I'm not sure how to do this from within a
				// plugin.

				// Another option would be to "soft-fail" for the cursor case
				// and not implement the restarting interface. It's possible to
				// track internally whether the journalctl failure was due to a
				// bad cursor and perform the process restart within the plugin
				// logic. In this case there are a class of errors which heka
				// should be able to recover from (e.g. process is accidentally
				// killed), but will not.
				pi.bad_matches = true
			}

		case <-pi.stopChan:
			ok = false
		}
	}

	return nil
}

func (pi *JournalCtlInput) writeToPack(data string, pack *PipelinePack, stream_name string) (cursor string) {
	pack.Message.SetUuid(uuid.NewRandom())
	pack.Message.SetTimestamp(time.Now().UnixNano())
	pack.Message.SetType("JournalCtlInput")
	pack.Message.SetPid(pi.heka_pid)
	pack.Message.SetHostname(pi.hostname)
	pack.Message.SetLogger(pi.ir.Name())

	var j interface{}
	err := json.Unmarshal([]byte(data), &j)
	if err != nil {
		pi.ir.LogError(err)
		return
	}
	m := j.(map[string]interface{})

	for k, v := range m {
		switch vv := v.(type) {
		case string:
			if k == "__CURSOR" {
				cursor = vv
			} else if k == "MESSAGE" {
				pack.Message.SetPayload(vv)
			} else {
				fPInputName, err := message.NewField(k, vv, "")
				if err == nil {
					pack.Message.AddField(fPInputName)
				} else {
					pi.ir.LogError(err)
				}
			}
		case []interface{}:
			field := message.NewFieldInit(k, message.Field_STRING, "")
			for _, u := range vv {
				switch v2 := u.(type) {
				case string:
					field.AddValue(v2)
				default:
					pi.ir.LogMessage(fmt.Sprintf("ignoring field in %s: %v (not a string)", k, v2))
				}
			}
			pack.Message.AddField(field)
		default:
			fmt.Println(k, "is of a type I don't know how to handle", vv)
		}
	}

	return
}

func (pi *JournalCtlInput) Stop() {
	// This will shutdown the JournalCtlInput::RunCmd goroutine
	pi.once.Do(func() {
		close(pi.stopChan)
	})
}

func (pi *JournalCtlInput) RunCmd() {
	var err error

	if err = pi.cmd.Start(); err != nil {
		pi.ir.LogError(fmt.Errorf("%s Start() error: [%s]",
			pi.ProcessName, err.Error()))
	}

	go pi.ParseOutput(pi.stdout, pi.stdoutChan)
	go pi.ParseErrorOutput(pi.stderr, pi.stderrChan)

	err = pi.cmd.Wait()
	if err != nil {
		pi.ir.LogError(fmt.Errorf("%s Wait() error: [%s]",
			pi.ProcessName, err.Error()))
	}
	close(pi.stdoutChan)
	close(pi.stderrChan)
}

func (pi *JournalCtlInput) ParseErrorOutput(r io.Reader, outputChannel chan string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		outputChannel <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		pi.ir.LogError(fmt.Errorf("Stream Error [%s]", err.Error()))
	}
}

func (pi *JournalCtlInput) ParseOutput(r io.Reader, outputChannel chan string) {
	var (
		record []byte
		err    error
	)

	for err == nil {
		// Use configured StreamParser to split output from commands.
		_, record, err = pi.parser.Parse(r)
		if err != nil {
			if err == io.EOF {
				record = pi.parser.GetRemainingData()
			} else if err == io.ErrShortBuffer {
				pi.ir.LogError(fmt.Errorf("record exceeded MAX_RECORD_SIZE %d",
					message.MAX_RECORD_SIZE))
				err = nil // non-fatal, keep going
			}
		}

		if len(record) > 0 {
			// Setup and send the Message
			outputChannel <- string(record)
		}

		if err != nil {
			// Go doesn't seem to have a good solution to streaming output
			// between subprocesses.  It seems like you have to read *all* the
			// content in a goroutine instead of just streaming the content.
			//
			// See: http://code.google.com/p/go/issues/detail?id=2266
			// and http://golang.org/pkg/os/exec/#Cmd.StdoutPipe
			if !strings.Contains(err.Error(), "read |0: bad file descriptor") &&
				(err != io.EOF) {
				pi.ir.LogError(fmt.Errorf("Stream Error [%s]", err.Error()))
			}
		}
	}
}

func (pi *JournalCtlInput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&pi.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&pi.processMessageFailures), "count")
	return nil
}

// CleanupForRestart implements the Restarting interface.
func (pi *JournalCtlInput) CleanupForRestart() {
	pi.Stop()
}

func init() {
	RegisterPlugin("JournalCtlInput", func() interface{} {
		return new(JournalCtlInput)
	})
}
