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

package systemd

import (
	. "github.com/mozilla-services/heka/pipeline"
	pipeline_ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mozilla-services/heka/pipelinemock"
	plugins_ts "github.com/mozilla-services/heka/plugins/testsupport"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"time"
)

func JournalCtlInputSpec(c gs.Context) {
	t := &pipeline_ts.SimpleT{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pConfig := NewPipelineConfig(nil)
	ith := new(plugins_ts.InputTestHelper)
	ith.Msg = pipeline_ts.GetTestMessage()
	ith.Pack = NewPipelinePack(pConfig.InputRecycleChan())

	// Specify localhost, but we're not really going to use the network
	ith.AddrStr = "localhost:55565"
	ith.ResolvedAddrStr = "127.0.0.1:55565"

	// set up mock helper, decoder set, and packSupply channel
	ith.MockHelper = pipelinemock.NewMockPluginHelper(ctrl)
	ith.MockInputRunner = pipelinemock.NewMockInputRunner(ctrl)
	ith.Decoder = pipelinemock.NewMockDecoderRunner(ctrl)
	ith.PackSupply = make(chan *PipelinePack, 1)
	ith.DecodeChan = make(chan *PipelinePack)

	c.Specify("A JournalCtlInput", func() {
		pInput := JournalCtlInput{}

		ith.MockInputRunner.EXPECT().InChan().Return(ith.PackSupply).AnyTimes()
		ith.MockInputRunner.EXPECT().Name().Return("logger").AnyTimes()

		enccall := ith.MockHelper.EXPECT().DecoderRunner("RegexpDecoder", "logger-RegexpDecoder").AnyTimes()
		enccall.Return(ith.Decoder, true)

		mockDecoderRunner := ith.Decoder.(*pipelinemock.MockDecoderRunner)
		mockDecoderRunner.EXPECT().InChan().Return(ith.DecodeChan).AnyTimes()

		config := pInput.ConfigStruct().(*JournalCtlInputConfig)
		if config != nil {
		}

		ith.MockHelper.EXPECT().PipelineConfig().Return(pConfig)
		ith.MockHelper.EXPECT().Hostname().Return(pConfig.Hostname())

		tickChan := make(chan time.Time)
		ith.MockInputRunner.EXPECT().Ticker().Return(tickChan)

		c.Specify("parses a binary journalctl message", func() {
		})
		c.Specify("parses a message exceeding maximum size", func() {
		})
		c.Specify("gracefully recovers from a bad cursor", func() {
		})
		c.Specify("handles bad matches", func() {
		})
		c.Specify("writes correct checkpoint", func() {
		})
		c.Specify("ignores duplicate first message", func() {
		})
		c.Specify("properly discards large message", func() {
		})

	})
}
