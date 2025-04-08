package maxcompute_test

import (
	"fmt"
	"iter"
	"log/slog"
	"testing"
	"text/template"

	"github.com/GitRowin/orderedmapjson"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/internal/mocks"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestSourceProcess(t *testing.T) {
	t.Run("return error when query reader is not initialized", func(t *testing.T) {
		// given
		// create mockGetter
		mockGetter := &mocks.Getter{}
		mockGetter.On("Logger").Return(slog.Default())
		// create mc client
		client := &maxcompute.Client{
			QueryReader: func(query string) (maxcompute.RecordReaderCloser, error) {
				return nil, fmt.Errorf("query reader is not initialized")
			},
		}

		// when
		mc := &maxcompute.MaxcomputeSource{
			Client: client,
			Getter: mockGetter,
		}
		err := mc.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "query reader is not initialized")
	})
	t.Run("return error when record reader fail to read record", func(t *testing.T) {
		// given
		// create mockGetter
		mockGetter := &mocks.Getter{}
		mockGetter.On("Logger").Return(slog.Default())
		// create mockPreRecordReaderCloser
		mockPreRecordReaderCloser := mocks.NewRecordReaderCloser(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(nil, fmt.Errorf("error read record"))
		}
		mockPreRecordReaderCloser.On("ReadRecord").Return(seq2)
		// create mc client
		client := &maxcompute.Client{
			QueryReader: func(query string) (maxcompute.RecordReaderCloser, error) {
				return mockPreRecordReaderCloser, nil
			},
		}

		// when
		mc := &maxcompute.MaxcomputeSource{
			Client: client,
			Getter: mockGetter,
		}
		err := mc.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error read record")
	})
	t.Run("return error when query compilation is failing", func(t *testing.T) {
		// given
		// create mockGetter
		mockGetter := &mocks.Getter{}
		mockGetter.On("Logger").Return(slog.Default())
		// create mockRecordReaderCloser
		preRecord := model.NewRecord()
		preRecord.Set("field", "\x07") // \a character
		mockPreRecordReaderCloser := mocks.NewRecordReaderCloser(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(preRecord, nil)
		}
		mockPreRecordReaderCloser.On("ReadRecord").Return(seq2)
		// create mockRecordHelper
		preRecordWithMetadata := model.NewRecord()
		preRecordWithMetadata.Set("__METADATA__field", "\x07") // \a character
		mockRecordHelper := &mocks.RecordHelper{}
		mockRecordHelper.On("RecordWithMetadata", preRecord).Return(preRecordWithMetadata)
		// create mc client
		client := &maxcompute.Client{
			QueryReader: func(query string) (maxcompute.RecordReaderCloser, error) {
				return mockPreRecordReaderCloser, nil
			},
		}

		// when
		mc := &maxcompute.MaxcomputeSource{
			Client:        client,
			Getter:        mockGetter,
			RecordHelper:  mockRecordHelper,
			PreQuery:      "select * from table_pre",
			QueryTemplate: template.New("empty"),
		}
		err := mc.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "incomplete or empty template")
	})
	t.Run("return success", func(t *testing.T) {
		// given
		// create mockGetter
		mockGetter := &mocks.Getter{}
		mockGetter.On("Logger").Return(slog.Default())
		// create mockRecordReaderCloser
		preRecord := model.NewRecord()
		preRecord.Set("field", "value")
		mockPreRecordReaderCloser := mocks.NewRecordReaderCloser(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(preRecord, nil)
		}
		mockPreRecordReaderCloser.On("ReadRecord").Return(seq2)
		record := model.NewRecord()
		record.Set("name", "hello")
		mockRecordReaderCloser := mocks.NewRecordReaderCloser(t)
		var seq2Record iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2Record = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(record, nil)
		}
		mockRecordReaderCloser.On("ReadRecord").Return(seq2Record)
		// create mockRecordHelper
		preRecordWithMetadata := model.NewRecord()
		preRecordWithMetadata.Set("__METADATA__field", "value")
		mockRecordHelper := &mocks.RecordHelper{}
		mockRecordHelper.On("RecordWithMetadata", preRecord).Return(preRecordWithMetadata)
		// create mockRecordSender
		recordToBeSend := model.NewRecord()
		recordToBeSend.Set("name", "hello")
		recordToBeSend.Set("__METADATA__field", "value")
		mockRecordSender := &mocks.RecordSender{}
		mockRecordSender.On("SendRecord", recordToBeSend).Return(nil)
		// create mc client
		client := &maxcompute.Client{
			QueryReader: func(query string) (maxcompute.RecordReaderCloser, error) {
				if query == "select * from table" {
					return mockRecordReaderCloser, nil
				}
				return mockPreRecordReaderCloser, nil
			},
		}

		// when
		mc := &maxcompute.MaxcomputeSource{
			Client:        client,
			Getter:        mockGetter,
			RecordSender:  mockRecordSender,
			RecordHelper:  mockRecordHelper,
			PreQuery:      "select * from table_pre",
			QueryTemplate: template.Must(template.New("empty").Parse("select * from table")),
		}
		err := mc.Process()

		// then
		assert.NoError(t, err)
	})
}
