package file_test

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"testing"
	"text/template"

	"github.com/GitRowin/orderedmapjson"
	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/internal/compiler"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/mocks"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSinkProcess(t *testing.T) {
	t.Run("return error if read record returns error", func(t *testing.T) {
		// given
		// create mockRecordReader
		mockRecordReader := mocks.NewRecordReader(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(nil, fmt.Errorf("error"))
		}
		mockRecordReader.On("ReadRecord").Return(seq2)

		// when
		fileSink := &file.FileSink{
			// RecordReader: mockRecordReader,
		}
		err := fileSink.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error")
	})
	t.Run("return error if compile destination uri template is fail", func(t *testing.T) {
		// given
		// create mockRecordReader
		mockRecordReader := mocks.NewRecordReader(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(model.NewRecord(), nil)
		}
		mockRecordReader.On("ReadRecord").Return(seq2)

		// when
		fileSink := &file.FileSink{
			DestinationURITemplate: template.New("empty"),
			// RecordReader:           mockRecordReader,
		}
		err := fileSink.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "incomplete or empty template")
	})
	t.Run("return error if compiled destination uri is incorrect", func(t *testing.T) {
		// given
		// create mockRecordReader
		mockRecordReader := mocks.NewRecordReader(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			record := model.NewRecord()
			record.Set("field", "\x07") // \a character
			yield(record, nil)
		}
		mockRecordReader.On("ReadRecord").Return(seq2)
		// create mockGetter
		mockGetter := mocks.NewGetter(t)
		mockGetter.On("Logger").Return(slog.Default())
		// create empty template
		tmpl := template.Must(compiler.NewTemplate("empty string", "[[ .field ]]"))

		// when
		fileSink := &file.FileSink{
			DestinationURITemplate: tmpl,
			// RecordReader:           mockRecordReader,
			// Getter:                 mockGetter,
		}
		err := fileSink.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "invalid control character in URL")
	})
	t.Run("return error if compiled destination uri schema is not file://", func(t *testing.T) {
		// given
		// create mockRecordReader
		mockRecordReader := mocks.NewRecordReader(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			record := model.NewRecord()
			record.Set("field", "value")
			yield(record, nil)
		}
		mockRecordReader.On("ReadRecord").Return(seq2)
		// create mockGetter
		mockGetter := mocks.NewGetter(t)
		mockGetter.On("Logger").Return(slog.Default())
		// create empty template
		tmpl := template.Must(compiler.NewTemplate("empty string", "another://[[ .field ]]"))

		// when
		fileSink := &file.FileSink{
			DestinationURITemplate: tmpl,
			// RecordReader:           mockRecordReader,
			// Getter:                 mockGetter,
		}
		err := fileSink.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "invalid scheme: another")
	})
	t.Run("return error if writerFactory fail", func(t *testing.T) {
		// given
		// create mockRecordReader
		mockRecordReader := mocks.NewRecordReader(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			record := model.NewRecord()
			record.Set("field", "value")
			yield(record, nil)
		}
		mockRecordReader.On("ReadRecord").Return(seq2)
		// create mockGetter
		mockGetter := mocks.NewGetter(t)
		mockGetter.On("Logger").Return(slog.Default())
		// create empty template
		tmpl := template.Must(compiler.NewTemplate("empty string", "file:///tmp/[[ .field ]].json"))
		// create writerFactory
		_ = func(path string) (io.WriteCloser, error) {
			return nil, fmt.Errorf("error writer factory")
		}

		// when
		fileSink := &file.FileSink{
			DestinationURITemplate: tmpl,
			// RecordReader:           mockRecordReader,
			// WriterFactory:          writerFactory,
			// Getter:                 mockGetter,
		}
		err := fileSink.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error writer factory")
	})
	t.Run("return error if writing to file is failing", func(t *testing.T) {
		// given
		// record
		record := model.NewRecord()
		record.Set("field", "value")
		// create mockRecordReader
		mockRecordReader := mocks.NewRecordReader(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(record, nil)
		}
		mockRecordReader.On("ReadRecord").Return(seq2)
		// create mockGetter
		mockGetter := mocks.NewGetter(t)
		mockGetter.On("Logger").Return(slog.Default())
		// create empty template
		tmpl := template.Must(compiler.NewTemplate("empty string", "file:///tmp/[[ .field ]].json"))
		// create writeFlusher
		writeFlusher := mocks.NewWriteFlusher(t)
		writeFlusher.On("Write", mock.Anything).Return(0, fmt.Errorf("error writing"))
		// create writerFactory
		writerFactory := func(path string) (xio.WriteFlusher, error) {
			return writeFlusher, nil
		}
		// create recordHelper
		recordHelper := mocks.NewRecordHelper(t)
		recordHelper.On("RecordWithoutMetadata", mock.Anything).Return(record)

		// when
		fileSink := &file.FileSink{
			DestinationURITemplate: tmpl,
			// RecordReader:           mockRecordReader,
			// RecordHelper:           recordHelper,
			WriterFactory: writerFactory,
			// Getter:                 mockGetter,
			WriteHandlers:      make(map[string]xio.WriteFlusher),
			FileRecordCounters: make(map[string]int),
		}
		err := fileSink.Process()

		// then
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error writing")
	})
	t.Run("return success", func(t *testing.T) {
		// given
		// record
		record := model.NewRecord()
		record.Set("field", "value")
		// create mockRecordReader
		mockRecordReader := mocks.NewRecordReader(t)
		var seq2 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
		seq2 = func(yield func(*orderedmapjson.AnyOrderedMap, error) bool) {
			yield(record, nil)
		}
		mockRecordReader.On("ReadRecord").Return(seq2)
		// create mockGetter
		mockGetter := mocks.NewGetter(t)
		mockGetter.On("Logger").Return(slog.Default())
		// create empty template
		tmpl := template.Must(compiler.NewTemplate("empty string", "file:///tmp/[[ .field ]].json"))
		// create writeCloser
		buf := make([]byte, 32*1024)
		w := xio.NewBufferedWriter(bytes.NewBuffer(buf))
		// create writerFactory
		writerFactory := func(path string) (xio.WriteFlusher, error) {
			return w, nil
		}
		// create recordHelper
		recordHelper := mocks.NewRecordHelper(t)
		recordHelper.On("RecordWithoutMetadata", mock.Anything).Return(record)

		// when
		fileSink := &file.FileSink{
			DestinationURITemplate: tmpl,
			// RecordReader:           mockRecordReader,
			// RecordHelper:           recordHelper,
			WriterFactory: writerFactory,
			// Getter:                 mockGetter,
			WriteHandlers:      make(map[string]xio.WriteFlusher),
			FileRecordCounters: make(map[string]int),
		}
		err := fileSink.Process()

		// then
		assert.NoError(t, err)
	})

}
