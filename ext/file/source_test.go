package file_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/goto/optimus-any2any/ext/file"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/mocks"
	"github.com/stretchr/testify/assert"
)

func TestSourceProcess(t *testing.T) {
	t.Run("return success reading record from readers", func(t *testing.T) {
		// given
		buf := []byte("{\"key\": \"value\"}")
		// create mockSender
		mockSender := mocks.NewSender(t)
		mockSender.On("Send", buf)
		// create readers
		r := xio.NewBufferReader(bytes.NewReader(buf))

		// when
		f := &file.FileSource{
			Sender:  mockSender,
			Readers: []io.ReadCloser{r},
		}
		err := f.Process()

		// then
		assert.NoError(t, err)
	})
}
