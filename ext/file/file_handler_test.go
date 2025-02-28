package file_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/stretchr/testify/assert"
)

func TestStdFileHandler(t *testing.T) {
	l, _ := logger.NewLogger("DEBUG")
	t.Run("write before reach the buffer size with no flush", func(t *testing.T) {
		// given
		filename := fmt.Sprintf("/tmp/test%d.txt", rand.IntN(1000))
		fh, err := file.NewStdFileHandler(l, filename)
		assert.NoError(t, err)

		// when
		for i := 0; i < 63; i++ {
			fh.Write([]byte(fmt.Sprintf("%d\n", i)))
		}

		// then
		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		actual := bytes.Split(b, []byte("\n"))
		assert.Len(t, actual, 1) // 0 + new line eof

		os.Remove(filename)
	})
	t.Run("write before reach the buffer size with flush", func(t *testing.T) {
		// given
		filename := fmt.Sprintf("/tmp/test%d.txt", rand.IntN(1000))
		fh, err := file.NewStdFileHandler(l, filename)
		assert.NoError(t, err)

		// when
		for i := 0; i < 63; i++ {
			fh.Write([]byte(fmt.Sprintf("%d\n", i)))
		}
		fh.Flush()

		// then
		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		actual := bytes.Split(b, []byte("\n"))
		assert.Len(t, actual, 64) // 63 + new line eof

		os.Remove(filename)
	})
	t.Run("write when reach the buffer size with no flush", func(t *testing.T) {
		// given
		filename := fmt.Sprintf("/tmp/test%d.txt", rand.IntN(1000))
		fh, err := file.NewStdFileHandler(l, filename)
		assert.NoError(t, err)

		// when
		for i := 0; i < 64; i++ {
			fh.Write([]byte(fmt.Sprintf("%d\n", i)))
		}

		// then
		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		actual := bytes.Split(b, []byte("\n"))
		assert.Len(t, actual, 1) // 0 + new line eof

		os.Remove(filename)
	})
	t.Run("write when reach the buffer size with flush", func(t *testing.T) {
		// given
		filename := fmt.Sprintf("/tmp/test%d.txt", rand.IntN(1000))
		fh, err := file.NewStdFileHandler(l, filename)
		assert.NoError(t, err)

		// when
		for i := 0; i < 64; i++ {
			fh.Write([]byte(fmt.Sprintf("%d\n", i)))
		}
		fh.Flush()

		// then
		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		actual := bytes.Split(b, []byte("\n"))
		assert.Len(t, actual, 65) // 64 + new line eof

		os.Remove(filename)
	})
	t.Run("write after reach the buffer size with no flush", func(t *testing.T) {
		// given
		filename := fmt.Sprintf("/tmp/test%d.txt", rand.IntN(1000))
		fh, err := file.NewStdFileHandler(l, filename)
		assert.NoError(t, err)

		// when
		for i := 0; i < 70; i++ {
			fh.Write([]byte(fmt.Sprintf("%d\n", i)))
		}

		// then
		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		actual := bytes.Split(b, []byte("\n"))
		assert.Len(t, actual, 65) // 64 + new line eof

		os.Remove(filename)
	})
	t.Run("write after reach the buffer size with flush", func(t *testing.T) {
		// given
		filename := fmt.Sprintf("/tmp/test%d.txt", rand.IntN(1000))
		fh, err := file.NewStdFileHandler(l, filename)
		assert.NoError(t, err)

		// when
		for i := 0; i < 70; i++ {
			fh.Write([]byte(fmt.Sprintf("%d\n", i)))
		}
		fh.Flush()

		// then
		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		actual := bytes.Split(b, []byte("\n"))
		assert.Len(t, actual, 71) // 70 + new line eof

		os.Remove(filename)
	})
}
