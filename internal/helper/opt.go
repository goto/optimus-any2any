package helper

type RecordReaderOption func(*RecordReader)

func WithLogCheckpoint(count int) RecordReaderOption {
	return func(r *RecordReader) {
		r.logCheckpoint = count
	}
}
