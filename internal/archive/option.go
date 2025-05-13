package archive

type FileArchiverOption func(f *FileArchiver)

func WithPassword(password string) FileArchiverOption {
	return func(f *FileArchiver) {
		if password != "" {
			f.password = password
		}
	}
}

func WithExtension(extension string) FileArchiverOption {
	return func(f *FileArchiver) {
		if extension != "" {
			f.extension = extension
		}
	}
}
