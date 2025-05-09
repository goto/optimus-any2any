package archive

type Archiver interface {
	Archive(files []string) error
}
