package smtp

type StorageConfig struct {
	Mode           string
	DestinationDir string
	Credentials    string
	LinkExpiration int
}
