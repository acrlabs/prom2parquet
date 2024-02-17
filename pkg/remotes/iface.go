package remotes

type RemoteStore interface {
	Save(dir string) error
}
