package remotes

import "github.com/thediveo/enumflag/v2"

type Endpoint enumflag.Flag

const (
	None Endpoint = iota
	S3
)

type Store interface {
	Save(currentFile string) error
}
