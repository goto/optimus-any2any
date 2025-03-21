// Description: File handling interface.
// This is subject to change. It should be moved to a more appropriate package.
package extcommon

import "io"

// FileHandler is an interface for file handling.
type FileHandler io.WriteCloser
