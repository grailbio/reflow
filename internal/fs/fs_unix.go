package fs

import "syscall"

// Usage specifies disk usage information.
type Usage struct {
	// Total is the total number of bytes available on the disk.
	Total uint64

	// Free is the total number of bytes that are free on the disk.
	Free uint64

	// Avail is the total number of free bytes that are available to
	// unpriveliged users on the disk.
	Avail uint64
}

// Stat queries and returns disk usage information for the disk
// at the given path.
func Stat(path string) (Usage, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return Usage{}, err
	}

	size := uint64(stat.Bsize)
	return Usage{
		Total: uint64(stat.Blocks) * size,
		Free:  uint64(stat.Bfree) * size,
		Avail: uint64(stat.Bavail) * size,
	}, nil
}
