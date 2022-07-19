package reflow

const (

	// ReflowletProcessMemoryReservationPct is the amount of memory that's reserved for the reflowlet process,
	// currently set to 5%. The EC2 overhead and this memory reservation for the reflowlet process,
	// will both be accounted for when we do per-instance verification (using `reflow ec2verify`).
	// NOTE: Changing this will warrant re-verification of all instance types.
	ReflowletProcessMemoryReservationPct = 0.05
)

// GetMaxResourcesMemoryBufferFactor is the buffer factor to compute the max memory resources,
// when determining if a given resource requirement is within the threshold of resources an alloc can provide.
//
// Since the max amount of memory available on an alloc is computed based on ReflowletProcessMemoryReservationPct,
// we simply invert that computation to determine the max amount of actual memory likely available on a reflowlet.
func GetMaxResourcesMemoryBufferFactor() float64 {
	return 1.0 / (1.0 - ReflowletProcessMemoryReservationPct)
}
