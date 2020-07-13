package ec2cluster

import (
	"expvar"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
)

// ExpVarCluster is the expvar endpoint for ec2cluster information.
const ExpVarCluster = "ec2cluster"

// trackedInstance is a struct that contains a subset of the fields that define an ec2cluster
// instance.
type trackedInstance struct {
	InstanceType string
}

// InstanceTypeStat is a tuple that stores the count of instances of a specified type
// within an AWS EC2 cluster.
type InstanceTypeStat struct {
	// InstanceType is an AWS EC2 instance type, i.e. r3.8xlarge
	InstanceType string
	// Count is the number of instances with the specified type.
	Count int
}

// OverallStats is a set of variables that describe instances within an ec2cluster
// and various aggregations of those instances (i.e. by instance type).
type OverallStats struct {
	// InstanceIds is a slice of the instanceIds that are active within the ec2cluster maintained
	// by the current process.
	InstanceIds []string
	// TotalsByType is a slice of InstanceTypeStat tuples that define aggregations of the instances
	// in InstanceIds by instance type.
	TotalsByType []InstanceTypeStat
}

type statsImpl struct {
	reflowletInstances map[string]*trackedInstance
	mu                 sync.Mutex
	published          bool
}

func newStats() *statsImpl {
	return &statsImpl{
		reflowletInstances: make(map[string]*trackedInstance),
	}
}

// Publish the expvar to ExpVarCluster.
func (si *statsImpl) publish() {
	expvar.Publish(ExpVarCluster, expvar.Func(func() interface{} { return si.getStats() }))
}

func (si *statsImpl) setInstancesStats(instances map[string]*reflowletInstance) {
	reflowletInstances := make(map[string]*trackedInstance)
	for instId, inst := range instances {
		reflowletInstances[instId] = &trackedInstance{
			InstanceType: aws.StringValue(inst.InstanceType),
		}
	}

	si.mu.Lock()
	defer si.mu.Unlock()
	si.reflowletInstances = reflowletInstances
}

func (si *statsImpl) getStats() OverallStats {
	typeToStat := make(map[string]*InstanceTypeStat)
	instances := make([]string, 0)

	si.mu.Lock()
	for id, inst := range si.reflowletInstances {
		if stat, ok := typeToStat[inst.InstanceType]; ok {
			stat.Count += 1
		} else {
			typeToStat[inst.InstanceType] = &InstanceTypeStat{
				Count:        1,
				InstanceType: inst.InstanceType,
			}
		}
		instances = append(instances, id)
	}
	si.mu.Unlock()

	typeStats := make([]InstanceTypeStat, 0)
	for _, stat := range typeToStat {
		typeStats = append(typeStats, *stat)
	}
	return OverallStats{
		InstanceIds:  instances,
		TotalsByType: typeStats,
	}
}
