package ec2cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/reflow/log"
)

var (
	azIdToNameOnce once.Task
	// azIdToName maps Availability Zone ids to names.
	// See https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
	// The mapping is different for each AWS account, but does not change over time.
	azIdToName = make(map[string]string)

	azNameToSubnetOnce once.Task
	// azNameToSubnet maps Availability Zone names to subnets.
	azNameToSubnet = make(map[string]string)

	spotScoreLimiter = rate.NewLimiter(rate.Every(time.Second), 10) // 10 qps
)

// availabilityZones returns a list of availability zone names for the given region.
func availabilityZones(api ec2iface.EC2API, region string) ([]string, error) {
	err := azIdToNameOnce.Do(func() error {
		req := &ec2.DescribeAvailabilityZonesInput{
			Filters: []*ec2.Filter{{
				Name:   aws.String("region-name"),
				Values: aws.StringSlice([]string{region}),
			}},
		}
		resp, err := api.DescribeAvailabilityZones(req)
		if err != nil {
			return err
		}
		for _, az := range resp.AvailabilityZones {
			azIdToName[*az.ZoneId] = *az.ZoneName
		}
		return nil
	})
	zones := make([]string, 0, len(azIdToName))
	for _, zone := range azIdToName {
		zones = append(zones, zone)
	}
	return zones, err
}

// computeAzSubnetMap computes a mapping of availability-zone name to subnet id based on the given list of subnetIds.
func computeAzSubnetMap(api ec2iface.EC2API, subnetIds []string, log *log.Logger) error {
	return azNameToSubnetOnce.Do(func() error {
		req := &ec2.DescribeSubnetsInput{SubnetIds: aws.StringSlice(subnetIds)}
		resp, err := api.DescribeSubnets(req)
		if err != nil {
			return err
		}
		for _, sn := range resp.Subnets {
			if sn.AvailabilityZone == nil || sn.SubnetId == nil {
				continue
			}
			az, subnetId := *sn.AvailabilityZone, *sn.SubnetId
			if old, ok := azNameToSubnet[az]; ok {
				log.Debugf("computeAzSubnetMap: remapping subnet for AZ %s from %s to %s",
					az, old, subnetId)
			}
			azNameToSubnet[az] = subnetId
		}
		return nil
	})
}

// subnetForAZ returns an appropriate subnet for the given availability-zone name.
// subnetForAZ must be called only after computeAzSubnetMap has been called by the same process,
// otherwise, it will always return empty strings.
func subnetForAZ(azName string) string {
	return azNameToSubnet[azName]
}

// GetSpotPlacementScores returns spot placement scores for the given instance type in the given region.
// GetSpotPlacementScores returns a map of each Availability Zone name (within the given region) to the score.
// Note that the region is stripped from the AZ names
// (for eg: if region is "us-west-2", then AZ name "us-west-2a" is trimmed to "a").
func GetSpotPlacementScores(ctx context.Context, api ec2iface.EC2API, region, instanceType string) (map[string]int, error) {
	if _, err := availabilityZones(api, region); err != nil {
		return nil, err
	}
	if err := spotScoreLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("getSpotPlacementScores rate limiter: %v", err)
	}
	req := &ec2.GetSpotPlacementScoresInput{
		InstanceTypes:          aws.StringSlice([]string{instanceType}),
		RegionNames:            aws.StringSlice([]string{region}),
		SingleAvailabilityZone: aws.Bool(true),
		TargetCapacity:         aws.Int64(10),
		TargetCapacityUnitType: aws.String("units"),
	}
	resp, err := api.GetSpotPlacementScores(req)
	if err != nil {
		return nil, fmt.Errorf("getSpotPlacementScores ec2.GetSpotPlacementScores: %v", err)
	}
	azScores := make(map[string]int, len(resp.SpotPlacementScores))
	for _, s := range resp.SpotPlacementScores {
		az := *s.AvailabilityZoneId
		if azName, ok := azIdToName[az]; ok {
			az = strings.TrimPrefix(azName, region)
		}
		azScores[az] = int(*s.Score)
	}
	return azScores, nil
}
