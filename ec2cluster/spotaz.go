package ec2cluster

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/sync/once"
)

var (
	azMappingOnce once.Task
	// azMapping maps Availability Zone ids to names.
	// See https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
	// The mapping is different for each AWS account, but does not change over time.
	azMapping = make(map[string]string)
)

// availabilityZones returns a list of availability zone names for the given region.
func availabilityZones(api ec2iface.EC2API, region string) ([]string, error) {
	err := azMappingOnce.Do(func() error {
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
			azMapping[*az.ZoneId] = *az.ZoneName
		}
		return nil
	})
	zones := make([]string, 0, len(azMapping))
	for _, zone := range azMapping {
		zones = append(zones, zone)
	}
	return zones, err
}
