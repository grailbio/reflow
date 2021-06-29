// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/cloud/spotfeed"
	"github.com/grailbio/reflow/ec2cluster"
)

type costType int

const (
	costTypeUnknown costType = iota
	costTypeExact
	costTypeUpperBound
)

// Cost tracks the cost of something along with whether its an exact cost.
type Cost struct {
	value float64
	// type determines the cost's type (ie, exact or upper bound)
	typ costType
}

// NewCostUB creates a cost which is an Upper bound cost.
func NewCostUB(value float64) Cost {
	return Cost{value, costTypeUpperBound}
}

// NewCostExact creates an exact cost.
func NewCostExact(value float64) Cost {
	return Cost{value, costTypeExact}
}

func (c *Cost) Add(d Cost) {
	c.value += d.value
	switch {
	case d.typ == costTypeUnknown:
		// do nothing
	case c.typ == costTypeUnknown:
		c.typ = d.typ
	case c.typ == costTypeUpperBound || d.typ == costTypeUpperBound:
		c.typ = costTypeUpperBound
	default:
		c.typ = costTypeExact
	}
}

func (c *Cost) Mul(v float64) {
	if c.typ == costTypeUnknown {
		panic("Cost.Mul on uninitialized cost")
	}
	c.value *= v
}

func (c Cost) String() string {
	if c.typ == costTypeUnknown {
		return "<unknown>"
	}
	prefix := ""
	if c.typ == costTypeUpperBound {
		prefix = "<"
	}
	return fmt.Sprintf("%s%5.4f", prefix, c.value)
}

// exactCostTimeDiffTolerance is the tolerance on the difference between
// start and end times that we wanted to compute exact cost for and what we ended up getting based on spotfeed data.
const exactCostTimeDiffTolerance = 2 * time.Minute

type costComputer struct {
	q      spotfeed.Querier
	region string
}

func (c *costComputer) compute(instanceId string, instanceType string, instanceTerminated time.Time, costStart, costEnd time.Time) (cost Cost) {
	var (
		exactStart, exactEnd time.Time
		start, end           = costStart, costEnd
	)
	if c.q != nil {
		// We don't care about errors here, because we'll simply compute the upper-bound costs instead.
		if spotcost, err := c.q.Query(instanceId, spotfeed.Period{Start: start, End: end}, instanceTerminated); err == nil {
			cost.Add(NewCostExact(spotcost.ChargeUSD))
			exactStart, exactEnd = spotcost.Start, spotcost.End
		}
	}
	ubtimes := make([]spotfeed.Period, 0)

	// We assume that exactStart and exactEnd are either both zero or both non-zero.
	// and if non-zero, the period {exactStart, exactEnd} is within {start, end}.
	switch {
	case exactStart.IsZero() && exactEnd.IsZero():
		ubtimes = append(ubtimes, spotfeed.Period{Start: start, End: end})
	case diffTimes(start, exactStart) < exactCostTimeDiffTolerance && diffTimes(end, exactEnd) < exactCostTimeDiffTolerance:
		return
	default:
		if start.Before(exactStart) {
			ubtimes = append(ubtimes, spotfeed.Period{Start: start, End: exactStart})
		}
		if end.After(exactEnd) {
			ubtimes = append(ubtimes, spotfeed.Period{Start: exactEnd, End: end})
		}
	}
	if instanceType != "" {
		for _, ubt := range ubtimes {
			if hourlyPriceUsd := ec2cluster.OnDemandPrice(instanceType, c.region); hourlyPriceUsd > 0 {
				cost.Add(NewCostUB(hourlyPriceUsd * ubt.End.Sub(ubt.Start).Hours()))
			}
		}
	}
	return
}

// costComputer returns a cost computer.
// If exact is true, then it is backed by AWS Spot instance data feed data for the given {start, end} time range (if available).
// if exact is false, then start/end times are ignored.
func (c *Cmd) costComputer(ctx context.Context, exact bool, start, end time.Time) (cc *costComputer) {
	var sess *session.Session
	if err := c.Config.Instance(&sess); err != nil {
		c.Log.Errorf("spotfeed querier: no aws session!")
		return
	}
	cc = &costComputer{region: *sess.Config.Region}
	if !exact {
		return
	}
	// We pad the given time range just to make sure that we get costs for the time range we want.
	// The time period chosen for padding is not based on any particular reasoning.
	start, end = start.Add(-3*time.Hour), end.Add(3*time.Hour)
	// Since spotfeed is verbose, we discard its logs.
	discard := log.New(ioutil.Discard, "", 0)
	if loader, err := spotfeed.NewSpotFeedLoader(sess, discard, &start, &end, 0); err != nil {
		c.Log.Errorf("spotfeed loader: %v", err)
	} else if q, qerr := spotfeed.NewQuerier(ctx, loader); qerr != nil {
		c.Log.Errorf("spotfeed querier: %v", qerr)
	} else {
		cc.q = q
	}
	return
}
