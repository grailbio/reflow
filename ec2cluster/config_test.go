package ec2cluster

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/ecr/ecriface"
)

func TestValidateReflowletImage(t *testing.T) {
	for _, tc := range []struct {
		api       ecriface.ECRAPI
		img       string
		wantError bool
	}{
		{nil, "some_image", false}, // unparseable image names must not throw errors.
		{&mockECRClient{Err: nil}, "619867110810.dkr.ecr.us-west-2.amazonaws.com/reflowlet:1536103490", false},
		{&mockECRClient{Err: fmt.Errorf("some error")}, "619867110810.dkr.ecr.us-west-2.amazonaws.com/reflowlet:1536103490", true},
	} {
		got := validateReflowletImage(tc.api, tc.img, nil)
		if tc.wantError != (got != nil) {
			t.Errorf("validateReflowletImage(%v, %v, nil): got error: %v want error:%v", tc.api, tc.img, got, tc.wantError)
		}
	}
}
