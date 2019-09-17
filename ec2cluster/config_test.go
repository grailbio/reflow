package ec2cluster

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/ecr/ecriface"
)

const image = "619867110810.dkr.ecr.us-west-2.amazonaws.com/reflowbootstrap:reflowbootstrap"

func TestValidateBootstrapImage(t *testing.T) {
	for _, tc := range []struct {
		api       ecriface.ECRAPI
		img       string
		wantError bool
	}{
		{nil, "some_image", false}, // unparseable image names must not throw errors.
		{&mockECRClient{Err: nil}, image, false},
		{&mockECRClient{Err: fmt.Errorf("some error")}, image, true},
	} {
		got := validateBootstrapImage(tc.api, tc.img, nil)
		if tc.wantError != (got != nil) {
			t.Errorf("validateBootstrapImage(%v, %v, nil): got error: %v want error:%v", tc.api, tc.img, got, tc.wantError)
		}
	}
}
