package tool

import (
	"testing"

	"github.com/grailbio/reflow"
)

func TestParseName(t *testing.T) {
	d, _ := reflow.Digester.Parse("sha256:9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49")
	dShort, _ := reflow.Digester.Parse("sha256:9909853c00000000000000000000000000000000000000000000000000000000")
	hostname := "ec2-35-165-199-174.us-west-2.compute.amazonaws.com"
	serviceUrl := hostname + ":9000"
	allocID := "bb97e35db4101030"
	allocURI := serviceUrl + "/" + allocID
	execId := "9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49"
	execURI := allocURI + "/" + execId
	for _, tt := range []struct {
		raw   string
		want  name
		wantE bool
	}{
		{raw: "9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49", want: name{Kind: idName, ID: d}},
		{raw: "sha256:9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49", want: name{Kind: idName, ID: d}},
		{raw: "9909853c8", wantE: true},
		{raw: "9909853c", want: name{Kind: idName, ID: dShort}},
		{raw: "sha256:9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49", want: name{Kind: idName, ID: d}},
		{raw: hostname, want: name{Kind: hostName, Hostname: hostname}},
		{raw: allocURI, want: name{Kind: allocName, Hostname: hostname, HostAndPort: serviceUrl, AllocID: allocID}},
		{raw: execURI, want: name{Kind: execName, Hostname: hostname, HostAndPort: serviceUrl, AllocID: allocID, ID: d}},
		{raw: allocID + "/" + execId, want: name{Kind: execName, AllocID: allocID, ID: d}},
	} {
		n, err := parseName(tt.raw)
		if got, want := err != nil, tt.wantE; got != want {
			t.Errorf("got %v, want %v: error %v", got, want, err)
		}
		if err != nil {
			continue
		}
		if got, want := n, tt.want; got != want {
			t.Errorf("got %v, want %v ", got, want)
		}
	}
}

func TestName(t *testing.T) {
	hostname := "ec2-35-165-199-174.us-west-2.compute.amazonaws.com"
	serviceUrl := hostname + ":9000"
	allocID := "bb97e35db4101030"
	allocUri := serviceUrl + "/" + allocID
	id := "9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49"
	raw := allocUri + "/" + id

	n, err := parseName(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := allocURI(n), allocUri; got != want {
		t.Errorf("got %v, want %v ", got, want)
	}
	if got, want := execPath(n), "/"+allocID+"/"+id; got != want {
		t.Errorf("got %v, want %v ", got, want)
	}

	n, err = parseName(allocUri)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := allocURI(n), allocUri; got != want {
		t.Errorf("got %v, want %v ", got, want)
	}
}
