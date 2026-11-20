/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azuredisk

import (
	"flag"
	"reflect"
	"testing"
)

func TestDriverOptions_AddFlags(t *testing.T) {
	o := &DriverOptions{}
	typeInfo := reflect.TypeOf(*o)

	got := o.AddFlags()
	count := 0
	got.VisitAll(func(_ *flag.Flag) {
		count++
	})
	// Legacy origin authorities are programmatic hooks, never CLI flags.
	want := typeInfo.NumField()
	for _, name := range []string{"Origin", "PublishedBlock"} {
		if _, ok := typeInfo.FieldByName(name); ok {
			want--
		}
	}
	if count != want {
		t.Errorf("DriverOptions.AddFlags() = %v, want %v", count, want)
	}
	if err := got.Parse([]string{"--online-block-domain-config=canary.json"}); err != nil {
		t.Fatal(err)
	}
	if o.OnlineDomainConfig != "canary.json" || o.Origin != nil || o.PublishedBlock != nil {
		t.Fatal("online configuration must be explicit without enabling legacy authorities")
	}
}
