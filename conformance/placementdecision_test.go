/*
Copyright The Kubernetes Authors.

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

package conformance

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	cp "sigs.k8s.io/cluster-inventory-api/apis/v1alpha2"
	cpfake "sigs.k8s.io/cluster-inventory-api/client/clientset/versioned/fake"
)

const (
	testDecisions = "decisions"
	testScheduler = "scheduler"
)

func sampleDecision(name, scheduler string) *cp.PlacementDecision {
	return &cp.PlacementDecision{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       testDecisions,
			UID:             types.UID(name + "-uid"),
			ResourceVersion: "1",
		},
		Decisions: []cp.ClusterDecision{
			{ClusterProfileRef: cp.ClusterProfileReference{Name: "a", Namespace: testInventory}},
		},
		SchedulerName: scheduler,
	}
}

// decisionOptions serves both APIs and seeds one ClusterProfile plus the given
// decisions, so the same options can select either profile or both.
func decisionOptions(
	t *testing.T,
	decisions ...runtime.Object,
) (Options, *kubefake.Clientset, *cpfake.Clientset) {
	t.Helper()
	o, k, c := sampleOptions(append([]runtime.Object{sampleProfile("a", testInventory, "id-a")}, decisions...)...)
	if err := k.Tracker().Add(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testDecisions}}); err != nil {
		t.Fatal(err)
	}
	k.Resources[0].APIResources = append(k.Resources[0].APIResources, metav1.APIResource{
		Name:       cp.PlacementDecisionSchemeGroupVersionResource.Resource,
		Kind:       cp.PlacementDecisionKind,
		Namespaced: true,
	})
	o.DecisionNamespace = testDecisions
	o.SchedulerName = testScheduler
	return o, k, c
}

func TestUnitDefaultSelectionIsClusterProfile(t *testing.T) {
	o, _, _ := decisionOptions(t, sampleDecision("d", testScheduler))
	reports, err := RunAll(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Profiles[0].Name != ProducerProfile {
		t.Fatalf("default selection changed: %+v", reports)
	}
	if len(reports[0].Environment.ObservedDecisions) != 0 || reports[0].Environment.DecisionNamespace != "" {
		t.Fatalf("ClusterProfile report carries PlacementDecision inputs: %+v", reports[0].Environment)
	}
}

func TestUnitPlacementDecisionRun(t *testing.T) {
	o, k, c := decisionOptions(t, sampleDecision("b", testScheduler), sampleDecision("a", testScheduler),
		sampleDecision("other", testOther))
	o.APIs = []string{PlacementDecisionAPI}
	o.SupportedFeatures = nil
	r, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	p := r.Profiles[0]
	if p.Name != PlacementDecisionProducerProfile || p.Statistics.Passed != len(placementDecisionCatalog) {
		t.Fatalf("unexpected report: %+v", r)
	}
	if r.Specification.Revision != PlacementDecisionSpecRevision || r.Suite.CatalogDigest == CatalogDigest() {
		t.Fatalf("report is pinned to the ClusterProfile specification: %+v", r.Specification)
	}
	names := make([]string, 0, len(r.Environment.ObservedDecisions))
	for _, d := range r.Environment.ObservedDecisions {
		names = append(names, d.Name)
	}
	if !slices.Equal(names, []string{"a", "b"}) {
		t.Fatalf("observed decisions %v, want the scheduler's slices sorted by name", names)
	}
	if err = ValidateReport(r, true); err != nil {
		t.Fatal(err)
	}
	for _, actions := range [][]clienttesting.Action{k.Actions(), c.Actions()} {
		for _, action := range actions {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatalf("observer wrote resources: %+v", action)
			}
		}
	}

	r.Environment.ObservedDecisions = nil
	if err = ValidateReport(r, true); err == nil {
		t.Fatal("submission without observed decisions was accepted")
	}
}

func TestUnitPlacementDecisionSetup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Options, *kubefake.Clientset)
		id     string
		out    Outcome
	}{
		{"missing decision namespace", func(o *Options, k *kubefake.Clientset) {
			o.DecisionNamespace = ""
		}, CheckPlacementDecisionAPIAvailability, Unknown},
		{"ClusterProfile feature claimed", func(o *Options, k *kubefake.Clientset) {
			o.SupportedFeatures = []string{Joined}
		}, CheckPlacementDecisionAPIAvailability, Unknown},
		{"API not served", func(o *Options, k *kubefake.Clientset) {
			k.Resources[0].APIResources = k.Resources[0].APIResources[:1]
		}, CheckPlacementDecisionAPIAvailability, Failed},
		{"cluster scoped API", func(o *Options, k *kubefake.Clientset) {
			k.Resources[0].APIResources[1].Namespaced = false
		}, CheckPlacementDecisionNamespaced, Failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, k, _ := decisionOptions(t, sampleDecision("d", testScheduler))
			o.APIs = []string{PlacementDecisionAPI}
			o.SupportedFeatures = nil
			tc.change(&o, k)
			r, err := Run(context.Background(), o)
			if err == nil || findResult(t, r, tc.id).Outcome != tc.out {
				t.Fatalf("err=%v check=%+v", err, findResult(t, r, tc.id))
			}
		})
	}
}

func TestUnitBothProfilesWritePerAPIReports(t *testing.T) {
	o, _, _ := decisionOptions(t, sampleDecision("d", testScheduler))
	o.APIs = []string{ClusterProfileAPI, PlacementDecisionAPI}
	if _, err := RunAll(context.Background(), o); err == nil ||
		!strings.Contains(err.Error(), "per-api") {
		t.Fatalf("several profiles were written to one report: %v", err)
	}
	if _, err := Run(context.Background(), o); err == nil {
		t.Fatal("Run observed more than one profile")
	}

	o.ReportPerAPI = true
	reports, err := RunAll(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 {
		t.Fatalf("got %d reports, want one per profile", len(reports))
	}
	if got := reports[1].Profiles[0].SupportedFeatures; len(got) != 0 {
		t.Fatalf("ClusterProfile features leaked into the PlacementDecision report: %v", got)
	}
	dir := t.TempDir()
	for _, r := range reports {
		if err = WriteReports(dir, []string{ReportFormatYAML}, true, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"report-clusterprofile.yaml", "report-placementdecision.yaml"} {
		r, err := ReadReport(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err = ValidateReport(r, true); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("report directory has %d files: %v", len(entries), err)
	}
}

func TestUnitReportValidatedAgainstItsOwnProfile(t *testing.T) {
	o, _, _ := decisionOptions(t, sampleDecision("d", testScheduler))
	o.APIs = []string{PlacementDecisionAPI}
	o.SupportedFeatures = nil
	r, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*ConformanceReport)
	}{
		{"ClusterProfile specification", func(r *ConformanceReport) { r.Specification = clusterProfileProducer.spec }},
		{"ClusterProfile catalog", func(r *ConformanceReport) { r.Suite.CatalogDigest = CatalogDigest() }},
		{"API of another profile", func(r *ConformanceReport) { r.Profiles[0].API = ClusterProfileAPI }},
		{"unknown profile", func(r *ConformanceReport) { r.Profiles[0].Name = "typo" }},
		{"ClusterProfile feature", func(r *ConformanceReport) { r.Profiles[0].SupportedFeatures = []string{Joined} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := r
			changed.Profiles = slices.Clone(r.Profiles)
			tc.change(&changed)
			if err := ValidateReport(changed, false); err == nil {
				t.Fatal("report accepted")
			}
		})
	}
}
