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
	"errors"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cp "sigs.k8s.io/cluster-inventory-api/apis/v1alpha2"
)

const placementDecisionKEP = "keps/sig-multicluster/5313-placement-decision-api/README.md"

func placementDecisionRef(anchor string) string {
	return kepRef(PlacementDecisionSpecRevision, placementDecisionKEP, anchor)
}

// placementDecisionCatalog lists what a PlacementDecision producer is checked against.
var placementDecisionCatalog = []Requirement{
	{
		ID:          CheckPlacementDecisionAPIAvailability,
		Description: "PlacementDecision is served in multicluster.x-k8s.io/v1alpha2",
		Level:       LevelMust,
		Scope:       ScopeAPI,
		Ref:         placementDecisionRef(""),
	},
	{
		ID:          CheckPlacementDecisionNamespaced,
		Description: "PlacementDecision is namespace scoped",
		Level:       LevelMust,
		Scope:       ScopeAPI,
		Ref:         placementDecisionRef(""),
	},
}

var placementDecisionResource = servedResource{
	name:              cp.PlacementDecisionSchemeGroupVersionResource.Resource,
	listing:           "placement decisions",
	availabilityCheck: CheckPlacementDecisionAPIAvailability,
	namespacedCheck:   CheckPlacementDecisionNamespaced,
}

var placementDecisionProducer = profile{
	name:     PlacementDecisionProducerProfile,
	api:      PlacementDecisionAPI,
	spec:     Specification{Revision: PlacementDecisionSpecRevision, URL: placementDecisionRef("")},
	catalog:  placementDecisionCatalog,
	features: featuresOf(placementDecisionCatalog),
	digest:   digestOf(placementDecisionCatalog),
	environment: func(o Options) Environment {
		return Environment{
			DecisionNamespace: o.DecisionNamespace,
			SchedulerName:     o.SchedulerName,
			ObservedDecisions: []ObjectReference{},
		}
	},
	validateEnvironment: func(e Environment) error {
		if e.DecisionNamespace == "" {
			return errors.New("decision-namespace is required: select the namespace a scheduler publishes to")
		}
		return nil
	},
	observed: func(e Environment) (string, string, []ObjectReference) {
		return "decision", e.DecisionNamespace, e.ObservedDecisions
	},
	observe: observePlacementDecisions,
}

type decisionSnapshot struct {
	discovered
	// decisions are the slices published by the scheduler under test, sorted by name.
	decisions []cp.PlacementDecision
	listError error
}

func observePlacementDecisions(
	ctx context.Context,
	o Options,
	r *ConformanceReport,
) (func(Requirement) TestResult, error) {
	if err := readNamespace(ctx, o, "decision", o.DecisionNamespace); err != nil {
		return nil, err
	}
	var s decisionSnapshot
	s.resources, s.apiError = serverResources(ctx, o.Kubernetes.Discovery())
	s.listError = s.apiError
	if s.apiError == nil {
		var list *cp.PlacementDecisionList
		list, s.listError = o.ClusterInventory.ApisV1alpha2().PlacementDecisions(o.DecisionNamespace).List(
			ctx,
			metav1.ListOptions{},
		)
		if s.listError == nil {
			s.decisions = slices.DeleteFunc(list.Items, func(d cp.PlacementDecision) bool {
				return o.SchedulerName != "" && d.SchedulerName != o.SchedulerName
			})
		}
	}
	slices.SortFunc(s.decisions, func(a, b cp.PlacementDecision) int { return strings.Compare(a.Name, b.Name) })
	for _, d := range s.decisions {
		r.Environment.ObservedDecisions = append(r.Environment.ObservedDecisions, referenceTo(d.ObjectMeta))
	}
	return func(req Requirement) TestResult { return checkDecisions(req, s) }, nil
}

func checkDecisions(req Requirement, s decisionSnapshot) TestResult {
	if req.Scope == ScopeAPI {
		return checkAPI(req, s.discovered, placementDecisionResource, s.listError)
	}
	if s.listError != nil {
		return outcome(req, Unknown, "Cannot list placement decisions: "+s.listError.Error())
	}
	// Checks of the published slices (labels, slice size, references) go here.
	return outcome(req, Unknown, "No check implements "+req.ID)
}
