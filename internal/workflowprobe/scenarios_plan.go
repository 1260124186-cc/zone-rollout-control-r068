package workflowprobe

import (
	"fmt"
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runPlanRevisionLifecycle() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	blocked, _, err := createActivePlan(
		harness,
		"edge.example",
		2026092501,
		2,
		1,
		80,
	)
	if err != nil {
		return err
	}

	// Revisions remain locked once the plan is active.
	if err := expectFailure(harness.call(http.MethodPatch, "/v1/plans/"+blocked.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    2026092502,
		Authority: "ns1.edge.example",
		Entries: []domain.ZoneEntry{
			{Owner: "edge.example", Type: domain.EntryA, Value: "192.0.2.11"},
		},
	}, nil), http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// An active plan without any assessment cannot pass the new gate.
	if err := expectFailure(harness.call(http.MethodPatch, "/v1/plans/"+blocked.ID+"/complete", map[string]any{
		"expected_version": blocked.Version,
	}, nil), http.StatusConflict, string(domain.CodeAssessmentMissing)); err != nil {
		return err
	}

	if _, err := enrollNode(harness, "resolver-complete-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-complete-02", "west"); err != nil {
		return err
	}
	ready, revision, err := createActivePlan(harness, "complete.example", 2026092601, 2, 1, 80)
	if err != nil {
		return err
	}
	group, err := allocate(harness, ready.ID, "complete-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("complete-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}
	if _, err := harnessAssess(harness, ready.ID, "complete-assess-0001"); err != nil {
		return err
	}

	// The assessment bumps the plan version, so a stale expected_version must
	// be rejected: the verdict and thresholds have to come from one version.
	if err := expectFailure(harness.call(http.MethodPatch, "/v1/plans/"+ready.ID+"/complete", map[string]any{
		"expected_version": ready.Version,
	}, nil), http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}

	var current domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+ready.ID, nil, &current); err != nil {
		return err
	}
	var completed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+ready.ID+"/complete", map[string]any{
		"expected_version": current.Version,
	}, &completed); err != nil {
		return err
	}
	if completed.State != domain.PlanCompleted {
		return ensure(false, "plan with a consistent assessment did not complete")
	}

	// Reading a completed plan keeps working unchanged.
	var fetched domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+completed.ID, nil, &fetched); err != nil {
		return err
	}
	if fetched.State != domain.PlanCompleted || fetched.CompletedAt == nil {
		return ensure(false, "completed plan read did not preserve the final state")
	}

	// Repeating completion stays an idempotent invalid-state conflict instead
	// of re-running the gate.
	if err := expectFailure(harness.call(http.MethodPatch, "/v1/plans/"+completed.ID+"/complete", map[string]any{
		"expected_version": completed.Version,
	}, nil), http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// A review_required verdict is a distinct, distinguishable rejection.
	if _, err := enrollNode(harness, "resolver-review-01", "west"); err != nil {
		return err
	}
	reviewPlan, reviewRevision, err := createActivePlan(harness, "review-complete.example", 2026092701, 1, 1, 20)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		reviewRevision.Serial+1,
		30,
	); err != nil {
		return err
	}
	review, err := harnessAssess(harness, reviewPlan.ID, "review-assess-0001")
	if err != nil {
		return err
	}
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "review plan assessment was not review_required")
	}
	var reviewCurrent domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+reviewPlan.ID, nil, &reviewCurrent); err != nil {
		return err
	}
	return expectFailure(
		harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/complete", map[string]any{
			"expected_version": reviewCurrent.Version,
		}, nil),
		http.StatusConflict,
		string(domain.CodeAssessmentNotReady),
	)
}

func harnessAssess(h *harness, planID string, requestKey string) (domain.RolloutAssessment, error) {
	var result domain.RolloutAssessment
	err := h.call(http.MethodPatch, "/v1/plans/"+planID+"/assessments", domain.AssessmentInput{
		RequestKey: requestKey,
	}, &result)
	return result, err
}
