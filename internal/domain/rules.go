package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// The lease ledger keeps the leases that are still open for each plan so that
// release handling and the expiry sweep can settle a lease without walking the
// whole lease table in the snapshot.
func RegisterOpenLease(state *State, planID string, record OpenLeaseRecord) {
	if state.OpenLeases == nil {
		state.OpenLeases = make(map[string][]OpenLeaseRecord)
	}
	record.PlanID = planID
	records := state.OpenLeases[planID]
	for index := range records {
		if records[index].LeaseID == record.LeaseID {
			records[index] = record
			state.OpenLeases[planID] = records
			return
		}
	}
	state.OpenLeases[planID] = append(records, record)
}

func RemoveOpenLease(state *State, leaseID string) bool {
	removed := false
	for planID, records := range state.OpenLeases {
		kept := make([]OpenLeaseRecord, 0, len(records))
		for _, record := range records {
			if record.LeaseID == leaseID {
				removed = true
				continue
			}
			kept = append(kept, record)
		}
		if len(kept) == 0 {
			delete(state.OpenLeases, planID)
			continue
		}
		state.OpenLeases[planID] = kept
	}
	return removed
}

// openLeasePlans lists the plans that still hold open leases. A plan that has
// left the active phase is skipped because its leases are already settled.
func openLeasePlans(state *State) []string {
	planIDs := make([]string, 0, len(state.OpenLeases))
	for planID := range state.OpenLeases {
		plan, ok := state.Plans[planID]
		if !ok || plan.State != PlanActive {
			continue
		}
		planIDs = append(planIDs, planID)
	}
	sort.Strings(planIDs)
	return planIDs
}

// HasOpenLease reports whether the lease can still be settled. Only plans that
// are still active are considered, because a plan that has already finished has
// no lease left to settle.
func HasOpenLease(state *State, leaseID string) bool {
	for _, records := range state.OpenLeases {
		for _, record := range records {
			if record.LeaseID != leaseID {
				continue
			}
			plan, ok := state.Plans[record.PlanID]
			if !ok || plan.State != PlanActive {
				return false
			}
			return true
		}
	}
	return false
}

func DueOpenLeases(state *State, now time.Time) []OpenLeaseRecord {
	due := make([]OpenLeaseRecord, 0)
	for _, planID := range openLeasePlans(state) {
		for _, record := range state.OpenLeases[planID] {
			if record.ExpiresAt.After(now) {
				continue
			}
			due = append(due, record)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].ExpiresAt.Equal(due[j].ExpiresAt) {
			return due[i].LeaseID < due[j].LeaseID
		}
		return due[i].ExpiresAt.Before(due[j].ExpiresAt)
	})
	return due
}

// RebuildOpenLeases restores the ledger from the persisted leases after the
// process starts again. Plans that are no longer active are left out because
// their leases have already been settled by the completion flow.
func RebuildOpenLeases(state *State) {
	rebuilt := make(map[string][]OpenLeaseRecord)
	for leaseID, lease := range state.Leases {
		if lease.State != LeaseClaimed {
			continue
		}
		plan, ok := state.Plans[lease.PlanID]
		if !ok || plan.State != PlanActive {
			continue
		}
		rebuilt[lease.PlanID] = append(rebuilt[lease.PlanID], OpenLeaseRecord{
			LeaseID:      leaseID,
			PlanID:       lease.PlanID,
			NodeID:       lease.NodeID,
			AllocationID: lease.AllocationID,
			ClaimedAt:    lease.ClaimedAt,
			ExpiresAt:    lease.ExpiresAt,
		})
	}
	for planID := range rebuilt {
		sort.Slice(rebuilt[planID], func(i, j int) bool {
			return rebuilt[planID][i].LeaseID < rebuilt[planID][j].LeaseID
		})
	}
	state.OpenLeases = rebuilt
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
	}
	probeCount := len(probes)
	successCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Success {
			successCount++
			if probe.ObservedSerial != revision.Serial {
				reasons["serial_mismatch"] = struct{}{}
			}
		}
	}
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       probeCount,
		SuccessCount:     successCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: maximumLatency,
		CreatedAt:        now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
