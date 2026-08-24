package sync

import "context"

// ReferralSendFilter gates one profile's referral batch inside
// [Service.ProcessReferrals]. It is the seam consumers use to inject
// concurrent-race protection on the P2P Custom Messages send side —
// typically a wrapper around a persistent reservation store keyed on
// the profile so two concurrent triggers on the same profile can't
// both dispatch its unprocessed invitations.
//
// A nil filter (the zero value on a [Service]) dispatches every batch
// Consumers that need concurrency protection
// supply one via [ServiceWithReferralSendFilter].
//
// AllowSend runs ONCE per [ReferralBatch], before the send loop —
// gating the profile, not the individual message.
//
// The three outcomes:
//
//   - allow=true, err=nil: dispatch the batch. Failed entries stay
//     processedAt-empty and retry on the next trigger AFTER the
//     reservation window closes; there is no per-batch rollback.
//   - allow=false, err=nil: skip this batch entirely. No sends, no
//     write-back — a natural next trigger re-fetches and re-runs the
//     mapper once the reservation window clears.
//   - err non-nil: fail-closed. Same effect as allow=false (skip + no
//     write-back), and the error is joined into ProcessReferrals's
//     return so the caller sees the backing-store outage.
//
// The release closure is returned for symmetry with the wider
// protection stack's release-on-failure pattern but [Service] does
// NOT invoke it — see [Service.processReferralBatch] for the
// rationale (calling release on failure would pop the reservation
// timestamp and let a concurrent worker Proceed against a fresh slot,
// defeating the whole point of the gate). Consumers implementing
// this interface may return a nil release closure; the fez side
// tolerates either shape.
type ReferralSendFilter interface {
	AllowSend(ctx context.Context, profileID string) (allow bool, release func(context.Context) error, err error)
}
