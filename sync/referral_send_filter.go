package sync

import "context"

// ReferralSendFilter gates one profile's referral batch inside
// [Service.ProcessReferrals]. It is the seam consumers use to inject
// concurrent-race protection on the P2P Custom Messages send side —
// typically a wrapper around a persistent reservation store keyed on
// the profile so two concurrent triggers on the same profile can't
// both dispatch its unprocessed invitations.
//
// A nil filter (the zero value on a [Service]) dispatches every batch.
// Consumers that need concurrency protection
// supply one via [ServiceWithReferralSendFilter].
//
// AllowSend runs ONCE per [ReferralBatch], before the send loop —
// gating the profile, not the individual message.
//
// The three outcomes:
//
//   - allow=true, release non-nil, err=nil: dispatch the batch.
//     On zero-progress failure (every SendCustomMessage failed) the
//     caller invokes release(ctx) to roll the reservation back so a
//     legitimate retry on the next trigger can re-reserve; on any
//     partial or full success the reservation stands as the record of
//     work done for this profile.
//   - allow=false, err=nil: skip this batch entirely. No sends, no
//     write-back — a natural next trigger re-fetches and re-runs the
//     mapper once the reservation window clears.
//   - err non-nil: fail-closed. Same effect as allow=false (skip + no
//     write-back), and the error is joined into ProcessReferrals's
//     return so the caller sees the backing-store outage.
//
// A release-side error is joined into ProcessReferrals's return
// alongside any send errors but doesn't block the remaining batches.
type ReferralSendFilter interface {
	AllowSend(ctx context.Context, profileID string) (allow bool, release func(context.Context) error, err error)
}
