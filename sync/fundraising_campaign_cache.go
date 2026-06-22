package sync

import "context"

// FundraisingCampaignCache is an optional cross-call cache for
// [FundraisingCampaign] documents fetched from the Raisely API. The
// cache is keyed by p2pID (globally unique campaign identifier) and is
// consulted by [RaiselyFetcherAndUpdater.CachedFundraisingCampaign]
// before reaching Raisely.
//
// The cache is opt-in. Wiring it is the consumer's responsibility — fez
// ships the interface only; an implementation lives downstream (typically
// a shared cross-process store). A nil implementation means no caching at
// all — every CachedFundraisingCampaign call fetches directly from Raisely.
//
// # TTL
//
// Implementations own TTL internally. The interface deliberately does
// not expose it: the fetcher never needs to read it, and exposing it
// would invite callers to second-guess the implementation's expiry
// policy.
//
// # Fail-policy
//
// Backing-store errors are reported via the bool / error returns. The
// fetcher fails open on Get errors — treats them as a miss and fetches
// from Raisely — so a transient backing-store hiccup degrades to
// uncached behaviour, not to a stale or failed response. Set errors are
// logged and swallowed by the fetcher; the next call will retry. Delete
// errors propagate to the operator-driven cache-bust caller.
type FundraisingCampaignCache interface {
	// Get returns the cached [FundraisingCampaign] for p2pID. ok is true
	// when a non-expired entry is present; false (with a nil
	// *FundraisingCampaign) is a miss, an expired entry, or an entry the
	// implementation has otherwise chosen not to surface. A non-nil err
	// reports a backing-store failure; callers fail-open (treat as a
	// miss and fetch from Raisely).
	Get(ctx context.Context, p2pID string) (campaign *FundraisingCampaign, ok bool, err error)

	// Set writes campaign into the cache under p2pID, stamping org
	// alongside it. org is a labelling field — typically the <ORG>
	// segment of MAPPING_PATH via [CampaignEnvVar.Org] — surfaced for
	// downstream observability (per-org queries on the storage layer);
	// the interface itself does not act on it. p2pID is globally unique,
	// so org is not needed to disambiguate keys. Returns a non-nil err
	// on backing-store failure; the fetcher logs and continues.
	Set(ctx context.Context, p2pID, org string, campaign *FundraisingCampaign) error

	// Delete removes the entry for p2pID. Used by operator-driven
	// cache-bust paths; the fetcher itself does not call this. Returns
	// a non-nil err on backing-store failure.
	Delete(ctx context.Context, p2pID string) error
}
