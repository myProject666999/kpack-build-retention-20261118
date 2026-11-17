package image

import (
	"context"
	"sort"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	buildapi "github.com/pivotal/kpack/pkg/apis/build/v1alpha2"
)

const (
	// defaultBuildHistoryLimit mirrors the admission default in
	// pkg/apis/build/v1alpha2/image_validation.go (SetDefaults). It is only used
	// when a limit is absent or carries a value that admission would have
	// rejected, so that a malformed object can never turn retention into
	// "delete everything".
	defaultBuildHistoryLimit int64 = 10

	// maxBuildsDeletedPerReconcile bounds how many Delete calls a single
	// reconcile makes. Lowering a limit from 10 to 2 normally needs a handful of
	// deletes, but an Image that has accumulated thousands of Builds (or an
	// operator dropping the limit to 1 after an outage) would otherwise issue
	// them all at once. Deletes are cheap individually but each is a synchronous
	// API write, so a burst competes for API server capacity with the rest of
	// the controller. Whatever is left over is finished by a deliberate requeue.
	maxBuildsDeletedPerReconcile = 20

	// retentionRequeueDelay is how long the controller waits before deleting the
	// next batch. The Build deletions also enqueue the owning Image through the
	// informer, so this delay only guarantees convergence in the cases where an
	// event is not delivered (for example a Build that was already gone).
	retentionRequeueDelay = 10 * time.Second
)

// retentionPlan returns, in stable oldest-first order, the Builds that must be
// deleted to honour the Image's build history limits.
//
// Retention is defined against the finished Builds of each outcome
// independently: the newest `successBuildHistoryLimit` successful Builds and
// the newest `failedBuildHistoryLimit` failed Builds are kept. Everything that
// is still running and everything the Image still references is kept on top of
// those limits, so protected Builds can push an Image slightly above its
// configured limits. That is deliberate: the limits describe how much history
// to keep, and correctness (a Build that is executing, or whose output the
// Image advertises as current, must not disappear) outranks the exact count.
func (l buildList) retentionPlan(image *buildapi.Image) []*buildapi.Build {
	protected := l.protectedBuilds(image)
	successLimit := resolveBuildHistoryLimit(image.Spec.SuccessBuildHistoryLimit)
	failedLimit := resolveBuildHistoryLimit(image.Spec.FailedBuildHistoryLimit)

	candidates := append(
		excessBuilds(l.successful, successLimit, protected),
		excessBuilds(l.failed, failedLimit, protected)...,
	)

	// Delete the oldest excess first so that progress is observable and a
	// volatile clock or list order cannot change which Builds are chosen.
	sort.Slice(candidates, func(i, j int) bool {
		return buildOrderLess(candidates[i], candidates[j])
	})

	return candidates
}

// protectedBuilds returns the names of Builds that must not be deleted
// regardless of the configured limits.
func (l buildList) protectedBuilds(image *buildapi.Image) map[string]struct{} {
	protected := make(map[string]struct{}, len(l.running)+2)

	// A Build that is still executing (or has no Succeeded condition yet) must
	// never be pruned, no matter how old it looks or how small the limit is.
	for _, build := range l.running {
		protected[build.Name] = struct{}{}
	}

	// image.spec.status.latestBuildRef is the only place kpack itself refers to a
	// Build by name: `kp build logs <image>` and pkg/logs resolve the reference
	// to fetch that Build's logs, and the Ready message names it. Deleting the
	// most recent Build would break all of those.
	if ref := image.Status.LatestBuildRef; ref != "" {
		protected[ref] = struct{}{}
	}

	// The Image advertises image.status.latestImage as the artifact currently
	// deployed. If the most recent Build failed, that image was produced by an
	// older successful Build; if that Build has aged out of the successful
	// limit, the Image would claim an image whose provenance no longer exists.
	// Keep the Build that produced the advertised image.
	if latestImage := image.Status.LatestImage; latestImage != "" {
		for _, build := range l.successful {
			if build.BuiltImage() == latestImage {
				protected[build.Name] = struct{}{}
			}
		}
	}

	return protected
}

// excessBuilds returns the Builds in a single outcome category that may be
// deleted: everything except the newest limit Builds and the protected ones.
// category is ordered oldest first.
//
// Protection is additive: the newest limit Builds are retained, and a protected
// Build is retained even if it is older than that. This matches how running
// Builds behave (they are not part of any finished category and so never count
// against a limit) and means a protected Build can push an Image slightly above
// its configured limits. Protected Builds are never deleted.
func excessBuilds(category []*buildapi.Build, limit int, protected map[string]struct{}) []*buildapi.Build {
	var newestFirst []*buildapi.Build

	retained := 0
	for i := len(category) - 1; i >= 0; i-- {
		build := category[i]
		if retained < limit {
			retained++
			continue
		}
		if _, found := protected[build.Name]; found {
			continue
		}
		newestFirst = append(newestFirst, build)
	}

	// Restore oldest-first so callers can delete in a predictable order.
	toDelete := make([]*buildapi.Build, 0, len(newestFirst))
	for i := len(newestFirst) - 1; i >= 0; i-- {
		toDelete = append(toDelete, newestFirst[i])
	}

	return toDelete
}

// resolveBuildHistoryLimit converts the API field into a usable limit.
//
// A history limit of zero is neither "prune all" nor "keep everything": the
// Image API rejects it. See ImageSpec.validateBuildHistoryLimit in
// pkg/apis/build/v1alpha2/image_validation.go, which requires the value to be
// greater than 0, and Image.SetDefaults, which fills in 10 when the field is
// absent. Zero is therefore not a representation any admitted Image can carry.
// If the controller still observes nil or a non-positive value (a CRD applied
// with validation disabled, or an object converted from an older version), it
// applies the same documented default of 10 rather than guessing at either
// extreme, so a bad value can never wipe the history or let it grow unbounded.
func resolveBuildHistoryLimit(limit *int64) int {
	if limit == nil || *limit < 1 {
		return int(defaultBuildHistoryLimit)
	}
	if *limit > int64(^uint(0)>>1) {
		// Unreachable on 64-bit platforms; clamp defensively instead of
		// overflowing when converted to int.
		return int(^uint(0) >> 1)
	}
	return int(*limit)
}

// deleteOldBuilds deletes the Builds that exceed the Image's retention limits.
//
// Cleanup is best-effort and is run after the Image status has been written.
// A failure to delete one Build never prevents the others from being deleted,
// and never prevents the Image from reporting its status: the errors are
// aggregated and returned so the controller retries on a later reconcile.
// Objects that are already gone, or that changed between the list and the
// delete (a conflict, including a failed UID precondition), are not treated as
// errors because there is nothing left for this round to do; the next reconcile
// re-reads the state and decides again.
func (c *Reconciler) deleteOldBuilds(ctx context.Context, image *buildapi.Image, builds buildList) error {
	plan := builds.retentionPlan(image)
	if len(plan) == 0 {
		return nil
	}

	logger := logging.FromContext(ctx)

	var errs []error
	visited := make(map[string]struct{}, len(plan))
	capped := false
	for i, build := range plan {
		if i == maxBuildsDeletedPerReconcile {
			// Bound the load on the API server. The Build deletions above already
			// enqueue this Image again; the requeue below additionally covers
			// deletes that produced no event.
			capped = true
			break
		}

		// Each Build is visited at most once per reconcile. The plan cannot
		// currently contain a Build twice, but this keeps "never retry the same
		// object in one round" true locally rather than relying on that.
		if _, seen := visited[build.Name]; seen {
			continue
		}
		visited[build.Name] = struct{}{}

		err := c.Client.KpackV1alpha2().Builds(image.Namespace).Delete(ctx, build.Name, deleteOptionsWithUID(build))
		switch {
		case err == nil:
			logger.Infof("Deleted build %q from image %q because it exceeded the build history limit", build.Name, image.Name)
		case k8serrors.IsNotFound(err), k8serrors.IsGone(err), k8serrors.IsConflict(err):
			// Already gone, or the object was replaced between the list and the
			// delete. Either way this round has nothing to retry; the next
			// reconcile re-lists and re-evaluates.
			logger.Debugf("Skipped deleting build %q from image %q: %v", build.Name, image.Name, err)
		default:
			// Permission errors and transient API failures: report them without
			// aborting the remaining deletes.
			logger.Warnf("Failed deleting build %q from image %q: %v", build.Name, image.Name, err)
			errs = append(errs, err)
		}
	}

	if aggregate := utilerrors.NewAggregate(errs); aggregate != nil {
		// A returned error already makes the controller retry the key, and the
		// part of the plan above the cap will be picked up by that retry, so the
		// failure is not lost and no separate requeue is needed.
		return aggregate
	}

	if capped {
		return controller.NewRequeueAfter(retentionRequeueDelay)
	}

	return nil
}

// deleteOptionsWithUID makes the delete apply only to the object we decided to
// prune. If a Build with the same name was deleted and recreated in between (the
// only way the name could reappear), the precondition fails and we leave the new
// object alone; the next reconcile evaluates it on its own merits.
func deleteOptionsWithUID(build *buildapi.Build) metav1.DeleteOptions {
	if build.UID == "" {
		return metav1.DeleteOptions{}
	}
	uid := build.UID
	return metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
}
