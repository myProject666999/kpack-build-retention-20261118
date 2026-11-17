package image

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sclevine/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/pkg/controller"

	buildapi "github.com/pivotal/kpack/pkg/apis/build/v1alpha2"
	corev1alpha1 "github.com/pivotal/kpack/pkg/apis/core/v1alpha1"
	"github.com/pivotal/kpack/pkg/client/clientset/versioned/fake"
	"github.com/pivotal/kpack/pkg/reconciler/testhelpers"
)

// TestBuildRetentionReconcile exercises retention through a full Image
// reconcile against the in-memory client. Each row starts from an Image whose
// stored status already reflects a still-running newest Build, so the reconcile
// performs no build scheduling and no status write, and only prunes history.
func TestBuildRetentionReconcile(t *testing.T) {
	spec.Run(t, "Build retention during reconcile", testBuildRetentionReconcile)
}

func testBuildRetentionReconcile(t *testing.T, when spec.G, it spec.S) {
	const key = retentionNamespace + "/" + retentionImageName

	when("more history than the limits", func() {
		it("deletes exactly the oldest finished builds beyond the limits", func() {
			builds := mixedBuilds()
			// Successful: 1,3,5,7,9,11. Failed: 2,4,6,8,10,12. Running: 13.
			// Limits of 4 and 3 retain successful {5,7,9,11} and failed
			// {8,10,12}; the running build is never touched.
			image := retentionImage(int64Ptr(3), int64Ptr(4))

			h := reconcileOnce(t, key, image, builds)

			require.NoError(t, h.err)
			assert.Equal(t, []string{
				buildName(1), buildName(2), buildName(3), buildName(4), buildName(6),
			}, h.deleted)
			assert.Equal(t, []string{
				buildName(5), buildName(7), buildName(8), buildName(9),
				buildName(10), buildName(11), buildName(12), buildName(13),
			}, h.remaining())
		})

		it("never deletes the running build even when it is the oldest and limits are one", func() {
			builds := []*buildapi.Build{
				plannedBuild(1, corev1.ConditionUnknown, 0), // oldest, still running
				plannedBuild(2, corev1.ConditionTrue, time.Minute),
				plannedBuild(3, corev1.ConditionTrue, 2*time.Minute),
				plannedBuild(4, corev1.ConditionFalse, 3*time.Minute),
				plannedBuild(5, corev1.ConditionFalse, 4*time.Minute),
			}
			image := retentionImage(int64Ptr(1), int64Ptr(1))
			image.Status.LatestBuildRef = ""
			image.Status.LatestImage = ""

			h := reconcileOnce(t, key, image, builds)

			require.NoError(t, h.err)
			assert.Equal(t, []string{buildName(2), buildName(4)}, h.deleted)
			assert.Contains(t, h.remaining(), buildName(1))
			assert.Contains(t, h.remaining(), buildName(3))
			assert.Contains(t, h.remaining(), buildName(5))
		})

		it("counts a build that failed before running against the failed limit", func() {
			// A build whose source could not be fetched still ends as
			// Succeeded=False and has no step states, exactly like this one. It is
			// a failed build for retention purposes.
			neverStarted := plannedBuild(1, corev1.ConditionFalse, 0)
			neverStarted.Status.StepsCompleted = nil
			neverStarted.Status.StepStates = nil
			neverStarted.Status.LatestImage = ""

			builds := []*buildapi.Build{
				neverStarted,
				plannedBuild(2, corev1.ConditionFalse, time.Minute),
				plannedBuild(3, corev1.ConditionFalse, 2*time.Minute),
				plannedBuild(4, corev1.ConditionUnknown, 3*time.Minute), // newest, running
			}
			image := retentionImage(int64Ptr(2), int64Ptr(1))
			image.Status.LatestBuildRef = ""
			image.Status.LatestImage = ""

			h := reconcileOnce(t, key, image, builds)

			require.NoError(t, h.err)
			assert.Equal(t, []string{buildName(1)}, h.deleted)
			assert.Equal(t, []string{
				buildName(2), buildName(3), buildName(4),
			}, h.remaining())
		})

		it("deletes nothing when the history fits", func() {
			builds := []*buildapi.Build{
				plannedBuild(1, corev1.ConditionTrue, 0),
				plannedBuild(2, corev1.ConditionFalse, time.Minute),
				plannedBuild(3, corev1.ConditionUnknown, 2*time.Minute),
			}
			image := retentionImage(int64Ptr(3), int64Ptr(3))
			image.Status.LatestBuildRef = ""
			image.Status.LatestImage = ""

			h := reconcileOnce(t, key, image, builds)

			require.NoError(t, h.err)
			assert.Empty(t, h.deleted)
			assert.Len(t, h.remaining(), 3)
		})
	})

	when("limits change between reconciles", func() {
		it("prunes to a lowered limit and then deletes nothing when raised again", func() {
			image := retentionImage(int64Ptr(1), int64Ptr(1))
			image.Status.LatestBuildRef = ""
			image.Status.LatestImage = ""

			lowered := reconcileOnce(t, key, image, []*buildapi.Build{
				plannedBuild(1, corev1.ConditionTrue, 1*time.Minute),
				plannedBuild(2, corev1.ConditionTrue, 2*time.Minute),
				plannedBuild(3, corev1.ConditionTrue, 3*time.Minute),
				plannedBuild(4, corev1.ConditionTrue, 4*time.Minute),
				plannedBuild(5, corev1.ConditionUnknown, 5*time.Minute), // newest, running
			})

			require.NoError(t, lowered.err)
			assert.Equal(t, []string{buildName(1), buildName(2), buildName(3)}, lowered.deleted)

			// A second reconcile with a raised limit over the surviving objects
			// must neither delete anything nor recreate the deleted builds.
			raised := retentionImage(int64Ptr(10), int64Ptr(10))
			raised.Status.LatestBuildRef = ""
			raised.Status.LatestImage = ""

			again := reconcileOnce(t, key, raised, lowered.remainingBuilds())

			require.NoError(t, again.err)
			assert.Empty(t, again.deleted)
			assert.Equal(t, []string{buildName(4), buildName(5)}, again.remaining())
		})
	})

	when("a delete call fails", func() {
		builds := func() []*buildapi.Build {
			return []*buildapi.Build{
				plannedBuild(1, corev1.ConditionTrue, 1*time.Minute),
				plannedBuild(2, corev1.ConditionTrue, 2*time.Minute),
				plannedBuild(3, corev1.ConditionTrue, 3*time.Minute),
				plannedBuild(4, corev1.ConditionFalse, 4*time.Minute),
				plannedBuild(5, corev1.ConditionFalse, 5*time.Minute),
				plannedBuild(6, corev1.ConditionUnknown, 6*time.Minute), // newest, running
			}
		}

		newHandler := func() *reconcileHandler {
			image := retentionImage(int64Ptr(1), int64Ptr(2))
			image.Status.LatestBuildRef = buildName(5)
			image.Status.LatestImage = ""
			return newReconcileHandler(t, key, image, builds())
		}

		it("does not abort the other deletes, reports the error, and converges next round", func() {
			failing := buildName(1) // oldest successful build, due for deletion
			h := newHandler()
			h.failDeletes(failing, k8serrors.NewForbidden(
				schema.GroupResource{Group: "kpack.io", Resource: "builds"}, failing, errors.New("access denied")))

			require.Error(t, h.reconcile())
			assert.Contains(t, h.err.Error(), "access denied")

			// The other due deletion (failed build 4) still went through, and the
			// failed one is still present.
			assert.Contains(t, h.deleted, buildName(4))
			assert.NotContains(t, h.remaining(), buildName(4))
			assert.Contains(t, h.remaining(), failing)

			// Next round: stop failing, reconcile again, and the remaining excess
			// build is removed. A failed delete is not retried within one round,
			// so the first round attempted it exactly once.
			assert.Equal(t, 1, h.deleteAttempts(failing))

			h.allowDeletes()
			require.NoError(t, h.reconcile())

			assert.NotContains(t, h.remaining(), failing)
			assert.Equal(t, 2, h.deleteAttempts(failing)) // one per round
		})

		it("treats not-found and conflict as already handled", func() {
			h := newHandler()
			h.failDeletes(buildName(1), k8serrors.NewNotFound(
				schema.GroupResource{Group: "kpack.io", Resource: "builds"}, buildName(1)))
			h.failDeletes(buildName(4), k8serrors.NewConflict(
				schema.GroupResource{Group: "kpack.io", Resource: "builds"}, buildName(4), errors.New("uid changed")))

			require.NoError(t, h.reconcile())
		})
	})

	when("a large batch of builds expires at once", func() {
		it("deletes a bounded batch and requests a requeue", func() {
			// More successful builds than the per-reconcile cap, plus a running
			// newest build so that no new build is scheduled.
			successful := int64(maxBuildsDeletedPerReconcile) + 5
			var builds []*buildapi.Build
			for i := int64(1); i <= successful; i++ {
				builds = append(builds, plannedBuild(i, corev1.ConditionTrue, time.Duration(i)*time.Minute))
			}
			builds = append(builds, plannedBuild(
				successful+1,
				corev1.ConditionUnknown,
				time.Duration(successful+1)*time.Minute,
			))

			image := retentionImage(int64Ptr(1), int64Ptr(1))
			image.Status.LatestBuildRef = ""
			image.Status.LatestImage = ""

			h := reconcileOnce(t, key, image, builds)

			requeue, delay := controller.IsRequeueKey(h.err)
			require.True(t, requeue, "expected a requeue, got %v", h.err)
			assert.Equal(t, retentionRequeueDelay, delay)
			assert.Len(t, h.deleted, maxBuildsDeletedPerReconcile)

			// The oldest builds are the ones removed, so the next round can simply
			// continue from where this one stopped.
			assert.Equal(t, buildName(1), h.deleted[0])
			assert.Equal(t, buildName(int64(maxBuildsDeletedPerReconcile)), h.deleted[len(h.deleted)-1])
		})

		it("reports a delete failure rather than hiding it behind the requeue", func() {
			successful := int64(maxBuildsDeletedPerReconcile) + 5
			var builds []*buildapi.Build
			for i := int64(1); i <= successful; i++ {
				builds = append(builds, plannedBuild(i, corev1.ConditionTrue, time.Duration(i)*time.Minute))
			}
			builds = append(builds, plannedBuild(
				successful+1,
				corev1.ConditionUnknown,
				time.Duration(successful+1)*time.Minute,
			))

			image := retentionImage(int64Ptr(1), int64Ptr(1))
			image.Status.LatestBuildRef = ""
			image.Status.LatestImage = ""

			h := newReconcileHandler(t, key, image, builds)
			h.failDeletes(buildName(2), k8serrors.NewForbidden(
				schema.GroupResource{Group: "kpack.io", Resource: "builds"}, buildName(2), errors.New("access denied")))

			err := h.reconcile()

			// A real failure means the key is retried with backoff, and the part
			// above the cap is picked up by that retry.
			require.Error(t, err)
			assert.Contains(t, err.Error(), "access denied")
			requeue, _ := controller.IsRequeueKey(err)
			assert.False(t, requeue)
		})
	})
}

// reconcileHandler owns one in-memory client and records what a reconcile did.
type reconcileHandler struct {
	t       *testing.T
	key     string
	image   *buildapi.Image
	builds  []*buildapi.Build
	client  *fake.Clientset
	listers testhelpers.Listers
	err     error

	// deleted accumulates, across reconciles, the names whose delete actually
	// removed the object.
	deleted []string
}

func newReconcileHandler(t *testing.T, key string, image *buildapi.Image, builds []*buildapi.Build) *reconcileHandler {
	objects := make([]runtime.Object, 0, len(builds)+2)
	objects = append(objects, image, retentionBuilder())
	for _, build := range builds {
		objects = append(objects, build)
	}

	listers := testhelpers.NewListers(objects)

	return &reconcileHandler{
		t:       t,
		key:     key,
		image:   image,
		builds:  builds,
		client:  fake.NewSimpleClientset(listers.BuildServiceObjects()...),
		listers: listers,
	}
}

func reconcileOnce(t *testing.T, key string, image *buildapi.Image, builds []*buildapi.Build) *reconcileHandler {
	h := newReconcileHandler(t, key, image, builds)
	h.err = h.reconcile()
	return h
}

func (h *reconcileHandler) reconciler() *Reconciler {
	return &Reconciler{
		Client:               h.client,
		ImageLister:          h.listers.GetImageLister(),
		BuildLister:          h.listers.GetBuildLister(),
		DuckBuilderLister:    h.listers.GetDuckBuilderLister(),
		SourceResolverLister: h.listers.GetSourceResolverLister(),
		PvcLister:            h.listers.GetPersistentVolumeClaimLister(),
		Tracker:              &testhelpers.FakeTracker{},
		K8sClient:            k8sfake.NewSimpleClientset(h.listers.GetKubeObjects()...),
	}
}

func (h *reconcileHandler) reconcile() error {
	previousAttempts := len(h.deleteActions())

	err := h.reconciler().Reconcile(context.Background(), h.key)
	h.err = err

	// Record what this round actually removed, in the order the reconciler
	// issued the deletes. The fake client records an action even when a reactor
	// rejects it, so a name counts only if the object is gone afterwards.
	for _, name := range h.deleteActions()[previousAttempts:] {
		if !h.exists(name) {
			h.deleted = append(h.deleted, name)
		}
	}

	return err
}

// deleteActions returns the names of every Build delete the client was asked
// to perform, in order.
func (h *reconcileHandler) deleteActions() []string {
	var names []string
	for _, action := range h.client.Actions() {
		if action.GetVerb() != "delete" || action.GetResource().Resource != "builds" {
			continue
		}
		names = append(names, action.(clientgotesting.DeleteAction).GetName())
	}
	return names
}

func (h *reconcileHandler) exists(name string) bool {
	for _, build := range h.buildList() {
		if build.Name == name {
			return true
		}
	}
	return false
}

func (h *reconcileHandler) buildList() []buildapi.Build {
	list, err := h.client.KpackV1alpha2().Builds(retentionNamespace).List(
		context.Background(), metav1.ListOptions{})
	require.NoError(h.t, err)
	return list.Items
}

func (h *reconcileHandler) remaining() []string {
	var names []string
	for _, build := range h.buildList() {
		names = append(names, build.Name)
	}
	sort.Slice(names, func(i, j int) bool { return nameOrder(names[i], names[j]) })
	return names
}

// nameOrder orders "<prefix>-build-<n>" names by n so assertions read naturally
// (build-9 before build-10).
func nameOrder(a, b string) bool {
	an, aok := numberFromBuildName(a)
	bn, bok := numberFromBuildName(b)
	if aok && bok && an != bn {
		return an < bn
	}
	return a < b
}

func numberFromBuildName(name string) (int64, bool) {
	const infix = "-build-"
	i := strings.LastIndex(name, infix)
	if i < 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(name[i+len(infix):], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (h *reconcileHandler) remainingBuilds() []*buildapi.Build {
	items := h.buildList()
	builds := make([]*buildapi.Build, 0, len(items))
	for i := range items {
		build := items[i]
		builds = append(builds, &build)
	}
	return builds
}

func (h *reconcileHandler) deleteAttempts(name string) int {
	attempts := 0
	for _, attempted := range h.deleteActions() {
		if attempted == name {
			attempts++
		}
	}
	return attempts
}

func (h *reconcileHandler) failDeletes(name string, err error) {
	h.client.PrependReactor("delete", "builds", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		if action.(clientgotesting.DeleteAction).GetName() == name {
			return true, nil, err
		}
		return false, nil, nil
	})
}

// allowDeletes removes any failure injection added with failDeletes. The
// reconcile creates a fresh Kubernetes client per call, but the Kpack client is
// shared, so its reaction chain is cleared here.
func (h *reconcileHandler) allowDeletes() {
	h.client.ReactionChain = nil
	h.client.AddReactor("delete", "builds", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return false, nil, nil
	})
}

// retentionImage returns an Image whose stored status already reflects a
// running newest build, so a reconcile only performs retention. Callers that
// want no status-based protection clear LatestBuildRef and LatestImage.
func retentionImage(failedLimit, successLimit *int64) *buildapi.Image {
	newest := buildName(1000)
	return &buildapi.Image{
		ObjectMeta: metav1.ObjectMeta{
			Name:       retentionImageName,
			Namespace:  retentionNamespace,
			Generation: 1,
		},
		Spec: buildapi.ImageSpec{
			Tag:                      retentionImageTag,
			Builder:                  corev1.ObjectReference{Kind: buildapi.BuilderKind, Name: "test-builder"},
			ServiceAccountName:       "service-account",
			FailedBuildHistoryLimit:  failedLimit,
			SuccessBuildHistoryLimit: successLimit,
			ImageTaggingStrategy:     corev1alpha1.None,
			Build:                    &buildapi.ImageBuild{},
			Source: corev1alpha1.SourceConfig{
				Git: &corev1alpha1.Git{URL: "https://some.git/url", Revision: "1234567"},
			},
		},
		Status: buildapi.ImageStatus{
			Status: corev1alpha1.Status{
				ObservedGeneration: 1,
				Conditions: corev1alpha1.Conditions{
					{
						Type:    corev1alpha1.ConditionReady,
						Status:  corev1.ConditionUnknown,
						Reason:  BuildRunningReason,
						Message: "Build '" + newest + "' is executing",
					},
					{
						Type:   buildapi.ConditionBuilderReady,
						Status: corev1.ConditionTrue,
						Reason: buildapi.BuilderReady,
					},
					{
						Type:   buildapi.ConditionBuilderUpToDate,
						Status: corev1.ConditionTrue,
						Reason: buildapi.BuilderUpToDate,
					},
				},
			},
			LatestBuildRef: newest,
			BuildCounter:   1000,
		},
	}
}

func retentionBuilder() *buildapi.Builder {
	return &buildapi.Builder{
		ObjectMeta: metav1.ObjectMeta{Name: "test-builder", Namespace: retentionNamespace},
		TypeMeta: metav1.TypeMeta{
			Kind:       buildapi.BuilderKind,
			APIVersion: "kpack.io/v1alpha2",
		},
		Status: buildapi.BuilderStatus{
			LatestImage: "some/builder@sha256:acf123",
			BuilderMetadata: corev1alpha1.BuildpackMetadataList{
				{Id: "buildpack.version", Version: "version"},
			},
			Stack: corev1alpha1.BuildStack{
				RunImage: "some/run@sha256:67e3de2af270bf09c02e9a644aeb7e87e6b3c049abe6766bf6b6c3728a83e7fb",
				ID:       "io.buildpacks.stacks.bionic",
			},
			Lifecycle: buildapi.ResolvedClusterLifecycle{Version: "some-version"},
			Status: corev1alpha1.Status{
				Conditions: corev1alpha1.Conditions{
					{Type: corev1alpha1.ConditionReady, Status: corev1.ConditionTrue},
					{Type: buildapi.ConditionUpToDate, Status: corev1.ConditionTrue},
				},
			},
		},
	}
}
