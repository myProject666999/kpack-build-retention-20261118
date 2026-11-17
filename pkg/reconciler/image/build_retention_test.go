package image

import (
	"strconv"
	"testing"
	"time"

	"github.com/sclevine/spec"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	buildapi "github.com/pivotal/kpack/pkg/apis/build/v1alpha2"
	corev1alpha1 "github.com/pivotal/kpack/pkg/apis/core/v1alpha1"
)

func TestBuildRetentionPlanning(t *testing.T) {
	spec.Run(t, "Build retention planning", testBuildRetentionPlanning)
}

func testBuildRetentionPlanning(t *testing.T, when spec.G, it spec.S) {
	const (
		imageName = "image-name"
		namespace = "some-namespace"
	)

	var image *buildapi.Image

	it.Before(func() {
		image = &buildapi.Image{
			ObjectMeta: metav1.ObjectMeta{Name: imageName, Namespace: namespace},
			Spec: buildapi.ImageSpec{
				Tag:                      retentionImageTag,
				FailedBuildHistoryLimit:  int64Ptr(2),
				SuccessBuildHistoryLimit: int64Ptr(2),
			},
		}
	})

	when("ordering builds", func() {
		it("orders by build number rather than creation timestamp", func() {
			// Timestamps deliberately run backwards relative to build number.
			builds := []*buildapi.Build{
				plannedBuild(1, corev1.ConditionTrue, 10*time.Hour),
				plannedBuild(2, corev1.ConditionTrue, 5*time.Hour),
				plannedBuild(3, corev1.ConditionTrue, time.Hour),
				plannedBuild(4, corev1.ConditionTrue, time.Minute),
			}

			list := newBuildList(builds)

			assert.Equal(t, []string{
				buildName(1), buildName(2), buildName(3), buildName(4),
			}, buildNames(list.all))
		})

		it("is stable for builds that share a creation timestamp", func() {
			// No distinct timestamps at all: build number must decide. Input order
			// is reversed to prove the result does not depend on listing order.
			builds := []*buildapi.Build{
				plannedBuild(5, corev1.ConditionTrue, 0),
				plannedBuild(4, corev1.ConditionTrue, 0),
				plannedBuild(3, corev1.ConditionTrue, 0),
				plannedBuild(2, corev1.ConditionTrue, 0),
				plannedBuild(1, corev1.ConditionTrue, 0),
			}

			list := newBuildList(builds)

			assert.Equal(t, []string{
				buildName(1), buildName(2), buildName(3), buildName(4), buildName(5),
			}, buildNames(list.all))
		})

		it("falls back to the name suffix when the build number label is absent", func() {
			// Older objects created before the label was always populated.
			builds := []*buildapi.Build{
				unlabelledBuild(3, 0),
				unlabelledBuild(1, 0),
				unlabelledBuild(2, 0),
			}

			list := newBuildList(builds)

			assert.Equal(t, []string{
				buildName(1), buildName(2), buildName(3),
			}, buildNames(list.all))
		})

		it("reads the number from the legacy v1alpha1 name shape", func() {
			legacy := func(number int64) *buildapi.Build {
				build := plannedBuild(number, corev1.ConditionTrue, 0)
				delete(build.Labels, buildapi.BuildNumberLabel)
				build.Name = retentionImageName + "-build-" + strconv.FormatInt(number, 10) + "-"
				return build
			}

			list := newBuildList([]*buildapi.Build{legacy(2), legacy(1)})

			assert.Equal(t, "image-name-build-1-", list.all[0].Name)
			assert.Equal(t, "image-name-build-2-", list.all[1].Name)
		})

		it("does not read a number from a name whose suffix is not digits", func() {
			build := plannedBuild(1, corev1.ConditionTrue, 0)
			delete(build.Labels, buildapi.BuildNumberLabel)
			build.Name = retentionImageName + "-build-latest"

			_, numbered := buildNumber(build)

			assert.False(t, numbered)
		})

		it("places builds with no usable number first, deterministically", func() {
			unnumberedA := &buildapi.Build{
				ObjectMeta: metav1.ObjectMeta{Name: "manual-a", Namespace: namespace},
				Status:     succeededStatus(corev1.ConditionTrue),
			}
			unnumberedB := &buildapi.Build{
				ObjectMeta: metav1.ObjectMeta{Name: "manual-b", Namespace: namespace},
				Status:     succeededStatus(corev1.ConditionTrue),
			}

			list := newBuildList([]*buildapi.Build{
				unnumberedB,
				plannedBuild(1, corev1.ConditionTrue, 0),
				unnumberedA,
			})

			assert.Equal(t, []string{"manual-a", "manual-b", buildName(1)}, buildNames(list.all))
		})
	})

	when("planning deletions", func() {
		it("keeps exactly the newest builds of each outcome for a dozen mixed builds", func() {
			// Successful: 1,3,5,7,9,11. Failed: 2,4,6,8,10,12. Running: 13.
			builds := mixedBuilds()

			// The image still refers to the failed build 12 (latestBuildRef) and
			// advertises the image produced by successful build 11 (latestImage).
			image.Status.LatestBuildRef = buildName(12)
			image.Status.LatestImage = retentionImageTag + "@sha256:build-11"

			plan := buildNames(newBuildList(builds).retentionPlan(image))

			// Limits of 2 per outcome keep successful {9,11} and failed {10,12};
			// everything else that is finished and unprotected goes, oldest first.
			assert.Equal(t, []string{
				buildName(1), buildName(2), buildName(3), buildName(4),
				buildName(5), buildName(6), buildName(7), buildName(8),
			}, plan)
		})

		it("never prunes a running build even when it is the oldest and the limits are one", func() {
			image.Spec.SuccessBuildHistoryLimit = int64Ptr(1)
			image.Spec.FailedBuildHistoryLimit = int64Ptr(1)

			builds := []*buildapi.Build{
				plannedBuild(1, corev1.ConditionUnknown, 0), // oldest, still running
				plannedBuild(2, corev1.ConditionTrue, time.Minute),
				plannedBuild(3, corev1.ConditionTrue, 2*time.Minute),
				plannedBuild(4, corev1.ConditionFalse, 3*time.Minute),
				plannedBuild(5, corev1.ConditionFalse, 4*time.Minute),
			}

			plan := buildNames(newBuildList(builds).retentionPlan(image))

			assert.NotContains(t, plan, buildName(1))
			assert.NotContains(t, plan, buildName(3))
			assert.NotContains(t, plan, buildName(5))
			assert.Equal(t, []string{buildName(2), buildName(4)}, plan)
		})

		it("protects the build named by status.latestBuildRef", func() {
			image.Status.LatestBuildRef = buildName(1)

			builds := []*buildapi.Build{
				plannedBuild(1, corev1.ConditionTrue, 0),
				plannedBuild(2, corev1.ConditionTrue, time.Minute),
				plannedBuild(3, corev1.ConditionTrue, 2*time.Minute),
				plannedBuild(4, corev1.ConditionTrue, 3*time.Minute),
			}

			plan := buildNames(newBuildList(builds).retentionPlan(image))

			// Protection is additive: builds 3 and 4 fill the limit of 2 and
			// build 1 is kept on top because status references it.
			assert.Equal(t, []string{buildName(2)}, plan)
		})

		it("protects the build that produced the image the image still advertises", func() {
			// The newest build failed, so latestImage remains the output of 2.
			image.Status.LatestBuildRef = buildName(4)
			image.Status.LatestImage = retentionImageTag + "@sha256:build-2"

			builds := []*buildapi.Build{
				plannedBuild(1, corev1.ConditionTrue, 0),
				plannedBuild(2, corev1.ConditionTrue, time.Minute),
				plannedBuild(3, corev1.ConditionTrue, 2*time.Minute),
				plannedBuild(4, corev1.ConditionFalse, 3*time.Minute),
			}

			plan := buildNames(newBuildList(builds).retentionPlan(image))

			assert.Equal(t, []string{buildName(1)}, plan)
		})

		it("deletes nothing when the history is within the limits", func() {
			builds := []*buildapi.Build{
				plannedBuild(1, corev1.ConditionTrue, 0),
				plannedBuild(2, corev1.ConditionTrue, time.Minute),
				plannedBuild(3, corev1.ConditionFalse, 2*time.Minute),
				plannedBuild(4, corev1.ConditionFalse, 3*time.Minute),
			}

			assert.Empty(t, newBuildList(builds).retentionPlan(image))
		})

		it("prunes down to a lowered limit and then stops deleting", func() {
			image.Spec.SuccessBuildHistoryLimit = int64Ptr(2)

			builds := make([]*buildapi.Build, 0, 10)
			for i := 1; i <= 10; i++ {
				builds = append(builds, plannedBuild(int64(i), corev1.ConditionTrue, time.Duration(i)*time.Minute))
			}

			plan := buildNames(newBuildList(builds).retentionPlan(image))

			assert.Equal(t, []string{
				buildName(1), buildName(2), buildName(3), buildName(4),
				buildName(5), buildName(6), buildName(7), buildName(8),
			}, plan)
		})

		it("deletes nothing when the limit is raised again, and resolves no deleted object", func() {
			image.Spec.SuccessBuildHistoryLimit = int64Ptr(10)

			// Represents the state after the previous reconcile removed 1..8.
			builds := []*buildapi.Build{
				plannedBuild(9, corev1.ConditionTrue, 9*time.Minute),
				plannedBuild(10, corev1.ConditionTrue, 10*time.Minute),
			}

			assert.Empty(t, newBuildList(builds).retentionPlan(image))
		})

		it("treats a build with no Succeeded condition as running", func() {
			unknown := &buildapi.Build{
				ObjectMeta: metav1.ObjectMeta{
					Name:      buildName(1),
					Namespace: namespace,
					Labels:    map[string]string{buildapi.BuildNumberLabel: "1"},
				},
			}

			list := newBuildList([]*buildapi.Build{unknown})

			assert.Len(t, list.running, 1)
			assert.Empty(t, list.retentionPlan(image))
		})
	})

	when("a dozen mixed builds", func() {
		it("retains exactly the expected set when the limit is lowered", func() {
			// 1,3,5,7,9,11 successful; 2,4,6,8,10,12 failed; 13 running. The
			// image references the newest failed build (12) and advertises the
			// output of successful build 11.
			image.Status.LatestBuildRef = buildName(12)
			image.Status.LatestImage = retentionImageTag + "@sha256:build-11"

			builds := mixedBuilds()

			list := newBuildList(builds)
			plan := buildNames(list.retentionPlan(image))

			kept := map[string]bool{}
			for _, build := range list.all {
				kept[build.Name] = true
			}
			for _, name := range plan {
				delete(kept, name)
			}

			var retained []string
			for _, build := range list.all {
				if kept[build.Name] {
					retained = append(retained, build.Name)
				}
			}

			// Limits of 2 keep the newest two of each outcome (9,11 and 10,12);
			// the running build 13 and the two status references are additive.
			assert.Equal(t, []string{
				buildName(9), buildName(10), buildName(11), buildName(12), buildName(13),
			}, retained)
			assert.Len(t, plan, 8) // the other eight finished builds go
		})
	})

	when("resolving the configured limit", func() {
		it("uses a positive configured value", func() {
			assert.Equal(t, 2, resolveBuildHistoryLimit(int64Ptr(2)))
		})

		it("uses the admission default when the field is absent", func() {
			assert.Equal(t, int(defaultBuildHistoryLimit), resolveBuildHistoryLimit(nil))
		})

		it("uses the admission default rather than deleting everything for zero or negative values", func() {
			// ImageSpec.validateBuildHistoryLimit rejects values below 1, so this
			// is a defence for objects that bypassed admission. Zero is not the
			// API's spelling of "delete all".
			assert.Equal(t, int(defaultBuildHistoryLimit), resolveBuildHistoryLimit(int64Ptr(0)))
			assert.Equal(t, int(defaultBuildHistoryLimit), resolveBuildHistoryLimit(int64Ptr(-1)))
		})
	})
}

func int64Ptr(i int64) *int64 {
	return &i
}

const (
	retentionImageName = "image-name"
	retentionNamespace = "some-namespace"
	retentionImageTag  = "some/image"
)

func buildName(n int64) string {
	return retentionImageName + "-build-" + strconv.FormatInt(n, 10)
}

func plannedBuild(number int64, status corev1.ConditionStatus, age time.Duration) *buildapi.Build {
	return &buildapi.Build{
		ObjectMeta: metav1.ObjectMeta{
			Name:      buildName(number),
			Namespace: retentionNamespace,
			UID:       types.UID("uid-" + buildName(number)),
			Labels: map[string]string{
				buildapi.BuildNumberLabel: strconv.FormatInt(number, 10),
				buildapi.ImageLabel:       retentionImageName,
			},
			CreationTimestamp: metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(age)),
		},
		Status: succeededStatus(status),
	}
}

func unlabelledBuild(number int64, age time.Duration) *buildapi.Build {
	build := plannedBuild(number, corev1.ConditionTrue, age)
	delete(build.Labels, buildapi.BuildNumberLabel)
	return build
}

func succeededStatus(status corev1.ConditionStatus) buildapi.BuildStatus {
	return buildapi.BuildStatus{
		Status: corev1alpha1.Status{
			Conditions: corev1alpha1.Conditions{
				{Type: corev1alpha1.ConditionSucceeded, Status: status},
			},
		},
	}
}

func mixedBuilds() []*buildapi.Build {
	builds := make([]*buildapi.Build, 0, 13)
	for i := int64(1); i <= 12; i++ {
		status := corev1.ConditionTrue
		if i%2 == 0 {
			status = corev1.ConditionFalse
		}
		build := plannedBuild(i, status, time.Duration(i)*time.Minute)
		if status == corev1.ConditionTrue {
			// Give each successful build a distinct output image so a status
			// image reference can identify exactly one of them.
			build.Status.LatestImage = retentionImageTag + "@sha256:build-" + strconv.FormatInt(i, 10)
		}
		builds = append(builds, build)
	}
	// The newest build is still executing.
	builds = append(builds, plannedBuild(13, corev1.ConditionUnknown, 13*time.Minute))

	return builds
}

func buildNames(builds []*buildapi.Build) []string {
	names := make([]string, 0, len(builds))
	for _, build := range builds {
		names = append(names, build.Name)
	}
	return names
}
