**Problem:**

The `Image` API has two fields, `spec.successBuildHistoryLimit` and `spec.failedBuildHistoryLimit`, documented as "the maximum number of successful/failed builds for an image that will be retained". The controller implemented them as a cap on a count, pruned one build per reconcile, sorted by `metadata.creationTimestamp`, and ignored every other reason to keep a `Build`. That leaves four problems:

1. **It deletes things it must not.** Nothing stops the controller from deleting a `Build` that is still executing, the `Build` named by `status.latestBuildRef` (which `kp build logs <image>` resolves), or the successful `Build` that produced `status.latestImage` when the newest build failed.
2. **"Newest" is not well defined.** `sort.Sort` was called on a `Less` that returns `true` for equal timestamps, which is an invalid strict weak ordering. Builds can share a second, the cluster clock can move, and informer list order is not stable across restarts, so which build is "the oldest" could change between reconciles.
3. **Pruning is unbounded and fragile.** One delete per reconcile, and any delete error aborted the round, including the error that the object was already gone.
4. **Zero is undefined.** Admission rejects limits below 1, but the controller read the pointer directly, so anything that bypassed admission (`nil`, `0`) would panic or delete everything.

**Outcome:**

- Retention is defined over finished `Build`s of each outcome independently, and by build number rather than wall-clock time.
- In-progress `Build`s and `Build`s the `Image` still references are never deleted, even above the configured limits.
- A round of pruning deletes a bounded batch, tolerates individual failures, distinguishes "already gone"/"changed" from real errors, and always converges.
- The zero/absent case follows the documented API contract: values below 1 are rejected by admission, absent defaults to 10, and the controller applies the same default instead of inventing a meaning for 0.
- All behaviour is covered by tests against the in-memory client.

**Actions to take:**

**Which builds are kept.** For each outcome, keep the newest `limit` builds. On top of that, never delete:

- a `Build` whose `Succeeded` condition is `Unknown`, or that has no `Succeeded` condition at all (kpack treats a `Build` without the condition as not yet started);
- the `Build` named by `image.status.latestBuildRef`;
- the successful `Build` whose `status.latestImage` equals `image.status.latestImage`.

These protections are additive: a protected build is kept in addition to the limit rather than consuming a slot in it, so an image can temporarily hold slightly more than its configured history. Keeping the limits exact would require deleting a build that is executing or still advertised, which is not acceptable.

The complete set of places kpack refers to a `Build` by name was audited:

| Reference | Where | Protected? |
| --- | --- | --- |
| `image.status.latestBuildRef` | `pkg/logs/wait_for_image.go` (`kp build logs`, `kp image status`), the Ready condition message | Yes |
| the image kpack reports as current (`status.latestImage`), produced by some successful `Build` | `Image.LatestForImage` | Yes |
| `kpack.io/build` label on the build's pod | `pkg/apis/build/v1alpha2/build_pod.go` | Not needed: the pod is owned by the `Build` and is garbage collected with it |
| `Spec.LastBuild.Image` on a newer `Build` | build pod generation | Not needed: it stores an image reference, not a build name |
| SLSA provenance subject `https://kpack.io/<ns>/<build>/...` | `pkg/slsa/attest.go` | Not applicable: attestations are pushed to the registry, not looked up by build name |

**Stable ordering.** Builds are ordered by `image.kpack.io/buildNumber`. Build numbers are assigned by the image controller from `status.buildCounter` and increase monotonically for the lifetime of the `Image`, so they are a stable, cheap stand-in for creation order that does not depend on the clock or on list order. If the label is missing (older objects), the number is parsed from the `<image>-build-<n>` name. Only if neither is present does the ordering fall back to creation timestamp, then name, then UID, all deterministic. Unnumbered builds sort before numbered ones, so an unnumbered build can never masquerade as the most recent build (which drives scheduling and `LastBuild`).

Note that a manual `Build` created with `kubectl` and no `image.kpack.io/image` label is not selected at all, since selection is by that label; only builds the `Image` owns are considered.

**Zero means neither.** `ImageSpec.validateBuildHistoryLimit` rejects any value below 1 with "build history limit must be greater than 0", and `Image.SetDefaults` fills in 10 when the field is nil. Therefore:

- **Absent (nil)** → 10, the documented default. This already happens in admission; the controller applies it again only for objects that bypassed the webhook.
- **Zero or negative** → also 10. Zero is not the API's spelling of "prune everything" (it is invalid), and treating a malformed value as "keep nothing" would be silently destructive. Falling back to the documented default is the least surprising behaviour and can never delete a day's history because someone wrote a bad number.
- **"Unlimited" is not expressible.** There is no sentinel for "keep everything"; an operator who wants that sets a very large value. Adding a sentinel would change the API contract and is out of scope.

**Builds that never started.** A build that fails before any container runs (source fetch failure, or a builder that became unready between scheduling and execution) still ends with `Succeeded=False` and is therefore a failed build by the existing definition. It is counted against `failedBuildHistoryLimit` and pruned like any other failed build. The alternative — excluding never-started builds — was rejected for two reasons:

- It cannot be decided reliably from the `Build` object. There is no condition or step-state marking "never started": a build whose prepare step failed to pull source and a build whose prepare step crashed after cloning both end as `Succeeded=False` with the same shape. Any heuristic would misclassify real failures.
- The scenario the exclusion would guard against does not occur. `isBuildRequired` in `pkg/reconciler/image/build_required.go` returns `ConditionStatus=Unknown` without creating a `Build` whenever the `SourceResolver` or builder is not ready. A failed source fetch is reported on the `Image` status ("SourceResolver is not ready"), not as a stream of failed `Build`s. During a builder outage kpack holds the image at `BuilderNotReady` rather than churning builds.

Counting them as failures is also the safer default: a build that did run and fail and a build that never ran both occupy a history slot, and the failed limit (10 by default) comfortably absorbs either.

**Where cleanup runs.** Cleanup is part of the image reconcile, after the status update, rather than a separate periodic cleaner.

| | In the image reconcile (chosen) | Separate periodic cleaner |
| --- | --- | --- |
| Cost | One namespaced `Build` list per reconcile. The reconcile already lists builds to decide whether to build, so the snapshot is reused and the additional cost is a label-filtered in-memory list. | Same list, but on a timer, plus a new controller, its own informer event wiring, and its own error handling. |
| Latency | Pruning follows an image change immediately. | History can exceed the limit until the next tick; the over-limit window is the tick period. |
| Correctness | Prunes in the same snapshot it just used, so it cannot race a build decision made from a different view. | Delegates reconciling the same objects while the image reconcile is running. |
| Scale | A reconcile per image change; an image with thousands of builds pays a sort of thousands of pointers. Sorting 10k pointers is microseconds and listing is already required. | Trades the per-reconcile list for one pass over all images, at the cost of the new component. |

The per-reconcile list already existed and is needed to pick the last build, so the marginal cost of retention is a sort and a filter over data that is already in memory. With 1,000 builds the additional work is one allocation of a 1,000-element slice and an `O(n log n)` sort, which is negligible next to the registry and pod operations a reconcile performs. A separate cleaner would be justified only if the list itself (not retention) were too expensive, which is not the case here.

**Bounded, failure-tolerant deletes.** At most 20 builds are deleted per reconcile. If more remain, the reconcile returns `controller.NewRequeueAfter(10s)`; if any delete failed, it returns an aggregate error so the controller backs the key off and retries. The next round re-lists from the informer and re-plans, so it never retries the same object within a round and does not depend on informer events having arrived.

Delete outcomes are classified:

- success → the build is gone;
- `NotFound` / `Gone` → already deleted by someone else, nothing to do;
- `Conflict` (including a failed UID precondition) → the object changed between list and delete, so this round's decision no longer applies; re-evaluate next round;
- anything else (forbidden, unauthorized, server errors) → recorded and aggregated; the remaining deletes still run.

**Alternatives:**

- **Delete by `DeleteCollection` with a label selector.** Cannot express "keep the newest N", would need `fieldSelector` support that custom resources do not have, and would not distinguish protected builds.
- **Sort by creation timestamp only.** Simpler, but the existing sort was not even a valid ordering, and timestamps are not unique or monotonic across a clock change. Build numbers are already present and stable.
- **Store a list of retained build names in `Image.status`.** Makes "which are retained" explicit and removes the need to derive it, but adds a status field and a write on every build, and can drift from the actual objects. Deriving from the builds themselves keeps the single source of truth.

**Risks:**

- An image with a very long history will be above its limit for a few requeue cycles while the batches drain. This is visible in the build list but bounded and self-correcting.
- A build that is protected indefinitely (for example a `latestBuildRef` that is never updated because the image stops being reconciled) is never pruned. This is intended: the reference is live, and clearing it is the responsibility of the reconcile that moves the image forward.
- The 20-per-round cap is a constant, not configurable. It is generous for normal limits (10 is the default) and only throttles pathological histories.
