# OAF workflow runtime candidate — retry lease fix

Apply **on top of** the workflow-runtime-candidate branch. This is a focused correction, **not** the completed OAF release.

* Adds fenced `StepRegistry.release()` for failed handlers. Release is a no-op if another worker has taken ownership or the step completed.
* Releases a claim on ordinary handler exceptions so a reclaimed message can retry without waiting for lease expiry.
* Stops step renewal once terminal commit begins.

Still required: migrate old unit tests to explicit step-registry fixtures, replace direct commit tests with fenced workflow commits, implement deterministic trigger publishing, and add full failure-injection tests. The candidate should not be committed or used in production yet.
