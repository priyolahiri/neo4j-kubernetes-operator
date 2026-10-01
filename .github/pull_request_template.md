## Summary

<!-- What does this PR change, and why? Link any issue: Closes #123 -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation
- [ ] Refactor / chore / CI

## Testing

<!-- How did you verify this? -->

- [ ] `make test-unit` passes locally
- [ ] `make lint` passes locally
- [ ] Core Integration Tests are green (they run automatically when a PR touches
      runtime paths such as `internal/`, `api/`, `cmd/`, `config/`)
- [ ] Extended Integration Tests dispatched against this branch (Actions →
      "Extended Integration Tests" → Run workflow, or
      `gh workflow run integration-tests.yml --ref <branch>`) — required if you
      changed the cluster, standalone, backup, restore, or sharding controllers.
      There is no PR-label or commit-message trigger for it.

## Checklist

- [ ] Conventional Commit title (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, `ci:`)
- [ ] Ran `make sync-all` and committed regenerated artifacts if I changed Go API
      types, kubebuilder/RBAC markers, or CRDs (CI's `check-drift` gate fails otherwise)
- [ ] Updated docs (API reference / user guides) if behavior or fields changed
- [ ] Respected the project invariants in `CLAUDE.md` (no admission webhooks,
      KIND-only dev, V2 discovery, server-based architecture, …)
