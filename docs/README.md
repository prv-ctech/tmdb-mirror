# Documentation map

Current operator and implementation contracts:

- [`README.md`](../README.md): stack overview and quick start.
- [`api.md`](api.md): public, admin, and media HTTP contracts.
- [`deployment-production.md`](deployment-production.md): Compose, environment,
  permissions, worker startup, and media layout.
- [`backup-recovery.md`](backup-recovery.md): scheduled/manual pgBackRest and
  offline PITR.
- [`stress-testing.md`](stress-testing.md): isolated Docker verification.
- [`release.md`](release.md): image and digest-pinned release publication.

Superpowers-generated planning records are local agent artifacts and are not
tracked. Current runtime facts come from code, SQLx migrations, Compose files,
and scripts; when those change, update the active documents in the same change.
