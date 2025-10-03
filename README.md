# syft-data-science

New implementation of [syft-rds](https://github.com/OpenMined/syft-rds)

- Separate `syft-datasets` package to manage Syft datasets
- `syft-runtimes` as a general folder-based job runner
- New `syft-rds` package that contains only a thin layer for a DS to submit jobs / runtimes creation, DOs to review and accept a job / accept to build a runtime
