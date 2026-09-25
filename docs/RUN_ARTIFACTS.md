# Generated run artifacts

Generated research artifacts should not clutter the repository root.

Default location:

```text
runs/
```

The following v6 tools now place their automatically named outputs there:

- `maker_bbo_probe`
- `maker_transport_probe`
- `maker_window_ab`
- `maker_paper_45to30`
- future dedicated experiment/diagnostic runners should use `jevymarket.run_paths.run_output_path`

This includes experiment databases, their SQLite sidecars/lock files, and compressed reports.

Explicit paths remain authoritative. If an operator supplies `--db` or `--out`, that exact location is used and its parent directory is created if needed.

Long-lived project files, source code, `.env`, configuration, and the existing main Maker database are not moved by this convention.

`/runs/` is ignored by Git and is local research state. Deleting it removes local run history but does not change source code.
