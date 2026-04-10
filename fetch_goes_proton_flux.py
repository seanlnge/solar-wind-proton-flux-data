"""
Download GOES-18 SGPS L2 1-minute NetCDF files from NOAA NGDC and convert to CSV.

Date range: 2022-10-01 inclusive through 2025-12-31 inclusive (end before 2026-01-01).
"""

from __future__ import annotations

import argparse
import sys
import warnings
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

BASE = (
    "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/"
    "goes/goes18/l2/data/sgps-l2-avg1m/{y:04d}/{m:02d}/"
    "sci_sgps-l2-avg1m_g18_d{ymd}_{ver}.nc"
)
# NOAA bumps the product revision (v3-0-0, v3-0-1, …) over time; try 404 → next patch.
DEFAULT_MAX_PATCH = 32


def _scalar_attr(ds: xr.Dataset, name: str) -> float | int | None:
    if name not in ds:
        return None
    v = ds[name].values
    if v.shape == ():
        return v.item()
    return None


def dataset_to_csv(ds: xr.Dataset, csv_path: Path) -> None:
    """Write one tidy CSV per file: differential, integral, and alpha blocks stacked."""
    tmeta = ds[["L1bRecordsInAvg", "yaw_flip_flag"]].to_dataframe().reset_index()

    diff_names = [
        v
        for v in ds.data_vars
        if ds[v].dims == ("time", "sensor_units", "diff_channels")
    ]
    diff_df = ds[diff_names].to_dataframe().reset_index()
    for e in (
        "DiffProtonLowerEnergy",
        "DiffProtonUpperEnergy",
        "DiffProtonEffectiveEnergy",
    ):
        e_df = ds[[e]].to_dataframe().reset_index()
        diff_df = diff_df.merge(e_df, on=["sensor_units", "diff_channels"], how="left")
    diff_df = diff_df.merge(tmeta, on="time", how="left")
    diff_df["record_type"] = "differential_proton"

    int_names = [
        v for v in ds.data_vars if ds[v].dims == ("time", "sensor_units")
    ]
    int_df = ds[int_names].to_dataframe().reset_index()
    int_df = int_df.merge(
        ds[["IntegralProtonEffectiveEnergy"]].to_dataframe().reset_index(),
        on="sensor_units",
        how="left",
    )
    int_df = int_df.merge(tmeta, on="time", how="left")
    int_df["record_type"] = "integral_proton"

    alpha_names = [
        v
        for v in ds.data_vars
        if ds[v].dims == ("time", "sensor_units", "diff_alpha_channels")
    ]
    alpha_df = ds[alpha_names].to_dataframe().reset_index()
    for e in (
        "DiffAlphaLowerEnergy",
        "DiffAlphaUpperEnergy",
        "DiffAlphaEffectiveEnergy",
    ):
        e_df = ds[[e]].to_dataframe().reset_index()
        alpha_df = alpha_df.merge(
            e_df, on=["sensor_units", "diff_alpha_channels"], how="left"
        )
    alpha_df = alpha_df.merge(tmeta, on="time", how="left")
    alpha_df["record_type"] = "differential_alpha"

    lut = _scalar_attr(ds, "ExpectedLUTNotFound")
    for d in (diff_df, int_df, alpha_df):
        d["ExpectedLUTNotFound"] = lut

    all_cols = sorted(set(diff_df.columns) | set(int_df.columns) | set(alpha_df.columns))
    frames = [
        diff_df.reindex(columns=all_cols),
        int_df.reindex(columns=all_cols),
        alpha_df.reindex(columns=all_cols),
    ]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        out = pd.concat(frames, ignore_index=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(csv_path, index=False)


def nc_to_csv(nc_path: Path, csv_path: Path) -> None:
    with xr.open_dataset(nc_path) as ds:
        dataset_to_csv(ds, csv_path)


def url_for_day(d: date, ver: str) -> str:
    ymd = f"{d.year:04d}{d.month:02d}{d.day:02d}"
    return BASE.format(y=d.year, m=d.month, ymd=ymd, ver=ver)


def ymd_str(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def csv_any_exists_for_day(out_dir: Path, ymd: str) -> bool:
    return any(out_dir.glob(f"sci_sgps-l2-avg1m_g18_d{ymd}_v*.csv"))


def download_day_try_versions(
    d: date,
    nc_dir: Path,
    max_patch: int,
) -> tuple[Path, str] | None:
    """
    Try v3-0-0 … v3-0-{max_patch}; on 404 bump patch until one URL succeeds.
    Returns (nc_path, ver) or None if all failed.
    """
    ymd = ymd_str(d)
    for patch in range(max_patch + 1):
        ver = f"v3-0-{patch}"
        url = url_for_day(d, ver)
        nc_path = nc_dir / f"sci_sgps-l2-avg1m_g18_d{ymd}_{ver}.nc"
        if download(url, nc_path):
            return nc_path, ver
    return None


def daterange(start: date, end: date):
    """Yield each calendar day where start <= day < end."""
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(days=1)


def download(url: str, dest: Path, timeout: int = 120) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "scipre-fetch-goes/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return False
            dest.write_bytes(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise
    except urllib.error.URLError:
        raise
    return True


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "proton_flux_data",
        help="Directory for output CSV files",
    )
    p.add_argument(
        "--start",
        default="2022-10-01",
        help="Start date (inclusive), YYYY-MM-DD",
    )
    p.add_argument(
        "--end",
        default="2026-01-01",
        help="End date (exclusive), YYYY-MM-DD",
    )
    p.add_argument(
        "--keep-nc",
        action="store_true",
        help="Keep downloaded .nc files under out-dir/_nc",
    )
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip days that already have a CSV in out-dir (any v3-0-* name)",
    )
    p.add_argument(
        "--max-patch",
        type=int,
        default=DEFAULT_MAX_PATCH,
        metavar="N",
        help=f"Max v3-0-N patch to try (default {DEFAULT_MAX_PATCH})",
    )
    args = p.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end <= start:
        print("error: --end must be after --start", file=sys.stderr)
        return 1

    out_dir: Path = args.out_dir
    nc_dir = out_dir / "_nc" if args.keep_nc else out_dir / ".nc_tmp"

    ok, missing, failed = 0, 0, 0
    for d in daterange(start, end):
        ymd = ymd_str(d)
        if args.skip_existing and csv_any_exists_for_day(out_dir, ymd):
            ok += 1
            continue

        got = download_day_try_versions(d, nc_dir, args.max_patch)
        if got is None:
            print(
                f"{d}  no file through v3-0-{args.max_patch} "
                f"(example {url_for_day(d, 'v3-0-0')})",
                flush=True,
            )
            missing += 1
            continue

        nc_path, ver = got
        csv_path = out_dir / f"sci_sgps-l2-avg1m_g18_d{ymd}_{ver}.csv"
        print(f"{d}  ok {ver}  {url_for_day(d, ver)}", flush=True)

        try:
            nc_to_csv(nc_path, csv_path)
        except Exception as e:
            print(f"  convert error: {e}", file=sys.stderr)
            failed += 1
            if not args.keep_nc:
                nc_path.unlink(missing_ok=True)
            continue

        ok += 1
        if not args.keep_nc:
            nc_path.unlink(missing_ok=True)

    if not args.keep_nc:
        try:
            nc_dir.rmdir()
        except OSError:
            pass

    print(
        f"Done. converted={ok} missing={missing} failed={failed} -> {out_dir}",
        flush=True,
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
