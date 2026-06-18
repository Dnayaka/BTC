#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_binance_klines.py
═══════════════════════════════════════════════════════════════════════════
Pengambil data candle (klines) historis dari Binance Public Data
(https://data.binance.vision) — TANPA API key, TANPA rate limit.

Default: BTCUSDT.P (USDⓈ-M perpetual futures), interval 15m, 1 tahun terakhir.

Catatan penting soal environment Claude Code on the web:
  Host `data.binance.vision` HARUS dimasukkan ke network egress allowlist
  environment sebelum script ini bisa jalan. Kalau belum, semua request akan
  kena HTTP 403 "Host not in allowlist". Lihat:
  https://code.claude.com/docs/en/claude-code-on-the-web

Hanya memakai standard library (urllib, zipfile, hashlib, csv, datetime),
jadi TIDAK perlu `pip install` apa pun (PyPI juga diblokir allowlist).

Contoh pemakaian
────────────────
  # Default: BTCUSDT 15m futures USDⓈ-M, 12 bulan terakhir
  python3 fetch_binance_klines.py

  # Kustom rentang / simbol / interval / market
  python3 fetch_binance_klines.py --symbol BTCUSDT --interval 15m \
      --months 12 --market um --out dynamic_rsi/data/BTCUSDT.P_15m_1y.csv

  --market : um (USDⓈ-M futures, default) | cm (COIN-M) | spot
"""

import argparse
import csv
import datetime as dt
import hashlib
import io
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile

BASE = "https://data.binance.vision/data"

# Kolom mentah CSV klines Binance (12 kolom)
RAW_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]

# Kolom output (rapi + datetime_utc untuk keterbacaan)
OUT_COLS = [
    "datetime_utc", "open_time_ms", "open", "high", "low", "close",
    "volume", "close_time_ms", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote",
]

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
    "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
}


class NotFound(Exception):
    """HTTP 404 — file belum/tidak tersedia (mis. file bulanan bulan berjalan)."""


# ── Network ────────────────────────────────────────────────────────────────
def download(url, retries=5):
    """Unduh URL -> bytes. Backoff eksponensial. 404 dilempar sbg NotFound."""
    delay = 2
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "klines-fetcher/1.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NotFound(url)
            if e.code == 403:
                # Hampir pasti egress allowlist — tidak ada gunanya retry.
                body = ""
                try:
                    body = e.read().decode("utf-8", "ignore")[:200]
                except Exception:
                    pass
                raise SystemExit(
                    f"\n[FATAL] HTTP 403 untuk {url}\n"
                    f"        {body}\n"
                    f"        => Tambahkan 'data.binance.vision' ke network egress allowlist "
                    f"environment, lalu jalankan ulang.\n"
                )
            last_err = e
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
        if attempt < retries:
            print(f"    ! gagal ({last_err}); retry {attempt}/{retries-1} dalam {delay}s")
            time.sleep(delay)
            delay *= 2
    raise SystemExit(f"[FATAL] Gagal mengunduh {url} setelah {retries} percobaan: {last_err}")


def download_zip_rows(zip_url, verify=True):
    """Unduh .zip (+verifikasi SHA256 dari .CHECKSUM), kembalikan list baris CSV mentah."""
    blob = download(zip_url)
    if verify:
        try:
            chk = download(zip_url + ".CHECKSUM").decode("utf-8", "ignore").split()
            if chk:
                want = chk[0].lower()
                got = hashlib.sha256(blob).hexdigest()
                if want != got:
                    raise SystemExit(
                        f"[FATAL] Checksum MISMATCH {zip_url}\n  want={want}\n  got ={got}"
                    )
        except NotFound:
            print("    ~ .CHECKSUM tidak ada, lanjut tanpa verifikasi")
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        name = z.namelist()[0]
        text = z.read(name).decode("utf-8", "ignore")
    rows = []
    for line in text.splitlines():
        if not line:
            continue
        # Dump terbaru kadang menyertakan baris header (non-numerik) — lewati.
        if line[0].isalpha():
            continue
        rows.append(line.split(","))
    return rows


# ── Helper tanggal ───────────────────────────────────────────────────────--
def month_range(start, end):
    """Yield (year, month) inklusif dari bulan `start` s/d bulan `end`."""
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def days_in_month(year, month):
    nxt = dt.date(year + (month == 12), (month % 12) + 1, 1)
    cur = dt.date(year, month, 1)
    return (nxt - cur).days


def norm_ts(v):
    """Normalisasi open_time ke milidetik (Binance pindah ke mikrodetik utk data baru)."""
    n = int(v)
    return n // 1000 if n > 10**14 else n


# ── Inti ──────────────────────────────────────────────────────────────────-
def fetch_month(symbol, interval, market, year, month, verify=True):
    """Ambil 1 bulan. Coba file bulanan dulu; kalau 404, fallback ke file harian."""
    seg = "spot" if market == "spot" else f"futures/{market}"
    ym = f"{year:04d}-{month:02d}"
    monthly = f"{BASE}/{seg}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{ym}.zip"
    try:
        rows = download_zip_rows(monthly, verify)
        print(f"  [bulanan] {ym}: {len(rows):>6} baris")
        return rows
    except NotFound:
        pass  # Belum dipublikasi (mis. bulan berjalan) → pakai harian.

    rows = []
    for d in range(1, days_in_month(year, month) + 1):
        ymd = f"{year:04d}-{month:02d}-{d:02d}"
        daily = f"{BASE}/{seg}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{ymd}.zip"
        try:
            rows += download_zip_rows(daily, verify)
        except NotFound:
            continue  # Hari yg belum tersedia.
    print(f"  [harian ] {ym}: {len(rows):>6} baris")
    return rows


def main():
    ap = argparse.ArgumentParser(description="Ambil klines historis Binance (data.binance.vision).")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--interval", default="15m", choices=sorted(INTERVAL_MS))
    ap.add_argument("--market", default="um", choices=["um", "cm", "spot"],
                    help="um=USDⓈ-M futures (BTCUSDT.P), cm=COIN-M, spot")
    ap.add_argument("--months", type=int, default=12, help="Jumlah bulan ke belakang (default 12 = 1 tahun)")
    ap.add_argument("--out", default=None, help="Path CSV output")
    ap.add_argument("--no-verify", action="store_true", help="Lewati verifikasi SHA256")
    args = ap.parse_args()

    today = dt.datetime.now(dt.timezone.utc).date()
    # start = awal bulan, `months` bulan ke belakang (inklusif bulan ini)
    sy, sm = today.year, today.month - (args.months - 1)
    while sm <= 0:
        sm += 12
        sy -= 1
    start_month = dt.date(sy, sm, 1)
    start_ms = int(dt.datetime(sy, sm, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)

    out = args.out or f"dynamic_rsi/data/{args.symbol}.P_{args.interval}_{args.months}mo.csv"
    step = INTERVAL_MS[args.interval]

    print("═" * 70)
    print(f" Binance klines: {args.symbol} {args.interval} market={args.market}")
    print(f" Rentang       : {start_month}  →  {today}  ({args.months} bulan)")
    print(f" Output        : {out}")
    print("═" * 70)

    # Kumpulkan & dedupe berdasarkan open_time
    by_ts = {}
    for y, m in month_range(start_month, today):
        for row in fetch_month(args.symbol, args.interval, args.market, y, m, verify=not args.no_verify):
            try:
                t = norm_ts(row[0])
            except (ValueError, IndexError):
                continue
            if start_ms <= t <= now_ms:
                by_ts[t] = row

    if not by_ts:
        raise SystemExit("[FATAL] Tidak ada data terkumpul. Cek allowlist / simbol / market.")

    times = sorted(by_ts)

    # Validasi gap
    gaps, missing = [], 0
    for a, b in zip(times, times[1:]):
        d = b - a
        if d != step:
            n = d // step - 1
            missing += max(n, 0)
            if len(gaps) < 10:
                gaps.append((a, b, n))

    # Tulis CSV
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(OUT_COLS)
        for t in times:
            r = by_ts[t]
            iso = dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            ct = norm_ts(r[6]) if len(r) > 6 else ""
            w.writerow([iso, t, r[1], r[2], r[3], r[4], r[5], ct,
                        r[7] if len(r) > 7 else "",
                        r[8] if len(r) > 8 else "",
                        r[9] if len(r) > 9 else "",
                        r[10] if len(r) > 10 else ""])

    span_days = (times[-1] - times[0]) / 86_400_000
    expected = int((times[-1] - times[0]) // step) + 1
    size_mb = os.path.getsize(out) / 1_048_576

    print("─" * 70)
    print(f" Candle terkumpul : {len(times):,}")
    print(f" Diharapkan (kontinu): {expected:,}  | hilang: {missing:,}")
    print(f" Periode          : "
          f"{dt.datetime.fromtimestamp(times[0]/1000, dt.timezone.utc)} → "
          f"{dt.datetime.fromtimestamp(times[-1]/1000, dt.timezone.utc)}  ({span_days:.1f} hari)")
    print(f" Jumlah gap       : {len(gaps)}{' (>10, ditampilkan 10 pertama)' if missing and len(gaps)==10 else ''}")
    for a, b, n in gaps:
        print(f"   - {dt.datetime.fromtimestamp(a/1000, dt.timezone.utc)} "
              f"→ {dt.datetime.fromtimestamp(b/1000, dt.timezone.utc)}  ({n} candle hilang)")
    print(f" File             : {out}  ({size_mb:.2f} MB)")
    print("─" * 70)
    print("Selesai ✓")


if __name__ == "__main__":
    sys.exit(main())
