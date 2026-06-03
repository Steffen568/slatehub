#!/usr/bin/env python3
"""
refresh_excel.py — Automate Excel Power Query Refresh + Export CSVs

1. Opens MLB_PQs_ALL.xlsx silently in Excel
2. Triggers Refresh All (runs all Power Queries)
3. Waits for all queries to finish
4. Exports each sheet as a season-stamped CSV to the MLB CSVs folder
5. Closes and saves Excel

CSVs are named with the season year (e.g., dash_2026.csv)
so prior seasons are never overwritten.

Run this BEFORE load_fangraphs_excel.py (or refresh_all.py calls it automatically)

Usage:
  py -3.12 refresh_excel.py
"""

import os, sys, time, csv
import win32com.client

from config import SEASON

# -- Paths -----------------------------------------------------------------
EXCEL_PATH = r"C:\Users\Steffen's PC\Desktop\WebDev\MLB_PQs\MLB_PQs_ALL.xlsx"
CSV_DIR    = r"C:\Users\Steffen's PC\Desktop\WebDev\MLB CSVs"

# -- Sheet name -> season-stamped CSV filename -----------------------------
SHEET_MAP = {
    # Pitcher stats
    'Dash'           : f'dash_{SEASON}.csv',
    'BattedBall'     : f'batted_ball_{SEASON}.csv',
    'Pitching+'      : f'pitching_plus_{SEASON}.csv',
    'Stuff+'         : f'stuff_plus_{SEASON}.csv',
    'Location+'      : f'location_plus_{SEASON}.csv',
    # Batter stats
    'HitterDash'     : f'hitter_dash_{SEASON}.csv',
    'HitterStatcas'  : f'hitter_statcast_{SEASON}.csv',
    'BatTracking'    : f'bat_tracking_{SEASON}.csv',
    # Batter splits
    'vRHP'           : f'splits_batter_vs_rhp_{SEASON}.csv',
    'vLHP'           : f'splits_batter_vs_lhp_{SEASON}.csv',
    # Pitcher splits
    'vLHH Stand'     : f'splits_pitcher_vs_lhh_std_{SEASON}.csv',
    'vLHH Adv'       : f'splits_pitcher_vs_lhh_adv_{SEASON}.csv',
    'vRHH Stand'     : f'splits_pitcher_vs_rhh_std_{SEASON}.csv',
    'vRHH Adv'       : f'splits_pitcher_vs_rhh_adv_{SEASON}.csv',
}

MAX_WAIT_SECONDS = 120


def run():
    print(f"\nSlateHub — Excel Power Query Refresh ({SEASON})")
    print("=" * 40)

    if not os.path.exists(EXCEL_PATH):
        print(f"ERROR: Excel file not found at:\n  {EXCEL_PATH}")
        sys.exit(1)

    os.makedirs(CSV_DIR, exist_ok=True)

    print(f"Opening Excel: {EXCEL_PATH}")

    excel = None
    wb    = None

    try:
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible        = False
        excel.DisplayAlerts  = False
        excel.ScreenUpdating = False

        wb = excel.Workbooks.Open(EXCEL_PATH)
        print("  Workbook opened")

        print("  Refreshing all Power Queries...")
        wb.RefreshAll()

        print(f"  Waiting for queries to complete (max {MAX_WAIT_SECONDS}s)...")
        start = time.time()
        while True:
            time.sleep(2)
            elapsed = time.time() - start

            still_refreshing = False
            for conn in wb.Connections:
                try:
                    if conn.OLEDBConnection.Refreshing:
                        still_refreshing = True
                        break
                except:
                    try:
                        if conn.ODBCConnection.Refreshing:
                            still_refreshing = True
                            break
                    except:
                        pass

            if not still_refreshing:
                print(f"  All queries complete ({elapsed:.1f}s)")
                break

            if elapsed > MAX_WAIT_SECONDS:
                print(f"  Timeout after {MAX_WAIT_SECONDS}s — exporting whatever is loaded")
                break

            print(f"    Still refreshing... ({elapsed:.1f}s)")

        wb.Save()
        print("  Workbook saved")

        print(f"\n  Exporting {len(SHEET_MAP)} sheets to {CSV_DIR}...")
        exported = 0

        for sheet_name, csv_name in SHEET_MAP.items():
            sheet = None
            for ws in wb.Worksheets:
                if ws.Name.lower() == sheet_name.lower():
                    sheet = ws
                    break

            if sheet is None:
                print(f"  Sheet not found: '{sheet_name}' — skipping")
                continue

            used_range = sheet.UsedRange
            rows = used_range.Value

            if not rows:
                print(f"  Sheet '{sheet_name}' is empty — skipping")
                continue

            out_path = os.path.join(CSV_DIR, csv_name)

            with open(out_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in rows:
                    cleaned = []
                    for cell in row:
                        if cell is None:
                            cleaned.append('')
                        elif isinstance(cell, float) and cell == int(cell):
                            cleaned.append(int(cell))
                        else:
                            cleaned.append(cell)
                    writer.writerow(cleaned)

            data_rows = len(rows) - 1
            print(f"  {sheet_name} -> {csv_name} ({data_rows} rows)")
            exported += 1

        print(f"\n  Exported {exported}/{len(SHEET_MAP)} sheets to:")
        print(f"  {CSV_DIR}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        try:
            if wb:
                wb.Close(SaveChanges=False)
            if excel:
                excel.Quit()
            print("\n  Excel closed")
        except:
            pass

    print("\nDone! Now run:")
    print(f"  py -3.12 load_fangraphs_excel.py")


if __name__ == '__main__':
    run()
