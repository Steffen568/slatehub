#!/usr/bin/env python3
"""
refresh_excel_splits.py — Automate Excel Power Query Refresh + Export CSVs

1. Opens MLB_PQs.xlsx silently in Excel
2. Triggers Refresh All (runs all Power Queries)
3. Waits for all queries to finish
4. Exports each sheet as a CSV to the slatehub-backend directory
5. Closes and saves Excel

Run this BEFORE sync_excel_splits.py (or refresh_all.py will call it automatically)

Usage:
  py -3.12 refresh_excel_splits.py
"""

import os, sys, time, shutil
import win32com.client

# ── Paths
EXCEL_PATH  = r"C:\Users\Steffen's PC\Desktop\MLB_PQs\MLB_PQs.xlsx"
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))

# ── Sheet name → CSV filename mapping
SHEET_MAP = {
    'vsLHP'              : 'splits_batter_vs_lhp.csv',
    'vsRHP'              : 'splits_batter_vs_rhp.csv',
    'Pitcher vs LHH Std' : 'splits_pitcher_vs_lhh_std.csv',
    'Pitcher vs LHH Adv' : 'splits_pitcher_vs_lhh_adv.csv',
    'Pitcher vs RHH Std' : 'splits_pitcher_vs_rhh_std.csv',
    'Pitcher vs RHH Adv' : 'splits_pitcher_vs_rhh_adv.csv',
}

MAX_WAIT_SECONDS = 120  # max time to wait for queries to refresh

def run():
    print("\nSlateHub — Excel Power Query Refresh")
    print("=" * 40)

    if not os.path.exists(EXCEL_PATH):
        print(f"✗ ERROR: Excel file not found at:\n  {EXCEL_PATH}")
        sys.exit(1)

    print(f"Opening Excel: {EXCEL_PATH}")

    excel = None
    wb    = None

    try:
        # Open Excel as a COM object — Visible=False runs it in background
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible       = False
        excel.DisplayAlerts = False
        excel.ScreenUpdating = False

        wb = excel.Workbooks.Open(EXCEL_PATH)
        print("  ✓ Workbook opened")

        # Trigger Refresh All
        print("  Refreshing all Power Queries...")
        wb.RefreshAll()

        # Wait for all background queries to finish
        # Excel runs PQ refreshes asynchronously so we poll until done
        print(f"  Waiting for queries to complete (max {MAX_WAIT_SECONDS}s)...")
        start = time.time()
        while True:
            time.sleep(2)
            elapsed = time.time() - start

            # Check if any connections are still refreshing
            still_refreshing = False
            for conn in wb.Connections:
                try:
                    if conn.OLEDBConnection.Refreshing:
                        still_refreshing = True
                        break
                except:
                    # Not all connection types have OLEDBConnection
                    try:
                        if conn.ODBCConnection.Refreshing:
                            still_refreshing = True
                            break
                    except:
                        pass

            if not still_refreshing:
                print(f"  ✓ All queries complete ({elapsed:.1f}s)")
                break

            if elapsed > MAX_WAIT_SECONDS:
                print(f"  ⚠ Timeout after {MAX_WAIT_SECONDS}s — exporting whatever is loaded")
                break

            print(f"    Still refreshing... ({elapsed:.1f}s)")

        # Save workbook
        wb.Save()
        print("  ✓ Workbook saved")

        # Export each sheet as CSV
        print("\n  Exporting sheets to CSV...")
        exported = 0

        for sheet_name, csv_name in SHEET_MAP.items():
            sheet = None
            # Find sheet by name (case-insensitive)
            for ws in wb.Worksheets:
                if ws.Name.lower() == sheet_name.lower():
                    sheet = ws
                    break

            if sheet is None:
                print(f"  ⚠ Sheet not found: '{sheet_name}' — skipping")
                continue

            # Read sheet data into a list of rows
            used_range = sheet.UsedRange
            rows = used_range.Value

            if not rows:
                print(f"  ⚠ Sheet '{sheet_name}' is empty — skipping")
                continue

            out_path = os.path.join(SCRIPT_DIR, csv_name)

            # Back up existing CSV
            if os.path.exists(out_path):
                shutil.copy2(out_path, out_path.replace('.csv', '_backup.csv'))

            # Write CSV manually to avoid needing pandas here
            import csv
            with open(out_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in rows:
                    # Convert None to empty string, numbers to string cleanly
                    cleaned = []
                    for cell in row:
                        if cell is None:
                            cleaned.append('')
                        elif isinstance(cell, float) and cell == int(cell):
                            cleaned.append(int(cell))
                        else:
                            cleaned.append(cell)
                    writer.writerow(cleaned)

            # Count data rows (minus header)
            data_rows = len(rows) - 1
            print(f"  ✓ {sheet_name} → {csv_name} ({data_rows} rows)")
            exported += 1

        print(f"\n  Exported {exported}/{len(SHEET_MAP)} sheets to:")
        print(f"  {SCRIPT_DIR}")

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # Always close Excel cleanly
        try:
            if wb:
                wb.Close(SaveChanges=False)
            if excel:
                excel.Quit()
            print("\n  ✓ Excel closed")
        except:
            pass

    print("\nDone! Now run:")
    print("  py -3.12 sync_excel_splits.py --upload-only")

if __name__ == '__main__':
    run()