"""
SlateHub — Central configuration
SEASON auto-detects based on today's date:
  - April through December → current year (regular season + postseason)
  - January through March  → previous year (offseason, last season's data)
No manual update needed at the start of each season.
"""

from datetime import date

_today = date.today()
SEASON = _today.year if _today.month >= 4 else _today.year - 1
