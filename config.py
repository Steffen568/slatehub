"""
SlateHub — Central configuration
SEASON auto-detects based on today's date:
  - Late March through December → current year (spring training + regular season + postseason)
  - January through mid-March   → previous year (offseason, last season's data)
Opening Day is typically late March, so the cutoff is March 20.
"""

from datetime import date

_today = date.today()
SEASON = _today.year if (_today.month >= 4 or (_today.month == 3 and _today.day >= 20)) else _today.year - 1
