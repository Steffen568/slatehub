#!/usr/bin/env python3
"""Check statcast_pitcher_pitch_arsenal columns"""
import pybaseball as pb
pb.cache.enable()

df = pb.statcast_pitcher_pitch_arsenal(2025, minP=50)
print("Columns:", list(df.columns))
print()
print("First row:")
print(df.iloc[0].to_dict())
