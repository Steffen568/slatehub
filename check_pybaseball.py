#!/usr/bin/env python3
"""Check what pybaseball functions are available for pitch-level velo/stuff"""
import pybaseball as pb

# Print all available functions
funcs = [f for f in dir(pb) if not f.startswith('_')]
print("Available pybaseball functions:")
for f in sorted(funcs):
    print(f"  {f}")
    