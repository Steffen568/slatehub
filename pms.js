// pms.js — canonical PMS logic. index.html is the master; this file is the single source of truth.
// Include via <script src="pms.js"></script> in every page that needs computePMS.
// generate_pool.py must mirror this logic exactly (Python port).

function computePhysicsMatchup(arsenalRows, attackAngle, swingPathTilt, pitcherArmAngle) {
  const LEAGUE_ATTACK = 12.0, LEAGUE_ARM = 45.0;
  const clip = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const safe = (v, fb) => (v != null && isFinite(v)) ? v : (fb ?? 0);
  if (!arsenalRows || !arsenalRows.length) return { kAdj: 1.0, contactAdj: 1.0, difficulty: 0, hrPlaneAdj: 1.0 };
  if (attackAngle == null) attackAngle = LEAGUE_ATTACK;
  if (swingPathTilt == null) swingPathTilt = 0.0;
  const totalUsage = arsenalRows.reduce((s, r) => s + safe(r.usage_pct, 0), 0);
  if (totalUsage <= 0) return { kAdj: 1.0, contactAdj: 1.0, difficulty: 0, hrPlaneAdj: 1.0 };
  const IVB_BASELINE = 8.0, IVB_VAA_COEFF = 0.088;
  let weightedDifficulty = 0, weightedVertGap = 0, weightSum = 0;
  for (const r of arsenalRows) {
    const u = safe(r.usage_pct, 0) / totalUsage;
    if (u <= 0) continue;
    const rh = safe(r.release_height, 6.0), ext = safe(r.extension, 6.5);
    const vaa = -(Math.atan((rh - 2.5) / Math.max(60.5 - ext, 1.0)) * 180 / Math.PI);
    const ivb = safe(r.ivb, IVB_BASELINE), hb = safe(r.hb, 0.0);
    const effectiveVaa = vaa + (ivb - IVB_BASELINE) * IVB_VAA_COEFF;
    const vertGap = Math.abs(attackAngle - Math.abs(effectiveVaa));
    const vertScore = clip(vertGap / 15.0, 0, 1);
    const latMismatch = clip((Math.abs(hb) - Math.abs(swingPathTilt) * 1.5) / 18.0, 0, 1);
    weightedDifficulty += (vertScore * 0.70 + latMismatch * 0.30) * u;
    weightedVertGap    += vertGap * u;
    weightSum += u;
  }
  if (weightSum <= 0) return { kAdj: 1.0, contactAdj: 1.0, difficulty: 0, hrPlaneAdj: 1.0 };
  let raw = weightedDifficulty / weightSum;
  if (pitcherArmAngle != null) {
    const armDev = pitcherArmAngle - LEAGUE_ARM;
    raw = clip(raw + clip(-armDev * (attackAngle - LEAGUE_ATTACK) * 0.001, 0, 0.15), 0, 1);
  }
  const avgVertGap = weightedVertGap / weightSum;
  const hrPlaneAdj = clip(1.0 - (avgVertGap - 5.0) * 0.015, 0.90, 1.10);
  return {
    kAdj:       clip(1.0 + raw * 0.08, 0.94, 1.10),
    contactAdj: clip(1.0 - raw * 0.05, 0.92, 1.05),
    difficulty: raw,
    hrPlaneAdj,
  };
}

function computePMS(pd, pSplits, bt, bStats, batHand, bSplits, vaa, l7xwoba, arsenalRows, pitHand) {
  const components = [];
  const toD = v => (v == null ? null : v > 1 ? v / 100 : v);
  const toR = v => (v == null ? null : v > 1 ? v : v * 100);

  // Resolve pitcher split for this batter's hand
  // Switch hitters: use whichever side is worse for the pitcher (higher xwOBA)
  let pSplit = null;
  let effectiveHand = batHand; // the side the batter will hit from
  if (batHand === 'S') {
    const xwL = parseFloat(pSplits?.L?.xwoba || pSplits?.L?.woba) || 0;
    const xwR = parseFloat(pSplits?.R?.xwoba || pSplits?.R?.woba) || 0;
    pSplit = xwL >= xwR ? pSplits?.L : pSplits?.R;
    effectiveHand = xwL >= xwR ? 'L' : 'R';
  } else if (batHand) {
    pSplit = pSplits?.[batHand];
  }

  // Resolve batter split — indexed by PITCHER hand, not batter hand
  let bSplit = null;
  if (bSplits && pitHand) {
    bSplit = bSplits[pitHand];
  }

  // 1. Platoon Vulnerability (3 pts) — most stable split, most predictive
  // Use max(xwoba, woba*0.92) — handles stale/missing xwOBA for low-PA pitchers
  const _pxw  = parseFloat(pSplit?.xwoba) || null;
  const _pwob = parseFloat(pSplit?.woba)  || null;
  const pxw = (_pxw != null || _pwob != null)
    ? Math.max(_pxw ?? 0, _pwob != null ? _pwob * 0.92 : 0) || null
    : null;
  if (pxw != null) {
    const p = pxw >= 0.350 ? 3 : pxw >= 0.320 ? 2 : pxw >= 0.300 ? 1 : 0;
    components.push({ name: 'Platoon Vulnerability', pts: p, max: 3, pxw });
  }

  // 2. Pitcher Stuff Quality (2 pts) — Stuff+ 0.73 y/y stability
  // Pitching+ combines stuff + command. Fall back to Stuff+, then xFIP.
  const pp = parseFloat(pd?.pitching_plus) || null;
  const sp = parseFloat(pd?.stuff_plus) || null;
  const xf = parseFloat(pd?.xfip) || null;
  if (pp != null || sp != null || xf != null) {
    let p = 0;
    if (pp != null) {
      p = pp <= 80 ? 2 : pp <= 100 ? 1 : 0;
    } else if (sp != null) {
      p = sp <= 80 ? 2 : sp <= 100 ? 1 : 0;
    } else {
      p = xf >= 4.50 ? 2 : xf >= 4.00 ? 1 : 0;
    }
    components.push({ name: 'Pitcher Stuff Quality', pts: p, max: 2, pp, sp, xf });
  }

  // 3. Barrel Opportunity (2 pts) — stabilizes in ~50 BIP, best power predictor
  const pBrl = toR(pd?.barrel_pct);
  const hBrl = toR(bStats?.barrel_pct);
  const hHH = toR(bStats?.hard_hit_pct);
  if (pBrl != null && (hBrl != null || hHH != null)) {
    let p = 0;
    if (pBrl >= 9 && hBrl >= 9) p = 2;
    else if (pBrl >= 9 || hBrl >= 9) p = 1;
    else if (pBrl >= 7 && hHH >= 38) p = 1; // hard hit fallback
    components.push({ name: 'Barrel Opportunity', pts: p, max: 2, pBrl, hBrl, hHH });
  }

  // 4. Physics Matchup (3 pts) — pitch geometry vs swing plane
  const atk = bt?.attack_angle ?? null;
  const tilt = bt?.swing_path_tilt ?? null;
  const spArm = pd?.arm_angle ?? null;
  if (arsenalRows && arsenalRows.length) {
    const { kAdj, contactAdj, difficulty, hrPlaneAdj } = computePhysicsMatchup(arsenalRows, atk, tilt, spArm);
    const hrDiff = (hrPlaneAdj - 1.0) / 0.10;
    const adjustedDifficulty = Math.max(0, Math.min(1, difficulty - hrDiff * 0.25));
    const p = adjustedDifficulty <= 0.15 ? 3 : adjustedDifficulty <= 0.35 ? 2 : adjustedDifficulty <= 0.55 ? 1 : 0;
    components.push({ name: 'Physics Matchup', pts: p, max: 3, difficulty: adjustedDifficulty, kAdj, contactAdj, hrPlaneAdj, atk, tilt, spArm });
  }

  // 5. K Environment (2 pts) — K% stabilizes after ~150 BF
  const kRaw = pSplit?.k_pct ?? pd?.k_pct;
  const swRaw = pd?.swstr_pct;
  if (kRaw != null) {
    const k = toD(kRaw);
    let p = k <= 0.18 ? 2 : k <= 0.24 ? 1 : 0;
    // Bonus: very low swinging strike rate means pitcher can't miss bats
    if (p < 2 && swRaw != null && toD(swRaw) <= 0.09) p = Math.min(p + 1, 2);
    components.push({ name: 'K Environment', pts: p, max: 2, k, sw: toD(swRaw) });
  }

  // 6. Contact Quality (2 pts) — batter contact skill vs pitcher hard-contact allowed
  const sqUp = parseFloat(bt?.squared_up_pct) || null;
  const bxw = parseFloat(bSplit?.xwoba || bSplit?.woba) || parseFloat(bStats?.xwoba) || null;
  const pHH = toR(pSplit?.hard_hit_pct) ?? toR(pd?.hard_hit_pct);
  if (bxw != null || sqUp != null) {
    const strongBatter = (bxw != null && bxw >= 0.370) || (sqUp != null && sqUp >= 13);
    const strongPitcher = (pHH != null && pHH >= 38) || (pBrl != null && pBrl >= 10);
    let p = 0;
    if (strongBatter && strongPitcher) p = 2;
    else if (strongBatter || strongPitcher) p = 1;
    components.push({ name: 'Contact Quality', pts: p, max: 2, sqUp, bxw, pHH });
  }

  // 7. Discipline Matchup (1 pt) — low-chase hitter vs wild pitcher
  const oSwing = toD(bStats?.o_swing_pct);
  const pBB = toD(pSplit?.bb_pct ?? pd?.bb_pct);
  if (oSwing != null && pBB != null) {
    const p = (oSwing <= 0.28 && pBB >= 0.09) ? 1 : 0;
    components.push({ name: 'Discipline Matchup', pts: p, max: 1, oSwing, pBB });
  }

  // 8. Recent Form (1 pt) — L7 xwOBA as tiebreaker
  if (l7xwoba != null) {
    const p = l7xwoba >= 0.400 ? 1 : 0;
    components.push({ name: 'Recent Form', pts: p, max: 1, l7xwoba });
  }

  // Score: earned / available * 10, +1 offset so neutral matchups land at 5
  const totalPts = components.reduce((s, c) => s + c.pts, 0);
  const maxPts = components.reduce((s, c) => s + c.max, 0);
  const score = maxPts > 0 ? Math.round((totalPts / maxPts) * 10) + 1 : 5;

  let grade, cls;
  if      (score >= 9) { grade = 'Advantage';  cls = 'mb-advantage';    }
  else if (score >= 7) { grade = 'Favorable';  cls = 'mb-favorable';    }
  else if (score >= 5) { grade = 'Neutral';    cls = 'mb-neutral';      }
  else if (score >= 3) { grade = 'Tough';      cls = 'mb-tough';        }
  else                 { grade = 'Very Tough'; cls = 'mb-pitcher-edge'; }

  return { score, grade, cls, pSplit, components, totalPts, maxPts };
}
