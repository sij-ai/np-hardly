#!/usr/bin/env python3
"""
NP-Hardly
*Working NP-Hard, or NP-Hardly Working!?*

A constraint-based, opinionated scheduling engine powered by Google OR-Tools CP-SAT.
Reads project configuration from YAML, builds a mathematical model, solves for the 
optimal schedule, and exports to CSV.

Constraint types supported:
  aggregate_hours, rolling_window, availability, assignment,
  attribute, pairing, minimum_rest, shift_composition, shift_span

All constraint types support MUST, MUST_NOT, PREFER, PREFER_NOT.

Usage:
    python np-hardly.py project.yaml
    python np-hardly.py config_dir/ -o schedule.csv --time-limit 120
    python np-hardly.py project.yaml --repair old_schedule.csv --debug
"""

from __future__ import annotations

import argparse
import csv
import logging
import multiprocessing
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from ortools.sat.python import cp_model

# ═══════════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════════
log = logging.getLogger("scheduler")


def _setup_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )


# ═══════════════════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Project:
    name: str
    owner: str = ""
    start_date: str = ""
    end_date: str = ""
    timezone: str = ""


@dataclass
class Role:
    id: str
    name: str
    description: str = ""


@dataclass
class ShiftRequirement:
    role_id: str
    min_headcount: int
    max_headcount: int


@dataclass
class Shift:
    id: str
    name: str
    start_dt: datetime
    end_dt: datetime
    duration_hours: float
    requirements: list[ShiftRequirement] = field(default_factory=list)


@dataclass
class Volunteer:
    id: str
    name: str
    gender: str = ""
    role_experience: dict[str, int] = field(default_factory=dict)


@dataclass
class ScheduleData:
    project: Project
    roles: list[Role]
    shifts: list[Shift]
    volunteers: list[Volunteer]
    constraints: list[dict] = field(default_factory=list)


@dataclass
class SoftTerm:
    constraint_idx: int
    description: str
    indicator: cp_model.IntVar
    weight: int
    enforcement: str
    is_penalty: bool = False


# ═══════════════════════════════════════════════════════════════════════════
# YAML Parsing — supports single file or directory of YAML files
# ═══════════════════════════════════════════════════════════════════════════

def _deep_merge(base: dict, overlay: dict) -> dict:
    """
    Merge overlay into base.
    - Lists are extended (not replaced).
    - Dicts are recursively merged.
    - Scalars are overwritten by overlay.
    """
    merged = dict(base)
    for key, val in overlay.items():
        if key in merged:
            if isinstance(merged[key], list) and isinstance(val, list):
                merged[key] = merged[key] + val
            elif isinstance(merged[key], dict) and isinstance(val, dict):
                merged[key] = _deep_merge(merged[key], val)
            else:
                merged[key] = val
        else:
            merged[key] = val
    return merged


def parse_yaml_file(filepath: str) -> ScheduleData:
    """
    Load YAML from a single file or a directory of .yaml/.yml files.
    When given a directory, all YAML files are merged: lists are extended,
    dicts are updated (last-wins for scalar values).
    """
    path = Path(filepath)

    if path.is_dir():
        log.info("Loading YAML directory: %s", path)
        yaml_files = sorted(
            list(path.glob("*.yaml")) + list(path.glob("*.yml"))
        )
        if not yaml_files:
            log.error("No .yaml/.yml files found in %s", path)
            sys.exit(1)
        log.info("Found %d YAML file(s): %s",
                 len(yaml_files), [f.name for f in yaml_files])

        raw: dict = {}
        for yf in yaml_files:
            log.debug("  Merging: %s", yf.name)
            with open(yf, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
            if content and isinstance(content, dict):
                raw = _deep_merge(raw, content)
            else:
                log.warning("  Skipping %s (empty or not a mapping)", yf.name)
    else:
        log.info("Loading YAML from: %s", path)
        if not path.exists():
            log.error("File not found: %s", filepath)
            sys.exit(1)
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

    log.debug("YAML top-level keys: %s", list(raw.keys()))

    # ── Project ──
    p = raw.get("project", {})
    project = Project(
        name=p.get("name", "Unnamed"), owner=p.get("owner", ""),
        start_date=str(p.get("start_date", "")),
        end_date=str(p.get("end_date", "")),
        timezone=p.get("timezone", ""),
    )
    log.debug("Project: %s (%s → %s)", project.name, project.start_date, project.end_date)

    # ── Roles ──
    roles: list[Role] = []
    seen_role_ids: set[str] = set()
    for r in raw.get("roles", []):
        rid = r["id"]
        if rid in seen_role_ids:
            log.debug("  Duplicate role '%s' — skipping", rid)
            continue
        seen_role_ids.add(rid)
        roles.append(Role(id=rid, name=r["name"], description=r.get("description", "")))
        log.debug("  Role  : %-18s '%s'", roles[-1].id, roles[-1].name)
    role_ids = {r.id for r in roles}
    log.info("Roles: %d  %s", len(roles), sorted(role_ids))

    # ── Shifts ──
    shifts: list[Shift] = []
    seen_shift_ids: set[str] = set()
    total_slots = 0
    for s in raw.get("shifts", []):
        sid = s["id"]
        if sid in seen_shift_ids:
            log.debug("  Duplicate shift '%s' — skipping", sid)
            continue
        seen_shift_ids.add(sid)
        sdt = datetime.fromisoformat(str(s["start_datetime"]))
        edt = datetime.fromisoformat(str(s["end_datetime"]))
        dur = (edt - sdt).total_seconds() / 3600.0
        reqs: list[ShiftRequirement] = []
        ss = 0
        for rid_key, cnt in s.get("requirements", {}).items():
            if rid_key not in role_ids:
                log.warning("Shift '%s' → unknown role '%s'", sid, rid_key)
            c = int(cnt)
            reqs.append(ShiftRequirement(role_id=rid_key, min_headcount=c, max_headcount=c))
            ss += c
        shift = Shift(id=sid, name=s["name"], start_dt=sdt, end_dt=edt,
                       duration_hours=dur, requirements=reqs)
        shifts.append(shift)
        total_slots += ss
        log.debug("  Shift : %-12s '%-16s' %s→%s (%.1fh) slots=%d",
                   shift.id, shift.name, sdt.strftime("%m-%d %H:%M"),
                   edt.strftime("%m-%d %H:%M"), dur, ss)
        for rq in reqs:
            log.debug("          req: %-18s exactly %d", rq.role_id, rq.min_headcount)
    log.info("Shifts: %d  Total slots: %d", len(shifts), total_slots)

    # ── Volunteers ──
    volunteers: list[Volunteer] = []
    seen_vol_ids: set[str] = set()
    for v in raw.get("volunteers", []):
        vid = v["id"]
        if vid in seen_vol_ids:
            log.debug("  Duplicate volunteer '%s' — skipping", vid)
            continue
        seen_vol_ids.add(vid)
        vol = Volunteer(
            id=vid, name=v["name"], gender=v.get("gender", ""),
            role_experience={str(k): int(val) for k, val in v.get("role_experience", {}).items()},
        )
        volunteers.append(vol)
        log.debug("  Vol   : %-8s %-10s %-6s exp=%s",
                   vol.id, vol.name, vol.gender, vol.role_experience)
    log.info("Volunteers: %d", len(volunteers))

    # ── Constraints ──
    constraints: list[dict] = raw.get("constraints") or []
    log.info("Constraints: %d", len(constraints))
    for i, c in enumerate(constraints):
        log.debug("  [%d] type=%-18s enf=%-12s w=%-4s '%s'",
                   i, c.get("type"), c.get("enforcement"),
                   c.get("weight", "-"), c.get("description", c.get("type", "")))

    # ── Cross-validation ──
    for vol in volunteers:
        for er in vol.role_experience:
            if er not in role_ids:
                log.warning("Vol '%s' exp for unknown role '%s'", vol.id, er)

    data = ScheduleData(project=project, roles=roles, shifts=shifts,
                         volunteers=volunteers, constraints=constraints)
    log.info("Parsing complete: %d roles, %d shifts, %d vols, %d constraints",
             len(roles), len(shifts), len(volunteers), len(constraints))
    return data


# ═══════════════════════════════════════════════════════════════════════════
# Repair: Parse previous CSV schedule
# ═══════════════════════════════════════════════════════════════════════════

def parse_repair_csv(
    csv_path: str,
    data: ScheduleData,
) -> list[tuple[str, str, str]]:
    """
    Parse a previous schedule CSV and return (vol_id, shift_id, role_id)
    tuples for every assignment found.

    CSV format expected (from our export):
      Row 0 = header: Shift, Start, End, Duration, <Role Name>, ...
      Row N = shift:  <name>, <start>, <end>, <dur>, <names>, ...

    Mapping strategy:
      - Shift name → shift_id
      - Column header (role name) → role_id
      - Cell volunteer name → volunteer_id
    """
    log.info("Parsing repair CSV: %s", csv_path)
    p = Path(csv_path)
    if not p.exists():
        log.error("Repair CSV not found: %s", csv_path)
        sys.exit(1)

    # Build reverse lookup maps
    name_to_vid: dict[str, str] = {}
    for v in data.volunteers:
        name_to_vid[v.name.strip().lower()] = v.id

    sname_to_sid: dict[str, str] = {}
    for s in data.shifts:
        sname_to_sid[s.name.strip().lower()] = s.id

    rname_to_rid: dict[str, str] = {}
    for r in data.roles:
        rname_to_rid[r.name.strip().lower()] = r.id

    assignments: list[tuple[str, str, str]] = []

    with open(p, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    # Identify role columns (columns after the first 4 metadata columns)
    META_COLS = 4  # Shift, Start, End, Duration
    role_col_map: dict[int, str] = {}
    for col_idx in range(META_COLS, len(header)):
        col_name = header[col_idx].strip().lower()
        rid = rname_to_rid.get(col_name)
        if rid:
            role_col_map[col_idx] = rid
        else:
            log.warning("  Repair CSV column '%s' doesn't match any role — skip",
                        header[col_idx])

    # Re-read to parse data rows
    with open(p, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row_num, row in enumerate(reader, start=2):
            if len(row) < META_COLS + 1:
                continue
            shift_name = row[0].strip().lower()
            sid = sname_to_sid.get(shift_name)
            if not sid:
                log.warning("  Row %d: shift '%s' not found — skip", row_num, row[0])
                continue

            for col_idx, rid in role_col_map.items():
                if col_idx >= len(row):
                    continue
                cell = row[col_idx].strip()
                if not cell or cell == "—":
                    continue
                for vname in cell.split(","):
                    vname_clean = vname.strip().lower()
                    vid = name_to_vid.get(vname_clean)
                    if vid:
                        assignments.append((vid, sid, rid))
                        log.debug("  Repair: %s → %s as %s", vname.strip(), sid, rid)
                    else:
                        log.warning("  Row %d: volunteer '%s' not found — skip",
                                    row_num, vname.strip())

    log.info("Repair CSV parsed: %d previous assignment(s) to preserve", len(assignments))
    return assignments


# ═══════════════════════════════════════════════════════════════════════════
# Constraint Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _get_vol_attr(vol: Volunteer, path: str) -> Any:
    cur: Any = vol
    for p in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif hasattr(cur, p):
            cur = getattr(cur, p)
        else:
            return None
    return cur


def _compare(actual: Any, op: str, target: Any) -> bool:
    if actual is None:
        actual = type(target)(0) if isinstance(target, (int, float)) else ""
    ops = {
        "LESS_THAN": lambda a, b: a < b,
        "LESS_THAN_OR_EQUAL": lambda a, b: a <= b,
        "GREATER_THAN": lambda a, b: a > b,
        "GREATER_THAN_OR_EQUAL": lambda a, b: a >= b,
        "EQUAL": lambda a, b: a == b,
        "NOT_EQUAL": lambda a, b: a != b,
    }
    fn = ops.get(op)
    return fn(actual, target) if fn else False


def _resolve_subject_vols(subject: dict, data: ScheduleData) -> set[str]:
    vids = subject.get("volunteer_ids")
    if vids is not None:
        if "ANY" in vids:
            r = {v.id for v in data.volunteers}
            log.debug("    Subject → ALL %d vols (ANY)", len(r))
            return r
        r = set(vids)
        log.debug("    Subject → specific: %s", sorted(r))
        return r
    fa = subject.get("filter_attribute")
    if fa:
        fo, fv = subject["filter_operator"], subject["filter_value"]
        r = {v.id for v in data.volunteers if _compare(_get_vol_attr(v, fa), fo, fv)}
        log.debug("    Subject filter [%s %s %s] → %d: %s", fa, fo, fv, len(r), sorted(r))
        return r
    return set()


def _vol_shift_vars(works, vid, sid, roles):
    return [works[(vid, sid, r.id)] for r in roles if (vid, sid, r.id) in works]


def _filter_vols_by_attr(data, fattr, fop, fval):
    return {v.id for v in data.volunteers if _compare(_get_vol_attr(v, fattr), fop, fval)}


def _find_rest_violation_pairs(shifts, min_rest_h):
    ss = sorted(shifts, key=lambda s: s.start_dt)
    pairs = []
    for i in range(len(ss)):
        for j in range(i + 1, len(ss)):
            gap_h = (ss[j].start_dt - ss[i].end_dt).total_seconds() / 3600.0
            if gap_h > 0 and gap_h < min_rest_h:
                pairs.append((ss[i], ss[j]))
    return pairs


def _compute_horizon(data: ScheduleData) -> tuple[datetime, int]:
    """
    Returns (earliest_start_dt, max_horizon_minutes).
    All shift times are expressed as minute offsets from earliest_start.
    """
    earliest = min(s.start_dt for s in data.shifts)
    latest = max(s.end_dt for s in data.shifts)
    horizon_min = int((latest - earliest).total_seconds() / 60) + 1
    return earliest, horizon_min


# ═══════════════════════════════════════════════════════════════════════════
# Hard Constraint Handlers
# ═══════════════════════════════════════════════════════════════════════════

def _hard_aggregate_hours(model, works, data, cd, **kw):
    cond = cd["condition"]
    mn = int(cond.get("min_hours", 0) * 60)
    mx = int(cond.get("max_hours", 999999) * 60)
    vids = _resolve_subject_vols(cd["subject"], data)
    ct = 0
    for v in data.volunteers:
        if v.id not in vids:
            continue
        expr = [works[(v.id, s.id, r.id)] * int(s.duration_hours * 60)
                for s in data.shifts for r in data.roles if (v.id, s.id, r.id) in works]
        if not expr:
            continue
        if cond.get("min_hours", 0) > 0:
            model.Add(sum(expr) >= mn); ct += 1
        model.Add(sum(expr) <= mx); ct += 1
        log.debug("    agg[%s]: %d ≤ Σmin ≤ %d", v.id, mn, mx)
    return ct


def _hard_rolling_window(model, works, data, cd, **kw):
    cond = cd["condition"]
    max_min = int(cond["max_hours_worked"] * 60)
    win_h = cond["window_size_hours"]
    vids = _resolve_subject_vols(cd["subject"], data)
    ss = sorted(data.shifts, key=lambda s: s.start_dt)
    groups, seen = [], set()
    for i in range(len(ss)):
        g = [ss[i]]
        for j in range(i + 1, len(ss)):
            if (ss[j].end_dt - ss[i].start_dt).total_seconds() / 3600 <= win_h:
                g.append(ss[j])
            else:
                break
        if len(g) >= 2 and sum(s.duration_hours for s in g) > cond["max_hours_worked"]:
            k = tuple(s.id for s in g)
            if k not in seen:
                seen.add(k); groups.append(g)
    log.debug("    Rolling %dh-in-%dh → %d group(s)",
              int(cond["max_hours_worked"]), int(win_h), len(groups))
    ct = 0
    for g in groups:
        for v in data.volunteers:
            if v.id not in vids:
                continue
            expr = [works[(v.id, s.id, r.id)] * int(s.duration_hours * 60)
                    for s in g for r in data.roles if (v.id, s.id, r.id) in works]
            if expr:
                model.Add(sum(expr) <= max_min); ct += 1
    return ct


def _hard_availability(model, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    us = datetime.fromisoformat(str(cond["start_datetime"]))
    ue = datetime.fromisoformat(str(cond["end_datetime"]))
    vids = _resolve_subject_vols(cd["subject"], data)
    ct = 0
    for v in data.volunteers:
        if v.id not in vids:
            continue
        for s in data.shifts:
            if s.start_dt < ue and s.end_dt > us:
                if enf == "MUST_NOT":
                    for r in data.roles:
                        k = (v.id, s.id, r.id)
                        if k in works:
                            model.Add(works[k] == 0); ct += 1
                    log.debug("    BLOCK %s from %s", v.id, s.id)
                elif enf == "MUST":
                    rv = _vol_shift_vars(works, v.id, s.id, data.roles)
                    if rv:
                        model.Add(sum(rv) >= 1); ct += 1
    return ct


def _hard_assignment(model, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    ts = cond.get("target_shift_id", "ANY")
    tr = cond.get("target_role_id", "ANY")
    vids = _resolve_subject_vols(cd["subject"], data)
    ct = 0
    for vid in vids:
        if not any(v.id == vid for v in data.volunteers):
            continue
        for s in [s for s in data.shifts if ts == "ANY" or s.id == ts]:
            if tr == "ANY":
                rv = _vol_shift_vars(works, vid, s.id, data.roles)
                if rv:
                    if enf == "MUST":
                        model.Add(sum(rv) >= 1)
                    else:
                        model.Add(sum(rv) == 0)
                    ct += 1
                    log.debug("    %s %s → %s (any role)", enf, vid, s.id)
            else:
                for r in [r for r in data.roles if r.id == tr]:
                    k = (vid, s.id, r.id)
                    if k in works:
                        model.Add(works[k] == (1 if enf == "MUST" else 0)); ct += 1
                        log.debug("    %s %s → %s as %s", enf, vid, s.id, r.id)
    return ct


def _hard_attribute(model, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    tr = cond.get("target_role_id", "ANY")
    ts = cond.get("target_shift_id", "ANY")
    filt = _resolve_subject_vols(cd["subject"], data)
    tshifts = [s for s in data.shifts if ts == "ANY" or s.id == ts]
    troles = [r for r in data.roles if tr == "ANY" or r.id == tr]
    ct = 0
    for vid in sorted(filt):
        for s in tshifts:
            for r in troles:
                k = (vid, s.id, r.id)
                if k in works:
                    model.Add(works[k] == (1 if enf == "MUST" else 0)); ct += 1
        log.debug("    ATTR %s: %s → role=%s shift=%s",
                   enf, vid, [r.id for r in troles], "ALL" if ts == "ANY" else ts)
    return ct


def _hard_pairing(model, works, data, cd, **kw):
    enf = cd["enforcement"]
    vol_ids = list(cd["subject"].get("volunteer_ids", []))
    if "ANY" in vol_ids:
        log.warning("    pairing with ANY — skip"); return 0
    known = {v.id for v in data.volunteers}
    vol_ids = [vid for vid in vol_ids if vid in known]
    if len(vol_ids) < 2:
        log.warning("    pairing needs ≥2 vols — skip"); return 0
    ct = 0
    if enf == "MUST":
        for s in data.shifts:
            bv = _vol_shift_vars(works, vol_ids[0], s.id, data.roles)
            if not bv:
                continue
            for vid in vol_ids[1:]:
                ov = _vol_shift_vars(works, vid, s.id, data.roles)
                if ov:
                    model.Add(sum(bv) == sum(ov)); ct += 1
                    log.debug("    MUST pair %s==%s on %s", vol_ids[0], vid, s.id)
    elif enf == "MUST_NOT":
        for s in data.shifts:
            slot_sums = []
            for vid in vol_ids:
                rv = _vol_shift_vars(works, vid, s.id, data.roles)
                if rv:
                    slot_sums.append(sum(rv))
            if len(slot_sums) >= 2:
                model.Add(sum(slot_sums) <= 1); ct += 1
                log.debug("    MUST_NOT pair ≤1 of %s on %s", vol_ids, s.id)
    return ct


def _hard_minimum_rest(model, works, data, cd, **kw):
    enf = cd["enforcement"]
    if enf == "MUST_NOT":
        log.warning("    minimum_rest MUST_NOT not meaningful — skip"); return 0
    cond = cd["condition"]
    min_rest_h = float(cond["min_rest_hours"])
    vids = _resolve_subject_vols(cd["subject"], data)
    pairs = _find_rest_violation_pairs(data.shifts, min_rest_h)
    log.debug("    min_rest %.1fh → %d violating pairs", min_rest_h, len(pairs))
    for s1, s2 in pairs:
        gap = (s2.start_dt - s1.end_dt).total_seconds() / 3600
        log.debug("      %s→%s gap=%.1fh", s1.id, s2.id, gap)
    ct = 0
    for s1, s2 in pairs:
        for v in data.volunteers:
            if v.id not in vids:
                continue
            rv1 = _vol_shift_vars(works, v.id, s1.id, data.roles)
            rv2 = _vol_shift_vars(works, v.id, s2.id, data.roles)
            if rv1 and rv2:
                model.Add(sum(rv1) + sum(rv2) <= 1); ct += 1
    return ct


def _hard_shift_composition(model, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    ts = cond.get("target_shift_id", "ANY")
    fa, fo, fv = cond["filter_attribute"], cond["filter_operator"], cond["filter_value"]
    mn = int(cond.get("min_count", 0))
    mx = int(cond.get("max_count", 999))
    matching = _filter_vols_by_attr(data, fa, fo, fv)
    log.debug("    Composition filter [%s %s %s] → %d vols: %s",
              fa, fo, fv, len(matching), sorted(matching))
    target_shifts = [s for s in data.shifts if ts == "ANY" or s.id == ts]
    ct = 0
    for s in target_shifts:
        count_expr = []
        for vid in matching:
            rv = _vol_shift_vars(works, vid, s.id, data.roles)
            if rv:
                count_expr.append(sum(rv))
        if not count_expr:
            if enf == "MUST" and mn > 0:
                log.error("    STRUCTURAL: %s needs min %d but 0 vars!", s.id, mn)
            continue
        total_count = sum(count_expr)
        if enf == "MUST":
            if mn > 0:
                model.Add(total_count >= mn); ct += 1
            model.Add(total_count <= mx); ct += 1
            log.debug("    COMP[%s]: %d ≤ count ≤ %d", s.id, mn, mx)
        elif enf == "MUST_NOT":
            b = model.NewBoolVar(f"comp_not_{s.id}")
            model.Add(total_count <= mn - 1).OnlyEnforceIf(b)
            model.Add(total_count >= mx + 1).OnlyEnforceIf(b.Not())
            ct += 3
            log.debug("    COMP_NOT[%s]: NOT in [%d,%d]", s.id, mn, mx)
    return ct


def _hard_shift_span(model, works, data, cd, **kw):
    """
    MUST shift_span → volunteer's first-to-last shift span ≤ max_span_hours.
    MUST_NOT        → span must NOT be ≤ max_span_hours (i.e. must exceed).
    """
    enf = cd["enforcement"]
    cond = cd["condition"]
    max_span_h = int(cond.get("max_span_hours", 999))
    vids = _resolve_subject_vols(cd["subject"], data)
    is_working = kw.get("is_working", {})

    origin, horizon_min = _compute_horizon(data)
    max_span_min = max_span_h * 60

    # Precompute shift offsets
    shift_start_off = {}
    shift_end_off = {}
    for s in data.shifts:
        shift_start_off[s.id] = int((s.start_dt - origin).total_seconds() / 60)
        shift_end_off[s.id] = int((s.end_dt - origin).total_seconds() / 60)

    ct = 0
    for v in data.volunteers:
        if v.id not in vids:
            continue

        iw_list = [(s, is_working[(v.id, s.id)])
                    for s in data.shifts if (v.id, s.id) in is_working]
        if not iw_list:
            continue

        tag = f"span_{v.id}"

        first_start = model.NewIntVar(0, horizon_min, f"fs_{tag}")
        last_end = model.NewIntVar(0, horizon_min, f"le_{tag}")
        works_any = model.NewBoolVar(f"wa_{tag}")

        iw_vars = [iw for _, iw in iw_list]
        model.Add(sum(iw_vars) >= 1).OnlyEnforceIf(works_any)
        model.Add(sum(iw_vars) == 0).OnlyEnforceIf(works_any.Not())

        for s, iw in iw_list:
            s_off = shift_start_off[s.id]
            e_off = shift_end_off[s.id]
            model.Add(first_start <= s_off).OnlyEnforceIf(iw)
            model.Add(last_end >= e_off).OnlyEnforceIf(iw)

        # When not working, collapse to 0
        model.Add(first_start == 0).OnlyEnforceIf(works_any.Not())
        model.Add(last_end == 0).OnlyEnforceIf(works_any.Not())

        span = model.NewIntVar(0, horizon_min, f"sp_{tag}")
        model.Add(span == last_end - first_start).OnlyEnforceIf(works_any)
        model.Add(span == 0).OnlyEnforceIf(works_any.Not())

        if enf == "MUST":
            model.Add(span <= max_span_min); ct += 1
            log.debug("    span[%s] ≤ %d min", v.id, max_span_min)
        elif enf == "MUST_NOT":
            model.Add(span > max_span_min).OnlyEnforceIf(works_any); ct += 1
            log.debug("    span[%s] > %d min (must_not)", v.id, max_span_min)

    return ct


_HARD_HANDLERS: dict[str, Any] = {
    "aggregate_hours":    _hard_aggregate_hours,
    "rolling_window":     _hard_rolling_window,
    "availability":       _hard_availability,
    "assignment":         _hard_assignment,
    "attribute":          _hard_attribute,
    "pairing":            _hard_pairing,
    "minimum_rest":       _hard_minimum_rest,
    "shift_composition":  _hard_shift_composition,
    "shift_span":         _hard_shift_span,
}


def apply_hard_constraints(model, works, data, is_working):
    log.info("Applying hard constraints …")
    stats = {"hard": 0, "soft_skip": 0, "unknown": 0, "cp": 0}
    for idx, cd in enumerate(data.constraints):
        enf = cd.get("enforcement", "")
        ctype = cd.get("type", "?")
        desc = cd.get("description", ctype)
        if enf not in ("MUST", "MUST_NOT"):
            log.debug("  [C%d] SKIP (soft) type=%-18s enf=%s", idx, ctype, enf)
            stats["soft_skip"] += 1
            continue
        log.debug("  [C%d] HARD %-9s type=%-18s '%s'", idx, enf, ctype, desc)
        handler = _HARD_HANDLERS.get(ctype)
        if not handler:
            log.warning("  [C%d] Unknown type '%s' — skip", idx, ctype)
            stats["unknown"] += 1
            continue
        added = handler(model, works, data, cd, is_working=is_working)
        stats["hard"] += 1
        stats["cp"] += added
        log.debug("    → %d CP-SAT constraint(s)", added)
    log.info("  Hard rules applied: %d → %d CP-SAT constraints",
             stats["hard"], stats["cp"])
    return stats


# ═══════════════════════════════════════════════════════════════════════════
# is_working Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _create_is_working_vars(model, works, data):
    is_working: dict[tuple[str, str], cp_model.IntVar] = {}
    ct = 0
    for v in data.volunteers:
        for s in data.shifts:
            rvars = _vol_shift_vars(works, v.id, s.id, data.roles)
            if rvars:
                iw = model.NewBoolVar(f"iw__{v.id}__{s.id}")
                model.Add(sum(rvars) == iw)
                is_working[(v.id, s.id)] = iw
                ct += 1
    log.debug("Created %d is_working BoolVars.", ct)
    return is_working


# ═══════════════════════════════════════════════════════════════════════════
# Soft Constraint Handlers
# ═══════════════════════════════════════════════════════════════════════════

def _soft_pairing(model, works, is_working, data, cd, cidx):
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    vol_ids = list(cd["subject"].get("volunteer_ids", []))
    if "ANY" in vol_ids:
        log.warning("    C%d: pairing with ANY — skip", cidx); return []
    known = {v.id for v in data.volunteers}
    vol_ids = [vid for vid in vol_ids if vid in known]
    if len(vol_ids) < 2:
        return []
    vname = {v.id: v.name for v in data.volunteers}
    pair_label = " & ".join(vname.get(vid, vid) for vid in vol_ids)
    match = cd["condition"].get("match_target", "shift")
    terms: list[SoftTerm] = []
    if match == "shift":
        for s in data.shifts:
            iw_list = []
            skip = False
            for vid in vol_ids:
                k = (vid, s.id)
                if k not in is_working:
                    skip = True; break
                iw_list.append(is_working[k])
            if skip or not iw_list:
                continue
            n = len(iw_list)
            paired = model.NewBoolVar(f"soft_pair_C{cidx}__{s.id}")
            for iw in iw_list:
                model.Add(paired <= iw)
            model.Add(paired >= sum(iw_list) - (n - 1))
            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"{pair_label} together [{s.name}]",
                indicator=paired, weight=weight, enforcement=enf))
    else:
        log.warning("    C%d: match_target='%s' unsupported", cidx, match)
    return terms


def _soft_aggregate_hours(model, works, is_working, data, cd, cidx):
    """AddMaxEquality-locked slack for aggregate hours."""
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    cond = cd["condition"]
    min_h = int(cond.get("min_hours", 0))
    max_h = int(cond.get("max_hours", 999))
    vids = _resolve_subject_vols(cd["subject"], data)
    max_possible_min = int(sum(s.duration_hours for s in data.shifts) * 60) + 60
    min_min = min_h * 60
    max_min = max_h * 60
    vname = {v.id: v.name for v in data.volunteers}
    terms: list[SoftTerm] = []
    for v in data.volunteers:
        if v.id not in vids:
            continue
        expr_parts = []
        for s in data.shifts:
            dur_m = int(s.duration_hours * 60)
            for r in data.roles:
                k = (v.id, s.id, r.id)
                if k in works:
                    expr_parts.append(works[k] * dur_m)
        if not expr_parts:
            continue
        tag = f"C{cidx}_{v.id}"
        total_var = model.NewIntVar(0, max_possible_min, f"total_min_{tag}")
        model.Add(total_var == sum(expr_parts))

        diff_under = model.NewIntVar(-max_possible_min, min_min, f"diff_under_{tag}")
        model.Add(diff_under == min_min - total_var)
        z_u = model.NewConstant(0)
        slack_under = model.NewIntVar(0, min_min, f"slk_under_{tag}")
        model.AddMaxEquality(slack_under, [z_u, diff_under])

        diff_over = model.NewIntVar(-max_possible_min, max_possible_min, f"diff_over_{tag}")
        model.Add(diff_over == total_var - max_min)
        z_o = model.NewConstant(0)
        slack_over = model.NewIntVar(0, max_possible_min - max_min, f"slk_over_{tag}")
        model.AddMaxEquality(slack_over, [z_o, diff_over])

        slack_under_h = model.NewIntVar(0, min_h, f"slkUh_{tag}")
        slack_over_h = model.NewIntVar(0, max_possible_min // 60, f"slkOh_{tag}")
        model.Add(slack_under_h * 60 == slack_under)
        model.Add(slack_over_h * 60 == slack_over)
        penalty_h = model.NewIntVar(0, min_h + max_possible_min // 60, f"pen_agg_{tag}")
        model.Add(penalty_h == slack_under_h + slack_over_h)

        if enf == "PREFER":
            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"{vname.get(v.id, v.id)} agg deviation",
                indicator=penalty_h, weight=weight, enforcement=enf,
                is_penalty=True))
        else:
            in_range = model.NewBoolVar(f"inrange_{tag}")
            model.Add(penalty_h == 0).OnlyEnforceIf(in_range)
            model.Add(penalty_h >= 1).OnlyEnforceIf(in_range.Not())
            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"{vname.get(v.id, v.id)} in [{min_h},{max_h}]h",
                indicator=in_range, weight=weight, enforcement=enf))
        log.debug("    soft_agg[%s]: [%d,%d]h w=%d enf=%s (MaxEquality locked)",
                   v.id, min_h, max_h, weight, enf)
    return terms


def _soft_assignment(model, works, is_working, data, cd, cidx):
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    cond = cd["condition"]
    ts = cond.get("target_shift_id", "ANY")
    tr = cond.get("target_role_id", "ANY")
    vids = _resolve_subject_vols(cd["subject"], data)
    vname = {v.id: v.name for v in data.volunteers}
    terms: list[SoftTerm] = []
    for vid in sorted(vids):
        for s in data.shifts:
            if ts != "ANY" and s.id != ts:
                continue
            if tr == "ANY":
                k = (vid, s.id)
                if k in is_working:
                    terms.append(SoftTerm(
                        cidx, f"{vname.get(vid, vid)} in {s.name}",
                        is_working[k], weight, enf))
            else:
                wk = (vid, s.id, tr)
                if wk in works:
                    terms.append(SoftTerm(
                        cidx, f"{vname.get(vid, vid)} as {tr} in {s.name}",
                        works[wk], weight, enf))
    return terms


def _soft_availability(model, works, is_working, data, cd, cidx):
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    cond = cd["condition"]
    us = datetime.fromisoformat(str(cond["start_datetime"]))
    ue = datetime.fromisoformat(str(cond["end_datetime"]))
    vids = _resolve_subject_vols(cd["subject"], data)
    terms: list[SoftTerm] = []
    for v in data.volunteers:
        if v.id not in vids:
            continue
        for s in data.shifts:
            if s.start_dt < ue and s.end_dt > us:
                k = (v.id, s.id)
                if k in is_working:
                    terms.append(SoftTerm(
                        cidx, f"{v.name} in {s.name} (avail)",
                        is_working[k], weight, enf))
    return terms


def _soft_attribute(model, works, is_working, data, cd, cidx):
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    cond = cd["condition"]
    tr = cond.get("target_role_id", "ANY")
    ts = cond.get("target_shift_id", "ANY")
    filt = _resolve_subject_vols(cd["subject"], data)
    terms: list[SoftTerm] = []
    for vid in sorted(filt):
        vn = next((v.name for v in data.volunteers if v.id == vid), vid)
        for s in data.shifts:
            if ts != "ANY" and s.id != ts:
                continue
            for r in data.roles:
                if tr != "ANY" and r.id != tr:
                    continue
                k = (vid, s.id, r.id)
                if k in works:
                    terms.append(SoftTerm(
                        cidx, f"{vn} as {r.id} [{s.name}]",
                        works[k], weight, enf))
    return terms


def _soft_minimum_rest(model, works, is_working, data, cd, cidx):
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    cond = cd["condition"]
    min_rest_h = float(cond["min_rest_hours"])
    vids = _resolve_subject_vols(cd["subject"], data)
    pairs = _find_rest_violation_pairs(data.shifts, min_rest_h)
    log.debug("    soft min_rest %.1fh → %d pairs", min_rest_h, len(pairs))
    vname = {v.id: v.name for v in data.volunteers}
    terms: list[SoftTerm] = []
    for s1, s2 in pairs:
        gap_h = (s2.start_dt - s1.end_dt).total_seconds() / 3600
        for v in data.volunteers:
            if v.id not in vids:
                continue
            k1, k2 = (v.id, s1.id), (v.id, s2.id)
            if k1 not in is_working or k2 not in is_working:
                continue
            both = model.NewBoolVar(f"rest_viol_C{cidx}_{v.id}_{s1.id}_{s2.id}")
            model.Add(both <= is_working[k1])
            model.Add(both <= is_working[k2])
            model.Add(both >= is_working[k1] + is_working[k2] - 1)
            actual_enf = "PREFER_NOT" if enf == "PREFER" else "PREFER"
            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"{vname.get(v.id, v.id)}: {s1.id}→{s2.id} gap={gap_h:.1f}h",
                indicator=both, weight=weight, enforcement=actual_enf))
    return terms


def _soft_shift_composition(model, works, is_working, data, cd, cidx):
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    cond = cd["condition"]
    ts = cond.get("target_shift_id", "ANY")
    fa, fo, fv = cond["filter_attribute"], cond["filter_operator"], cond["filter_value"]
    mn = int(cond.get("min_count", 0))
    mx = int(cond.get("max_count", 999))
    matching = _filter_vols_by_attr(data, fa, fo, fv)
    target_shifts = [s for s in data.shifts if ts == "ANY" or s.id == ts]
    terms: list[SoftTerm] = []
    for s in target_shifts:
        count_parts = []
        for vid in matching:
            k = (vid, s.id)
            if k in is_working:
                count_parts.append(is_working[k])
        if not count_parts:
            continue
        tag = f"C{cidx}_{s.id}"
        count_var = model.NewIntVar(0, len(count_parts), f"comp_cnt_{tag}")
        model.Add(count_var == sum(count_parts))

        diff_under = model.NewIntVar(-len(count_parts), mn, f"comp_dU_{tag}")
        model.Add(diff_under == mn - count_var)
        z_u = model.NewConstant(0)
        slack_under = model.NewIntVar(0, mn, f"comp_slkU_{tag}")
        model.AddMaxEquality(slack_under, [z_u, diff_under])

        diff_over = model.NewIntVar(-mx, len(count_parts), f"comp_dO_{tag}")
        model.Add(diff_over == count_var - mx)
        z_o = model.NewConstant(0)
        slack_over = model.NewIntVar(0, len(count_parts), f"comp_slkO_{tag}")
        model.AddMaxEquality(slack_over, [z_o, diff_over])

        penalty = model.NewIntVar(0, mn + len(count_parts), f"comp_pen_{tag}")
        model.Add(penalty == slack_under + slack_over)

        if enf == "PREFER":
            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"composition [{s.name}] target=[{mn},{mx}]",
                indicator=penalty, weight=weight, enforcement=enf,
                is_penalty=True))
        else:
            in_range = model.NewBoolVar(f"comp_inrange_{tag}")
            model.Add(penalty == 0).OnlyEnforceIf(in_range)
            model.Add(penalty >= 1).OnlyEnforceIf(in_range.Not())
            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"composition [{s.name}] in [{mn},{mx}]",
                indicator=in_range, weight=weight, enforcement=enf))
    return terms


def _soft_shift_span(model, works, is_working, data, cd, cidx):
    """
    PREFER shift_span → penalize span exceeding max_span_hours.
    PREFER_NOT        → penalize span being within max_span_hours.
    """
    weight = int(cd.get("weight", 1))
    enf = cd["enforcement"]
    cond = cd["condition"]
    max_span_h = int(cond.get("max_span_hours", 999))
    vids = _resolve_subject_vols(cd["subject"], data)

    origin, horizon_min = _compute_horizon(data)
    max_span_min = max_span_h * 60

    shift_start_off = {}
    shift_end_off = {}
    for s in data.shifts:
        shift_start_off[s.id] = int((s.start_dt - origin).total_seconds() / 60)
        shift_end_off[s.id] = int((s.end_dt - origin).total_seconds() / 60)

    vname = {v.id: v.name for v in data.volunteers}
    terms: list[SoftTerm] = []

    for v in data.volunteers:
        if v.id not in vids:
            continue

        iw_list = [(s, is_working[(v.id, s.id)])
                    for s in data.shifts if (v.id, s.id) in is_working]
        if not iw_list:
            continue

        tag = f"span_C{cidx}_{v.id}"
        first_start = model.NewIntVar(0, horizon_min, f"fs_{tag}")
        last_end = model.NewIntVar(0, horizon_min, f"le_{tag}")
        works_any = model.NewBoolVar(f"wa_{tag}")

        iw_vars = [iw for _, iw in iw_list]
        model.Add(sum(iw_vars) >= 1).OnlyEnforceIf(works_any)
        model.Add(sum(iw_vars) == 0).OnlyEnforceIf(works_any.Not())

        for s, iw in iw_list:
            model.Add(first_start <= shift_start_off[s.id]).OnlyEnforceIf(iw)
            model.Add(last_end >= shift_end_off[s.id]).OnlyEnforceIf(iw)

        model.Add(first_start == 0).OnlyEnforceIf(works_any.Not())
        model.Add(last_end == 0).OnlyEnforceIf(works_any.Not())

        span = model.NewIntVar(0, horizon_min, f"sp_{tag}")
        model.Add(span == last_end - first_start).OnlyEnforceIf(works_any)
        model.Add(span == 0).OnlyEnforceIf(works_any.Not())

        # Span in hours (integer)
        span_h = model.NewIntVar(0, horizon_min // 60 + 1, f"sph_{tag}")
        model.Add(span_h * 60 <= span)
        model.Add(span_h * 60 >= span - 59)

        if enf == "PREFER":
            # Penalize excess over max_span_hours
            diff_over = model.NewIntVar(-horizon_min // 60 - 1, horizon_min // 60 + 1,
                                         f"span_dO_{tag}")
            model.Add(diff_over == span_h - max_span_h)
            z = model.NewConstant(0)
            excess = model.NewIntVar(0, horizon_min // 60 + 1, f"span_ex_{tag}")
            model.AddMaxEquality(excess, [z, diff_over])

            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"{vname.get(v.id, v.id)} span excess (>{max_span_h}h)",
                indicator=excess, weight=weight, enforcement=enf,
                is_penalty=True))
        else:  # PREFER_NOT
            # Penalize being within max_span (unusual but symmetric)
            within = model.NewBoolVar(f"span_within_{tag}")
            model.Add(span <= max_span_min).OnlyEnforceIf(within)
            model.Add(span > max_span_min).OnlyEnforceIf(within.Not())
            terms.append(SoftTerm(
                constraint_idx=cidx,
                description=f"{vname.get(v.id, v.id)} span ≤{max_span_h}h",
                indicator=within, weight=weight, enforcement=enf))

        log.debug("    soft_span[%s] max=%dh w=%d enf=%s", v.id, max_span_h, weight, enf)

    return terms


_SOFT_HANDLERS: dict[str, Any] = {
    "pairing":            _soft_pairing,
    "aggregate_hours":    _soft_aggregate_hours,
    "assignment":         _soft_assignment,
    "availability":       _soft_availability,
    "attribute":          _soft_attribute,
    "minimum_rest":       _soft_minimum_rest,
    "shift_composition":  _soft_shift_composition,
    "shift_span":         _soft_shift_span,
}


def apply_soft_constraints(model, works, is_working, data):
    log.info("Applying soft constraints …")
    all_terms: list[SoftTerm] = []
    stats = {"soft": 0, "hard_skip": 0, "unknown": 0, "terms": 0}
    for idx, cd in enumerate(data.constraints):
        enf = cd.get("enforcement", "")
        ctype = cd.get("type", "?")
        desc = cd.get("description", ctype)
        if enf not in ("PREFER", "PREFER_NOT"):
            log.debug("  [C%d] SKIP (hard) type=%-18s", idx, ctype)
            stats["hard_skip"] += 1
            continue
        w = cd.get("weight", 1)
        log.debug("  [C%d] SOFT %-11s type=%-18s w=%s '%s'", idx, enf, ctype, w, desc)
        handler = _SOFT_HANDLERS.get(ctype)
        if not handler:
            log.warning("  [C%d] No soft handler for '%s' — skip", idx, ctype)
            stats["unknown"] += 1
            continue
        terms = handler(model, works, is_working, data, cd, idx)
        all_terms.extend(terms)
        stats["soft"] += 1
        stats["terms"] += len(terms)
        log.debug("    → %d term(s)", len(terms))
    log.info("  Soft rules applied: %d → %d indicator terms",
             stats["soft"], stats["terms"])
    return all_terms


# ═══════════════════════════════════════════════════════════════════════════
# Objective Function
# ═══════════════════════════════════════════════════════════════════════════

def build_objective(model, soft_terms, repair_parts=None):
    """
    Build Maximize objective from soft terms + repair glue.
    Returns True if an objective was set.
    """
    obj_parts: list = []

    # Soft constraint contributions
    for st in soft_terms:
        if st.is_penalty:
            if st.enforcement == "PREFER":
                obj_parts.append(-st.weight * st.indicator)
            else:
                obj_parts.append(st.weight * st.indicator)
        else:
            if st.enforcement == "PREFER":
                obj_parts.append(st.weight * st.indicator)
            else:
                obj_parts.append(-st.weight * st.indicator)

    # Repair glue contributions
    if repair_parts:
        obj_parts.extend(repair_parts)

    if not obj_parts:
        log.info("No objective terms → FEASIBILITY mode.")
        return False

    model.Maximize(sum(obj_parts))

    n_soft = len(soft_terms)
    n_repair = len(repair_parts) if repair_parts else 0
    max_reward = sum(st.weight for st in soft_terms
                     if not st.is_penalty and st.enforcement == "PREFER")
    log.info("Objective: Maximize Σ(contributions)  "
             "soft=%d  repair_glue=%d  max_soft_reward=+%d",
             n_soft, n_repair, max_reward)
    return True


# ═══════════════════════════════════════════════════════════════════════════
# Model Building & Solving
# ═══════════════════════════════════════════════════════════════════════════

def build_and_solve(
    data: ScheduleData,
    time_limit: float,
    num_workers: int,
    repair_assignments: list[tuple[str, str, str]] | None = None,
) -> tuple[str, Any, Any]:
    model = cp_model.CpModel()

    # ── Decision Variables ───────────────────────────────────────────────
    works: dict[tuple[str, str, str], cp_model.IntVar] = {}
    for v in data.volunteers:
        for s in data.shifts:
            for r in data.roles:
                works[(v.id, s.id, r.id)] = model.NewBoolVar(
                    f"w__{v.id}__{s.id}__{r.id}")
    log.info("Variables: %d (%dV × %dS × %dR)",
             len(works), len(data.volunteers), len(data.shifts), len(data.roles))

    # ── Base: Headcount ──────────────────────────────────────────────────
    ct_hc = 0
    for s in data.shifts:
        for rq in s.requirements:
            avars = [works[(v.id, s.id, rq.role_id)]
                     for v in data.volunteers if (v.id, s.id, rq.role_id) in works]
            model.Add(sum(avars) >= rq.min_headcount)
            model.Add(sum(avars) <= rq.max_headcount)
            ct_hc += 2
    log.debug("Headcount constraints: %d", ct_hc)

    # ── Base: One Role per Shift ─────────────────────────────────────────
    ct_or = 0
    for v in data.volunteers:
        for s in data.shifts:
            rv = _vol_shift_vars(works, v.id, s.id, data.roles)
            if rv:
                model.Add(sum(rv) <= 1); ct_or += 1
    log.debug("One-role constraints: %d", ct_or)

    # ── is_working (created BEFORE hard constraints for shift_span) ─────
    is_working = _create_is_working_vars(model, works, data)

    # ── Hard Constraints ─────────────────────────────────────────────────
    hstats = apply_hard_constraints(model, works, data, is_working)

    # ── Soft Constraints ─────────────────────────────────────────────────
    soft_terms = apply_soft_constraints(model, works, is_working, data)

    # ── Repair Glue ──────────────────────────────────────────────────────
    repair_parts = None
    if repair_assignments:
        GLUE_WEIGHT = 10_000
        repair_parts = []
        matched = 0
        for vid, sid, rid in repair_assignments:
            k = (vid, sid, rid)
            if k in works:
                repair_parts.append(works[k] * GLUE_WEIGHT)
                matched += 1
            else:
                log.debug("  Repair skip: %s (not in works dict)", k)
        log.info("Repair glue: %d/%d assignments matched (weight=%d each)",
                 matched, len(repair_assignments), GLUE_WEIGHT)

    # ── Objective ────────────────────────────────────────────────────────
    has_objective = build_objective(model, soft_terms, repair_parts)

    # ── Summary ──────────────────────────────────────────────────────────
    proto = model.Proto()
    log.info("Model: %d vars, %d constraints, mode=%s",
             len(proto.variables), len(proto.constraints),
             "OPTIMIZE" if has_objective else "FEASIBILITY")

    # ── Solve ────────────────────────────────────────────────────────────
    log.info("Solving (limit=%.0fs, workers=%d) …", time_limit, num_workers)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = time_limit
    solver.parameters.num_workers = num_workers
    solver.parameters.log_search_progress = log.isEnabledFor(logging.DEBUG)

    status_code = solver.Solve(model)
    status_name = solver.StatusName(status_code)

    log.info("Status: %s  (%.3fs, %d branches, %d conflicts)",
             status_name, solver.wall_time,
             solver.num_branches, solver.num_conflicts)
    if has_objective and status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        log.info("Objective: %.1f  (bound %.1f)",
                 solver.ObjectiveValue(), solver.BestObjectiveBound())

    if status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        _print_schedule(solver, works, data)
        _verify_hard_constraints(solver, works, data, is_working)
        if soft_terms:
            _report_soft_constraints(solver, soft_terms, has_objective)
        if repair_assignments:
            _report_repair(solver, works, repair_assignments, data)
    else:
        log.error("NO FEASIBLE SOLUTION — Status: %s", status_name)

    return status_name, solver, works


# ═══════════════════════════════════════════════════════════════════════════
# Schedule Output
# ═══════════════════════════════════════════════════════════════════════════

def _print_schedule(solver, works, data):
    role_map = {r.id: r for r in data.roles}
    role_abbr = {r.id: "".join(w[0].upper() for w in r.name.split())
                 for r in data.roles}
    total = 0

    log.debug("── Per-Shift Breakdown ──")
    for s in data.shifts:
        log.debug("┌─── %s  [%s]", s.name, s.id)
        log.debug("│    %s → %s  (%.1fh)",
                  s.start_dt.strftime("%a %Y-%m-%d %H:%M"),
                  s.end_dt.strftime("%a %Y-%m-%d %H:%M"), s.duration_hours)
        ra: dict[str, list[Volunteer]] = {}
        for v in data.volunteers:
            for r in data.roles:
                k = (v.id, s.id, r.id)
                if k in works and solver.Value(works[k]) == 1:
                    ra.setdefault(r.id, []).append(v); total += 1
        for rq in s.requirements:
            asgn = ra.get(rq.role_id, [])
            ok = "✓" if rq.min_headcount <= len(asgn) <= rq.max_headcount else "✗"
            log.debug("│  %s %-18s  need=%d  filled=%d",
                      ok, role_map[rq.role_id].name, rq.min_headcount, len(asgn))
            for vol in asgn:
                log.debug("│      → %-10s (%s) exp=%d",
                          vol.name, vol.id, vol.role_experience.get(rq.role_id, 0))
        log.debug("└" + "─" * 60)

    log.debug("── Per-Volunteer Summary ──")
    for v in data.volunteers:
        vs = []
        th = 0.0
        for s in data.shifts:
            for r in data.roles:
                k = (v.id, s.id, r.id)
                if k in works and solver.Value(works[k]) == 1:
                    vs.append((s, r)); th += s.duration_hours
        log.debug("  %-10s (%-6s) — %d shift(s), %.1f hrs",
                  v.name, v.id, len(vs), th)
        for s, r in vs:
            log.debug("      %-18s as %-18s (%.1fh)",
                      s.name, r.id, s.duration_hours)

    log.info("═" * 65)
    log.info("  ASSIGNMENT MATRIX")
    log.info("═" * 65)
    cw = max(len(s.id) for s in data.shifts) + 2
    log.info(f"  {'Name':<12}" + "".join(f"{s.id:>{cw}}" for s in data.shifts))
    log.info("  " + "─" * (12 + cw * len(data.shifts)))
    for v in data.volunteers:
        row = f"  {v.name:<12}"
        for s in data.shifts:
            a = "."
            for r in data.roles:
                if (v.id, s.id, r.id) in works and solver.Value(works[(v.id, s.id, r.id)]) == 1:
                    a = role_abbr[r.id]; break
            row += f"{a:>{cw}}"
        log.info(row)
    log.info("  Total assignments: %d", total)


# ═══════════════════════════════════════════════════════════════════════════
# Hard Constraint Verification
# ═══════════════════════════════════════════════════════════════════════════

def _vol_total_hours(solver, works, vid, shifts, roles):
    return sum(
        s.duration_hours for s in shifts for r in roles
        if (vid, s.id, r.id) in works and solver.Value(works[(vid, s.id, r.id)]) == 1
    )


def _vfy_aggregate(solver, works, data, cd, **kw):
    cond = cd["condition"]
    mn, mx = cond.get("min_hours", 0), cond.get("max_hours", 999999)
    vids = _resolve_subject_vols(cd["subject"], data)
    viols = []
    for v in data.volunteers:
        if v.id not in vids:
            continue
        th = _vol_total_hours(solver, works, v.id, data.shifts, data.roles)
        if th < mn or th > mx:
            viols.append(f"{v.name} ({v.id}): {th:.1f}h NOT in [{mn},{mx}]")
    return viols


def _vfy_rolling(solver, works, data, cd, **kw):
    cond = cd["condition"]
    mh, wh = cond["max_hours_worked"], cond["window_size_hours"]
    vids = _resolve_subject_vols(cd["subject"], data)
    ss = sorted(data.shifts, key=lambda s: s.start_dt)
    groups, seen = [], set()
    for i in range(len(ss)):
        g = [ss[i]]
        for j in range(i + 1, len(ss)):
            if (ss[j].end_dt - ss[i].start_dt).total_seconds() / 3600 <= wh:
                g.append(ss[j])
            else:
                break
        if len(g) >= 2:
            k = tuple(s.id for s in g)
            if k not in seen:
                seen.add(k); groups.append(g)
    viols = []
    for v in data.volunteers:
        if v.id not in vids:
            continue
        for g in groups:
            whr = _vol_total_hours(solver, works, v.id, g, data.roles)
            if whr > mh:
                viols.append(f"{v.name}: {whr:.1f}h in {[s.id for s in g]} > {mh}h")
    return viols


def _vfy_availability(solver, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    us = datetime.fromisoformat(str(cond["start_datetime"]))
    ue = datetime.fromisoformat(str(cond["end_datetime"]))
    vids = _resolve_subject_vols(cd["subject"], data)
    viols = []
    for v in data.volunteers:
        if v.id not in vids:
            continue
        for s in data.shifts:
            if s.start_dt < ue and s.end_dt > us:
                a = any((v.id, s.id, r.id) in works
                        and solver.Value(works[(v.id, s.id, r.id)]) == 1
                        for r in data.roles)
                if enf == "MUST_NOT" and a:
                    viols.append(f"{v.name} in {s.id} during block!")
    return viols


def _vfy_assignment(solver, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    ts, tr = cond.get("target_shift_id", "ANY"), cond.get("target_role_id", "ANY")
    vids = _resolve_subject_vols(cd["subject"], data)
    viols = []
    for vid in vids:
        v = next((v for v in data.volunteers if v.id == vid), None)
        if not v:
            continue
        for s in [s for s in data.shifts if ts == "ANY" or s.id == ts]:
            if tr == "ANY":
                a = any((vid, s.id, r.id) in works
                        and solver.Value(works[(vid, s.id, r.id)]) == 1
                        for r in data.roles)
            else:
                a = (vid, s.id, tr) in works and solver.Value(works[(vid, s.id, tr)]) == 1
            if enf == "MUST" and not a:
                viols.append(f"{v.name} NOT in {s.id}!")
            if enf == "MUST_NOT" and a:
                viols.append(f"{v.name} wrongly in {s.id}!")
    return viols


def _vfy_attribute(solver, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    tr, ts = cond.get("target_role_id", "ANY"), cond.get("target_shift_id", "ANY")
    filt = _resolve_subject_vols(cd["subject"], data)
    viols = []
    for vid in filt:
        v = next((v for v in data.volunteers if v.id == vid), None)
        if not v:
            continue
        for s in [s for s in data.shifts if ts == "ANY" or s.id == ts]:
            for r in [r for r in data.roles if tr == "ANY" or r.id == tr]:
                k = (vid, s.id, r.id)
                if k in works and enf == "MUST_NOT" and solver.Value(works[k]) == 1:
                    viols.append(f"{v.name} as {r.id} in {s.id}!")
    return viols


def _vfy_pairing(solver, works, data, cd, **kw):
    enf = cd["enforcement"]
    vol_ids = [vid for vid in cd["subject"].get("volunteer_ids", [])
               if vid in {v.id for v in data.volunteers}]
    if len(vol_ids) < 2:
        return []
    vname = {v.id: v.name for v in data.volunteers}
    viols = []
    for s in data.shifts:
        working = [any((vid, s.id, r.id) in works
                       and solver.Value(works[(vid, s.id, r.id)]) == 1
                       for r in data.roles) for vid in vol_ids]
        if enf == "MUST" and any(working) and not all(working):
            w = [vname[vol_ids[i]] for i, x in enumerate(working) if x]
            nw = [vname[vol_ids[i]] for i, x in enumerate(working) if not x]
            viols.append(f"{s.id}: {w} work but {nw} don't!")
        elif enf == "MUST_NOT" and sum(working) > 1:
            w = [vname[vol_ids[i]] for i, x in enumerate(working) if x]
            viols.append(f"{s.id}: {w} all work!")
    return viols


def _vfy_minimum_rest(solver, works, data, cd, **kw):
    if cd["enforcement"] != "MUST":
        return []
    cond = cd["condition"]
    min_rest_h = float(cond["min_rest_hours"])
    vids = _resolve_subject_vols(cd["subject"], data)
    pairs = _find_rest_violation_pairs(data.shifts, min_rest_h)
    viols = []
    for s1, s2 in pairs:
        gap = (s2.start_dt - s1.end_dt).total_seconds() / 3600
        for v in data.volunteers:
            if v.id not in vids:
                continue
            a1 = any((v.id, s1.id, r.id) in works
                     and solver.Value(works[(v.id, s1.id, r.id)]) == 1
                     for r in data.roles)
            a2 = any((v.id, s2.id, r.id) in works
                     and solver.Value(works[(v.id, s2.id, r.id)]) == 1
                     for r in data.roles)
            if a1 and a2:
                viols.append(f"{v.name}: {s1.id}&{s2.id} gap={gap:.1f}h!")
    return viols


def _vfy_shift_composition(solver, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    ts = cond.get("target_shift_id", "ANY")
    fa, fo, fv = cond["filter_attribute"], cond["filter_operator"], cond["filter_value"]
    mn, mx = int(cond.get("min_count", 0)), int(cond.get("max_count", 999))
    matching = _filter_vols_by_attr(data, fa, fo, fv)
    viols = []
    for s in [s for s in data.shifts if ts == "ANY" or s.id == ts]:
        cnt = sum(1 for vid in matching
                  if any((vid, s.id, r.id) in works
                         and solver.Value(works[(vid, s.id, r.id)]) == 1
                         for r in data.roles))
        if enf == "MUST" and (cnt < mn or cnt > mx):
            viols.append(f"{s.id}: {cnt} matching NOT in [{mn},{mx}]!")
        elif enf == "MUST_NOT" and mn <= cnt <= mx:
            viols.append(f"{s.id}: {cnt} matching IS in [{mn},{mx}]!")
    return viols


def _vfy_shift_span(solver, works, data, cd, **kw):
    enf = cd["enforcement"]
    cond = cd["condition"]
    max_span_h = int(cond.get("max_span_hours", 999))
    vids = _resolve_subject_vols(cd["subject"], data)
    viols = []
    for v in data.volunteers:
        if v.id not in vids:
            continue
        worked = [s for s in data.shifts
                  if any((v.id, s.id, r.id) in works
                         and solver.Value(works[(v.id, s.id, r.id)]) == 1
                         for r in data.roles)]
        if not worked:
            continue
        first = min(s.start_dt for s in worked)
        last = max(s.end_dt for s in worked)
        span_h = (last - first).total_seconds() / 3600
        if enf == "MUST" and span_h > max_span_h:
            viols.append(f"{v.name}: span={span_h:.1f}h > {max_span_h}h!")
        elif enf == "MUST_NOT" and span_h <= max_span_h:
            viols.append(f"{v.name}: span={span_h:.1f}h ≤ {max_span_h}h (must_not)!")
    return viols


_VFY: dict[str, Any] = {
    "aggregate_hours": _vfy_aggregate, "rolling_window": _vfy_rolling,
    "availability": _vfy_availability, "assignment": _vfy_assignment,
    "attribute": _vfy_attribute, "pairing": _vfy_pairing,
    "minimum_rest": _vfy_minimum_rest, "shift_composition": _vfy_shift_composition,
    "shift_span": _vfy_shift_span,
}


def _verify_hard_constraints(solver, works, data, is_working):
    all_ok = True

    log.debug("── Headcount Audit ──")
    for s in data.shifts:
        for rq in s.requirements:
            f = sum(1 for v in data.volunteers
                    if (v.id, s.id, rq.role_id) in works
                    and solver.Value(works[(v.id, s.id, rq.role_id)]) == 1)
            ok = rq.min_headcount <= f <= rq.max_headcount
            log.debug("  [%s] %-10s/%-18s filled=%d need=[%d,%d]",
                      "✓" if ok else "✗", s.id, rq.role_id,
                      f, rq.min_headcount, rq.max_headcount)
            if not ok:
                all_ok = False

    log.debug("── YAML Hard Constraints ──")
    for idx, cd in enumerate(data.constraints):
        enf = cd.get("enforcement", "")
        if enf not in ("MUST", "MUST_NOT"):
            continue
        ctype = cd.get("type", "?")
        desc = cd.get("description", ctype)
        fn = _VFY.get(ctype)
        viols = fn(solver, works, data, cd, is_working=is_working) if fn else []
        if viols:
            all_ok = False
            log.error("  [✗] C%d %-18s '%s'", idx, ctype, desc)
            for vl in viols:
                log.error("       %s", vl)
        else:
            log.debug("  [✓] C%d %-18s '%s'", idx, ctype, desc)

    if all_ok:
        log.info("Hard constraint verification: ALL PASSED ✓")
    else:
        log.error("HARD CONSTRAINT VIOLATIONS DETECTED")


# ═══════════════════════════════════════════════════════════════════════════
# Soft Constraint Report
# ═══════════════════════════════════════════════════════════════════════════

def _report_soft_constraints(solver, soft_terms, has_objective):
    log.info("═" * 65)
    log.info("  SOFT CONSTRAINT RESULTS")
    log.info("═" * 65)
    total_earned = 0
    n_ok = 0
    n_broken = 0
    by_cidx: dict[int, list[SoftTerm]] = {}
    for st in soft_terms:
        by_cidx.setdefault(st.constraint_idx, []).append(st)
    for cidx in sorted(by_cidx.keys()):
        terms = by_cidx[cidx]
        first = terms[0]
        c_earned, c_ok, c_broken = 0, 0, 0
        for st in terms:
            val = solver.Value(st.indicator)
            if st.is_penalty:
                if st.enforcement == "PREFER":
                    earned = -st.weight * val; satisfied = (val == 0)
                else:
                    earned = st.weight * val; satisfied = (val == 0)
            else:
                if st.enforcement == "PREFER":
                    satisfied = (val == 1); earned = st.weight if satisfied else 0
                else:
                    satisfied = (val == 0); earned = 0 if satisfied else -st.weight
            mark = "✓" if satisfied else "✗"
            log.debug("    [%s] %-50s val=%d %+d", mark, st.description, val, earned)
            c_earned += earned
            if satisfied:
                c_ok += 1
            else:
                c_broken += 1
        total_earned += c_earned
        n_ok += c_ok
        n_broken += c_broken
        log.info("  C%d  %-18s  %s  w=%d  → %+d  (%d✓ %d✗)",
                 cidx, first.enforcement,
                 "penalty" if first.is_penalty else "indicator",
                 first.weight, c_earned, c_ok, c_broken)

    log.info("═" * 65)
    log.info("  SOFT SCORE SUMMARY")
    log.info("═" * 65)
    log.info("  Satisfied : %d / %d", n_ok, n_ok + n_broken)
    log.info("  Broken    : %d / %d", n_broken, n_ok + n_broken)
    log.info("  Earned    : %+d", total_earned)
    if has_objective:
        log.info("  Objective : %.1f", solver.ObjectiveValue())
        log.info("  Bound     : %.1f", solver.BestObjectiveBound())


# ═══════════════════════════════════════════════════════════════════════════
# Repair Report
# ═══════════════════════════════════════════════════════════════════════════

def _report_repair(solver, works, repair_assignments, data):
    """Log which previous assignments were preserved vs changed."""
    vname = {v.id: v.name for v in data.volunteers}
    preserved = 0
    changed = 0
    for vid, sid, rid in repair_assignments:
        k = (vid, sid, rid)
        if k in works and solver.Value(works[k]) == 1:
            preserved += 1
        else:
            changed += 1
            log.debug("  Repair changed: %s was %s as %s in %s",
                      vname.get(vid, vid), vid, rid, sid)
    total = preserved + changed
    log.info("═" * 65)
    log.info("  REPAIR SUMMARY")
    log.info("═" * 65)
    log.info("  Preserved : %d / %d  (%.1f%%)",
             preserved, total, 100.0 * preserved / total if total else 0)
    log.info("  Changed   : %d / %d", changed, total)


# ═══════════════════════════════════════════════════════════════════════════
# CSV Export
# ═══════════════════════════════════════════════════════════════════════════

def export_csv(solver, works, data, output_path):
    role_map = {r.id: r for r in data.roles}
    seen_roles: list[str] = []
    for s in data.shifts:
        for rq in s.requirements:
            if rq.role_id not in seen_roles:
                seen_roles.append(rq.role_id)
    header = ["Shift", "Start", "End", "Duration"]
    header.extend(role_map[rid].name for rid in seen_roles)
    sorted_shifts = sorted(data.shifts, key=lambda s: s.start_dt)
    rows = []
    for s in sorted_shifts:
        row = [
            s.name,
            s.start_dt.strftime("%Y-%m-%d %H:%M"),
            s.end_dt.strftime("%Y-%m-%d %H:%M"),
            f"{s.duration_hours:.1f}h",
        ]
        for rid in seen_roles:
            names = []
            for v in data.volunteers:
                k = (v.id, s.id, rid)
                if k in works and solver.Value(works[k]) == 1:
                    names.append(v.name)
            row.append(", ".join(sorted(names)) if names else "—")
        rows.append(row)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    log.info("Schedule exported → %s  (%d shifts × %d roles)",
             path, len(rows), len(seen_roles))


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    default_threads = max(1, multiprocessing.cpu_count() // 4)

    parser = argparse.ArgumentParser(
        prog="scheduler",
        description="Employee/Volunteer Scheduler — "
                    "constraint-based scheduling powered by OR-Tools CP-SAT",
    )
    parser.add_argument(
        "input_yaml",
        help="Path to a YAML config file or a directory of .yaml/.yml files",
    )
    parser.add_argument(
        "--output", "-o",
        default="schedule.csv",
        help="Output CSV file path (default: schedule.csv)",
    )
    parser.add_argument(
        "--time-limit", "-t",
        type=float,
        default=60.0,
        help="Solver time limit in seconds (default: 60)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=default_threads,
        help=f"Number of solver worker threads (default: {default_threads}, "
             f"= cpu_count/4)",
    )
    parser.add_argument(
        "--repair",
        nargs="?",
        const="schedule.csv",
        default=None,
        metavar="FILE",
        help="Path to a previous schedule CSV to preserve assignments from. "
             "If flag is given without a path, defaults to 'schedule.csv'.",
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Enable DEBUG-level logging (verbose output)",
    )
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()
    _setup_logging(args.debug)

    log.info("═" * 65)
    log.info("  Employee/Volunteer Scheduler")
    log.info("═" * 65)
    log.info("  Input   : %s", args.input_yaml)
    log.info("  Output  : %s", args.output)
    log.info("  Limit   : %.0fs", args.time_limit)
    log.info("  Threads : %d", args.threads)
    if args.repair:
        log.info("  Repair  : %s", args.repair)
    log.info("  Debug   : %s", args.debug)

    data = parse_yaml_file(args.input_yaml)

    repair_assignments = None
    if args.repair:
        repair_assignments = parse_repair_csv(args.repair, data)

    status, solver, works = build_and_solve(
        data, args.time_limit, args.threads, repair_assignments)

    if status in ("OPTIMAL", "FEASIBLE"):
        export_csv(solver, works, data, args.output)

    log.info("═" * 65)
    log.info("  FINISHED — %s", status)
    log.info("═" * 65)


if __name__ == "__main__":
    main()
