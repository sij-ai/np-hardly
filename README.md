# NP-Hardly 🏕️📋
> *Working NP-Hard or is NP-Hardly Working?*

**NP-Hardly** is a highly strictly constrained, CP-SAT automated scheduling engine built on Google OR-Tools. 

I vibe-coded this in an afternoon to stop my dad from spending a month manually cross-referencing index cards to schedule a 35-person volunteer security crew for a 6-day continuous festival. It replaces human suffering with linear algebra.

### The Philosophy
* **Strict Mathematical Proofs:** If a schedule works, it is mathematically proven to be optimal. 
* **Zero UI:** Because building a web app for an annual event is a trap. You write YAML, it writes CSVs.
* **Unforgiving Logic:** Handles rolling windows, minimum rest periods, and shift composition requirements with ruthless efficiency.

### Killer Features
* **Modular YAML Compilation:** Keep your data organized. You can pass the engine a single `project.yaml` file, OR pass it an entire directory. It will automatically stitch `shifts.yaml`, `volunteers.yaml`, and `constraints.yaml` together at runtime.
* **The "Wyatt" Protocol (Minimal Perturbation):** When a volunteer texts you a week before the event to say they can't make their shift, you don't want to generate a brand new schedule that upends 20 other people's weekends. Pass the `--repair` flag with your old schedule, and the engine will surgically fix the hole by moving the absolute bare minimum number of assignments.

---

## Installation

Requires Python 3.9 or higher.

```bash
git clone https://git.sij.ai/sij/np-hardly.git
cd np-hardly
pip install -r requirements.txt
```

## Usage

### 1. Generate a Base Schedule
Pass a YAML configuration file to the engine. By default, it will use 1/4 of your CPU cores and run for 60 seconds.
```bash
python np-hardly.py examples/festival_base.yaml -o schedule.csv --threads 8
```

### 2. Stitching a Directory
Instead of a monolithic file, pass a directory of YAMLs. The engine will merge them perfectly.
```bash
python np-hardly.py my_event_data/ -o schedule.csv
```

### 3. Repairing a Schedule (The Wyatt Protocol)
Wyatt bailed on his Wednesday shifts. You wrote a quick `wyatt_update.yaml` constraint blocking his availability. Apply it to the existing schedule using `--repair` to minimize butterfly-effect disruptions:
```bash
python np-hardly.py examples/festival_base.yaml examples/wyatt_update.yaml \
  --repair schedule.csv \
  -o repaired_schedule.csv
```

---

## Constraint Types Supported
The engine supports 9 specific constraint types. Every constraint can be enforced as `MUST`, `MUST_NOT`, `PREFER`, or `PREFER_NOT` (with adjustable point weights for the soft constraints).

1. **`aggregate_hours`**: Min/Max total hours worked by a volunteer.
2. **`rolling_window`**: E.g., Max 8 hours of work in any 16-hour rolling window.
3. **`minimum_rest`**: E.g., Mandatory 12-hour gap between shifts.
4. **`shift_composition`**: E.g., Every shift MUST have at least 1 Veteran, and PREFERS not to have >3 Rookies.
5. **`pairing`**: Force (or prevent) two specific volunteers from working the same shifts.
6. **`availability`**: Block out specific date/time ranges for late arrivals.
7. **`assignment`**: Force a specific person to work a specific role on a specific shift.
8. **`attribute`**: E.g., Only volunteers with `camp_taxi >= 2` experience can be assigned to the Taxi role.
9. **`shift_span`**: Minimizes the chronological gap between a volunteer's very first clock-in and very last clock-out so they aren't stranded on-site for 6 days for 2 shifts.

