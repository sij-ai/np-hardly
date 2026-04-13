# NP-Hardly 🏕️📋
> *Working NP-Hard or is NP-Hardly Working?*

**NP-Hardly** is a constraint-based scheduling engine built on Google OR-Tools (CP-SAT). 

I originally built this so my dad could stop spending his summers manually cross-referencing index cards to schedule a 35-person volunteer crew for a 6-day festival. It translates human scheduling headaches into linear algebra.

### The Approach
* **Feasible vs. Optimal:** It doesn't just randomly guess until it finds a schedule that works (`FEASIBLE`). Given enough time, the solver evaluates the mathematical bounds of your rules to prove that no higher-scoring schedule can possibly exist (`OPTIMAL`).
* **Zero UI:** Building and maintaining a web app for an annual event is usually more trouble than it's worth. You write the rules in plain YAML; it spits out a CSV spreadsheet.
* **Consistent Logic:** It handles rolling windows, minimum rest periods, and shift composition requirements exactly the same way every time, without fatigue.

### Core Features
* **Modular YAML Configs:** Keep your data organized. You can pass the engine a single `project.yaml` file, or an entire directory. It will automatically stitch `shifts.yaml`, `volunteers.yaml`, and `constraints.yaml` together at runtime.
* **The "Wyatt" Protocol (Minimal Perturbation):** When a volunteer texts you a week before the event to say they can't make their shift, you don't want the engine to generate a brand-new schedule that completely upends 20 other people's weekends. Pass the `--repair` flag along with your old schedule. 
  *(Under the hood, the engine parses your old CSV and applies a +10,000 point "digital glue" reward to every existing assignment. It is mathematically forced to break the absolute minimum number of old assignments to make the new constraints fit).*

### A Quick Taste
No coding required. Constraints are written in plain, human-readable YAML:

```yaml
- type: "shift_composition"
  description: "Every shift MUST have at least 1 Veteran"
  enforcement: "MUST"
  subject: {volunteer_ids: ["ANY"]}
  condition:
    target_shift_id: "ANY"
    filter_attribute: "role_experience.camp_rover"
    filter_operator: "GREATER_THAN_OR_EQUAL"
    filter_value: 4
    min_count: 1
```

---

## Installation

Requires Python 3.9 or higher. Using a virtual environment is highly recommended.

```bash
git clone ssh://git@git.sij.ai:222/sij/np-hardly.git
cd np-hardly
python3 -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
```

---

## Usage Guide

### 1. Generate a Base Schedule
Pass a YAML configuration file to the engine. By default, it will use 1/4 of your CPU cores and run for 60 seconds.
```bash
python np-hardly.py examples/1_monolith/festival_base.yaml -o schedule.csv --threads 8
```

### 2. Repairing a Schedule (The Wyatt Protocol)
Imagine you just published `schedule.csv` to your crew. Wyatt calls and says he can't arrive until Thursday. 

Instead of rewriting the schedule by hand, write a quick `wyatt_update.yaml` constraint blocking his availability, and drop it into the exact same folder as your base configuration. Pass the **entire folder** to the engine, along with the `--repair` flag pointing to your original CSV:

```bash
python np-hardly.py examples/1_monolith/ \
  --repair schedule.csv \
  -o repaired_schedule.csv \
  --threads 8
```
The engine merges the base rules with Wyatt's new restriction, applies the digital glue to the old CSV, and outputs a `repaired_schedule.csv` (along with a printed summary of exactly how few assignments it had to change).

### 3. Fully Modular YAML
For larger events, you can break your data into as many files as you want (e.g., `01_project.yaml`, `02_shifts.yaml`, `03_volunteers.yaml`, `04_rules.yaml`). The engine stitches them all together seamlessly:
```bash
python np-hardly.py examples/2_modular/ -o schedule.csv
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
9. **`shift_span`**: Minimizes the chronological gap between a volunteer's first clock-in and last clock-out so they aren't stuck on-site for 6 days for 2 shifts.
