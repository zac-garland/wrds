# Notes folder cleanup recommendation

**Current state:** ~1.4 GB in `notes/`, 44 files at root plus `cache/` and a nested `notes/notes/`. Mix of active project notebooks, one-off R scripts, large data/cache files, and duplicates.

---

## 1. Remove or relocate the nested `notes/notes/` folder

**Issue:** You have `notes/notes/` with duplicate outputs and very large workspace pickles.

| Item | Size | Recommendation |
|------|------|----------------|
| `portfolio_contributions_viz.html` | ~640 KB | Duplicate of file generated into `notes/` by the notebook. **Delete** (notebook writes to `notes/` when run from project root). |
| `portfolio_contributions_to_return.csv` | ~4 MB | Same; notebook writes to `notes/`. **Delete** or keep one copy in `notes/`. |
| `merged_data_workspace.pkl` | ~136 MB | Old workspace snapshot. **Move to `trash/` or delete** if no longer needed. |
| `merged_data_workspace_new.pkl` | ~439 MB | Same. **Move to `trash/` or delete** if regenerable from the notebook. |

**Action:** Delete or move the two `.pkl` files to `../trash/` (or remove them). Delete the duplicate CSV and HTML from `notes/notes/`. Then remove the empty `notes/notes/` directory (or keep the folder only if you use it for something else).

---

## 2. Move large data and workspace files out of `notes/`

**Issue:** Big CSV/pickle/RData files live next to code and bloat the folder.

| File | Size | Recommendation |
|------|------|----------------|
| `analysis_data_norm.csv` | ~229 MB | Move to `../data/` or a `notes/data/` subfolder. Update notebook path if you use a literal path. |
| `merged_data_workspace.pkl` | ~315 MB | Move to `../data/` or `notes/data/` (or trash if obsolete). |
| `merged_data_workspace.RData` | ~70 MB | Same. |
| `merged_data_workspace_ex_healthcare.RData` | ~59 MB | Same. |

**Action:** Create `notes/data/` (or use project `data/`) for these. Move the files and fix any paths in notebooks/scripts. Add `notes/data/` to `.gitignore` if you don’t want it in version control (project already ignores `/data` and `/notes`).

---

## 3. Consolidate duplicate and legacy notebooks

**Issue:** Several notebook “families” with multiple versions; unclear which is canonical.

| Group | Files | Recommendation |
|-------|--------|----------------|
| Project 1 quintiles | `project_1_quintiles.ipynb`, `project_1_quintiles_backup.ipynb`, `project_1_quintiles_v3.ipynb` | Keep **`project_1_quintiles_v3.ipynb`** as main. Move `project_1_quintiles_backup.ipynb` to `notes/archive/` or delete. Consider archiving the older `project_1_quintiles.ipynb` if v3 has replaced it. |
| Reprex | `reprex_final.ipynb`, `reprex_final_extended.ipynb`, `reprex_final_extended_tuned.ipynb`, `reprex_eda.ipynb` | Keep the one you use (e.g. `reprex_final_extended_tuned.ipynb`). Move the others to `notes/archive/` or a `notes/reprex/` subfolder. |
| NickFavoriti Project0 | `NickFavoriti_Project0.ipynb`, `NickFavoriti_Project0_lnkhist_lag.ipynb`, `NickFavoriti_Project0_our_outputs.ipynb` | Keep the one you need; archive the rest (e.g. under `notes/archive/` or `notes/project0/`). |
| R&D reprex | `rd_reprex_final.R`, `rd_reprex_finalR.PY`, `rd_reprex_finalR.ipynb` | Same content in three forms. Keep the format you use (e.g. `.ipynb`); put the other two in `notes/archive/` or delete. |
| Recoded Project 0 | `recoded_project_0.ipynb` | Archive if superseded by another Project 0 notebook. |

**Action:** Create `notes/archive/` and move older/duplicate notebooks there. Optionally group by project (e.g. `notes/archive/project1/`, `notes/archive/reprex/`).

---

## 4. Group one-off and assignment scripts

**Issue:** Many small R scripts at top level (assignments, exploration, merge tests).

- **Assignments:** `assignment 1.R`, `assignment 1_zg_reprex.R`, `assignment_1_reprex.R`, `assignment 2.R`, `assignment 2 refactored.R`  
  → Move to `../assignments/` (you already have that folder) or to `notes/assignments/`.

- **Exploration / one-offs:** `explore project 1.R`, `wharton explore.R`, `check table access.R`, `manual opt check.R`, `test_merge_step_by_step.R`, `fut series.R`, `shorts and long.R`, `asset_cor.R`, `contr to growth project 1 emp finance.R`  
  → Move to `notes/scratch/` or `notes/exploration/` so the root of `notes/` is mostly current project notebooks + a few key scripts.

- **Merge / linking:** `compustat_crsp_link_sas_to_r.R`, `merge_compustat_crsp_simple_ccm_first_run.R`, `constructing_custom_equity_indexes.R`, `project 1 empirical finance.R`  
  → Keep in `notes/` or move to a single `notes/merge_and_data/` (or similar) if you want a clear “data pipeline” area.

**Action:** Create `notes/scratch/` (or `notes/exploration/`) and `notes/assignments/` (or rely on `../assignments/`). Move files accordingly; fix any cross-references.

---

## 5. Naming and small cleanups

- **Spaces in filenames:** e.g. `assignment 1.R`, `project 1 empirical finance.R`, `contr to growth project 1 emp finance.R`. Consider renaming to `assignment_1.R`, `project_1_empirical_finance.R` for scripts you keep (avoids quoting in terminals and some tools). Do this only where you’re sure nothing references the old names.
- **`notes/cache/`:** Already used by the notebook (~198 MB). Keep it; ensure `.gitignore` continues to ignore cache (project has `/cache/`).
- **Generated outputs:** `notes/portfolio_contributions_viz.html` and `notes/portfolio_contributions_to_return.csv` are produced by the notebook. Optionally add `notes/portfolio_contributions*` to `.gitignore` if you don’t want to track them (or keep them for quick viewing).
- **Other:** `custom_indexes_plot.png` could live in `../figs/` if it’s a shared figure. `python-example.Rmd` can stay or move to `notes/archive/` if obsolete.

---

## 6. Suggested layout after cleanup

```
notes/
├── CLEANUP_RECOMMENDATION.md     # this file
├── project_1_quintiles_v3.ipynb  # main project 1 notebook
├── reprex_final_extended_tuned.ipynb  # (or whichever reprex you use)
├── portfolio_contributions_viz.html    # generated
├── portfolio_contributions_to_return.csv  # generated
├── cache/                       # keep; notebook cache
├── data/                        # optional: analysis_data_norm.csv, workspace pkls
├── archive/                     # old/backup notebooks and scripts
│   ├── project1/
│   ├── reprex/
│   └── ...
├── scratch/                     # exploration / one-off R scripts
└── (a few key R scripts you use often)
```

---

## 7. Quick wins (minimal change)

If you want the smallest set of changes:

1. **Delete or move** everything in `notes/notes/` (two large `.pkl` files + duplicate CSV/HTML), then remove the `notes/notes/` directory.
2. **Create** `notes/archive/` and move `project_1_quintiles_backup.ipynb`, `project_1_quintiles.ipynb` (if v3 is canonical), and any reprex/NickFavoriti notebooks you don’t use.
3. **Leave** large CSV/pkl/RData in place for now but add a note (e.g. in this file) that they are candidates for moving to `data/` or for deletion when no longer needed.

That alone reduces clutter and removes ~750 MB from the nested folder.
