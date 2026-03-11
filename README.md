# Meridian MMM Plan-only Dashboard (Local Excel)

This version:
- **Hides** "Marketing analysis results" and "Explorer" menus.
- Focuses only on **Plan**.
- Reads MMM scenario planner export from a **local Excel file** (default: `mmm_data_download.xlsx`).

## Required sheets inside the Excel
- `budget_opt_specs`
- `budget_opt_grid_*` (e.g. `budget_opt_grid_poc-mmm_ALL`)
Optional: `budget_opt_results`

## Run
```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Data file
Put your downloaded file next to `streamlit_app.py` with name:
- `mmm_data_download.xlsx`

Or use the uploader in the app to select the Excel file.
