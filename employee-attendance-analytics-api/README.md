# Employee Attendance & Analytics API

FastAPI + MongoDB service for punch-in/punch-out, manual corrections with an audit trail, and analytics built on aggregation pipelines. All application code is in `app/main.py`.

## Run it

Requires Python 3.11+ and MongoDB 6.0+.

From the `hrone-attendance` directory (the directory containing `app` and `requirements.txt`):

**Windows PowerShell**

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker run -d -p 27017:27017 --name mongo mongo:7  # or use a MongoDB Atlas URI
$env:MONGO_URI = "mongodb://localhost:27017"
$env:MONGO_DB = "attendance_db"
.\.venv\Scripts\python.exe -m uvicorn app.main:app --port 8000
```

Using `python -m uvicorn` via the virtual environment's Python avoids stale Windows executable launchers if the project folder has been moved. If your terminal is in the parent directory, first run `cd .\hrone-attendance`.

If your prompt is currently `C:\Users\DHANUSH\Downloads\employee-attendance-analytics-api>`:

1. Start the API from that prompt with:

   ```powershell
   Set-Location .\hrone-attendance
   .\.venv\Scripts\python.exe -m uvicorn app.main:app --port 8000
   ```

2. In a second PowerShell window, run the self-check from the parent directory with:

   ```powershell
   .\hrone-attendance\.venv\Scripts\python.exe .\hrone-attendance\dev_tools\check_api.py
   ```

**macOS / Linux**

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker run -d -p 27017:27017 --name mongo mongo:7  # or use a MongoDB Atlas URI
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="attendance_db"
uvicorn app.main:app --port 8000
```

`MONGO_URI` and `MONGO_DB` are read from the environment (a local `.env` is also loaded as a fallback; real environment variables win). Interactive docs: http://localhost:8000/docs

All indexes are created idempotently at startup.

## Status

All endpoints in the contract are implemented: employees, punch-in, punch-out, PATCH corrections with history, attendance listing, the four analytics endpoints, `/health` and `/admin/explain/{endpoint}`. Nothing is knowingly missing.

## Decisions on ambiguous points

- `attendance_rate` is a fraction (`present_count / headcount`). `moving_avg_7d` averages the already-rounded 4-decimal rates in the window.
- Trend range limit: at most 92 calendar days, counted inclusively.
- A PATCH that loses a race with another write returns 409 (nothing is overwritten and no history entry is lost). A PATCH that changes nothing returns 422.
- Punch-out closes the most recent record whose `punch_in` is at or before `punched_at`; 404 if none exists, 409 if that record is already closed.
- Rounding is done with integer arithmetic (half-up), never with Python's `round()` or Mongo's `$round`, which round half to even.
- Database failures return 503 rather than 500.

## Optional self-check

`dev_tools/check_api.py` seeds its own awkward data (ties, an employee with no logs, a weekend punch, a mid-month joiner, an overnight shift), recomputes the expected analytics independently in plain Python, compares them with the API, and fires concurrent requests to test the races. Run it with the API up: `python dev_tools/check_api.py`.
