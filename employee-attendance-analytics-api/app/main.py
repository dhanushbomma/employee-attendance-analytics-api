
import json
import logging
import os
import re
import threading
import time as time_module
from calendar import monthrange
from contextlib import asynccontextmanager
from datetime import date as date_type
from datetime import datetime, time as time_type, timedelta, timezone
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError, ServerSelectionTimeoutError

# --------------------------------------------------------------------------
# 1. Configuration
# --------------------------------------------------------------------------
load_dotenv()  

log = logging.getLogger("attendance")

MONGO_URI = os.environ.get("MONGO_URI") or "mongodb://localhost:27017"
MONGO_DB = os.environ.get("MONGO_DB") or "attendance_db"


client = MongoClient(
    MONGO_URI, tz_aware=True, tzinfo=timezone.utc, serverSelectionTimeoutMS=5000
)
db = client[MONGO_DB]
employees = db["employees"]
logs = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")  
NON_PRESENCE = ("ABSENT", "LEAVE")
Status = Literal["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"]
PunchStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60  
OVERTIME_MIN_MINUTES = 30  
HALF_DAY_BELOW = 4.50  
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

_indexes_ready = False
_index_lock = threading.Lock()


def ensure_indexes() -> bool:
    """Create every index we rely on. Idempotent: safe to call on every start."""
    global _indexes_ready
    with _index_lock:
        if _indexes_ready:
            return True
        specs = [
            # employees
            (employees, [("emp_code", 1)], {"unique": True, "name": "uniq_emp_code"}),
            (employees, [("department", 1), ("emp_code", 1)], {"name": "dept_emp"}),
            (employees, [("department", 1), ("joined_on", 1)], {"name": "dept_joined"}),
            (employees, [("joined_on", 1)], {"name": "joined"}),
            
            (logs, [("emp_code", 1), ("date", 1)], {"unique": True, "name": "uniq_emp_date"}),
           
            (logs, [("date", -1), ("emp_code", 1), ("late_minutes", 1)], {"name": "date_emp_late"}),
       
            (logs, [("status", 1), ("date", -1), ("emp_code", 1)], {"name": "status_date_emp"}),
           
            (logs, [("emp_code", 1), ("punch_in", -1)], {"name": "emp_punch_in"}),
        ]
        ok = True
        for coll, keys, opts in specs:
            try:
                coll.create_index(keys, **opts)
            except ServerSelectionTimeoutError as exc:
                log.error("database not reachable, will retry in background: %s", exc)
                return False 
            except PyMongoError as exc:
                ok = False
                log.error("index %s failed: %s", opts.get("name"), exc)
        _indexes_ready = ok
        return ok


def _index_retry_worker() -> None:
    while not _indexes_ready:
        time_module.sleep(3)
        try:
            ensure_indexes()
        except Exception:  
            pass


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        done = ensure_indexes()
    except Exception as exc:
        log.error("could not create indexes at startup: %s", exc)
        done = False
    if not done: 
        threading.Thread(target=_index_retry_worker, daemon=True).start()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="1.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_request: Request, exc: PyMongoError):
    log.error("database error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database error"})


# --------------------------------------------------------------------------
# 2. Helpers
# --------------------------------------------------------------------------
def unprocessable(loc: list, msg: str):
    """Raise a 422 in the same shape FastAPI uses for its own validation errors."""
    raise RequestValidationError(
        [{"type": "value_error", "loc": loc, "msg": msg, "input": None}]
    )


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def ms_to_dt(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)  # exact integer arithmetic


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    return (aware(dt) - EPOCH) // timedelta(milliseconds=1)


def trunc_seconds(dt: datetime) -> datetime:
    return aware(dt).replace(microsecond=0)  # R1: whole seconds only


def now_utc() -> datetime:
    n = datetime.now(UTC)
    return n.replace(microsecond=(n.microsecond // 1000) * 1000)  # BSON keeps ms


DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
MONTH_RE = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")


def parse_date(value: str) -> date_type:
    if not DATE_RE.fullmatch(value):
        raise ValueError("date must look like YYYY-MM-DD")
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_or_422(value: str, loc: list) -> date_type:
    try:
        return parse_date(value)
    except ValueError:
        unprocessable(loc, "must be a real calendar date in YYYY-MM-DD format")


def month_bounds(month: str, loc: list) -> tuple[str, str]:
    """('2026-07-01', '2026-07-31') for '2026-07'."""
    if not MONTH_RE.fullmatch(month):
        unprocessable(loc, "month must look like YYYY-MM")
    year, mon = int(month[:4]), int(month[5:])
    if year < 1:
        unprocessable(loc, "invalid year")
    return f"{month}-01", f"{month}-{monthrange(year, mon)[1]:02d}"


def half_up_div(num: int, den: int) -> int:
    """round-half-up(num / den) for non-negative integers, with no float error."""
    return (2 * num + den) // (2 * den)


def hhmm(value: str) -> time_type:
    return time_type(int(value[:2]), int(value[3:5]))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares correctly as text


def attendance_date(punch_in: datetime, emp: dict) -> str:
    """R1: the IST calendar day a punch-in belongs to.

    For an overnight shift, a punch-in earlier than shift_end on the clock belongs
    to the shift that started the previous day.
    """
    ist = aware(punch_in).astimezone(IST)
    day = ist.date()
    if is_overnight(emp["shift_start"], emp["shift_end"]):
        if ist.time().replace(tzinfo=None) < hhmm(emp["shift_end"]):
            day -= timedelta(days=1)
    return day.isoformat()


def shift_start_dt(date_str: str, emp: dict) -> datetime:
    return datetime.combine(parse_date(date_str), hhmm(emp["shift_start"]), tzinfo=IST)


def shift_end_dt(date_str: str, emp: dict) -> datetime:
    day = parse_date(date_str)
    if is_overnight(emp["shift_start"], emp["shift_end"]):
        day += timedelta(days=1)
    return datetime.combine(day, hhmm(emp["shift_end"]), tzinfo=IST)


def calc_late_minutes(punch_in: datetime, date_str: str, emp: dict) -> int:
    """R2: 0 unless MORE than 10:00 after shift start; then minutes from shift start."""
    seconds = (punch_in - shift_start_dt(date_str, emp)) // timedelta(seconds=1)
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def calc_overtime_minutes(punch_out: datetime, date_str: str, emp: dict) -> int:
    """R3: whole minutes after shift end, counted only if at least 30."""
    seconds = (punch_out - shift_end_dt(date_str, emp)) // timedelta(seconds=1)
    minutes = seconds // 60 if seconds > 0 else 0
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0


def calc_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: hours rounded half-up to 2 decimals using integer maths (never round())."""
    seconds = (punch_out - punch_in) // timedelta(seconds=1)
    hundredths = half_up_div(seconds * 100, 3600)
    return hundredths / 100


def derive_fields(status: str, punch_in, punch_out, date_str: str, emp: dict) -> dict:
    """All stored derived fields for a record."""
    if status in NON_PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {
        "work_hours": None,
        "late_minutes": calc_late_minutes(punch_in, date_str, emp),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        hours = calc_work_hours(punch_in, punch_out)
        out["work_hours"] = hours
        out["overtime_minutes"] = calc_overtime_minutes(punch_out, date_str, emp)
        out["half_day"] = hours < HALF_DAY_BELOW  # R5
    return out


def weekend_dates(first: str, last: str) -> list[str]:
    """Saturday/Sunday dates between two ISO dates (R7)."""
    day, end, found = parse_date(first), parse_date(last), []
    while day <= end:
        if day.weekday() >= 5:
            found.append(day.isoformat())
        day += timedelta(days=1)
    return found


def count_working_days(start: date_type, end: date_type) -> int:
    count, day = 0, start
    while day <= end:
        if day.weekday() < 5:
            count += 1
        day += timedelta(days=1)
    return count


# --------------------------------------------------------------------------
# 3. Models and serializers
# --------------------------------------------------------------------------

EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
SHIFT_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=SHIFT_PATTERN)
    shift_end: str = Field(default="18:30", pattern=SHIFT_PATTERN)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _joined_on_is_real_date(cls, value: str) -> str:
        parse_date(value)
        return value

    @model_validator(mode="after")
    def _shift_not_empty(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1, max_length=50)
    punched_at: Optional[EpochMs] = None
    status: PunchStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1, max_length=50)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _history_value(key: str, value: Any) -> Any:
    return dt_to_ms(value) if key in ("punch_in", "punch_out") else value


def record_out(doc: dict) -> dict:
    history = []
    for entry in doc.get("history") or []:
        history.append(
            {
                "at": dt_to_ms(entry.get("at")),
                "by": entry.get("by"),
                "reason": entry.get("reason"),
                "changes": {
                    key: {"from": _history_value(key, ch.get("from")), "to": _history_value(key, ch.get("to"))}
                    for key, ch in (entry.get("changes") or {}).items()
                },
            }
        )
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": history,
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    return emp


# --------------------------------------------------------------------------
# 4a. Health and employees
# --------------------------------------------------------------------------
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        employees.insert_one(doc) 
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    query: dict = {}
    if department is not None:
        query["department"] = department
    total = employees.count_documents(query)
    cursor = employees.find(query).sort("emp_code", 1).skip((page - 1) * page_size).limit(page_size)
    return {
        "items": [employee_out(d) for d in cursor],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# --------------------------------------------------------------------------
# 4b. Attendance
# --------------------------------------------------------------------------
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = trunc_seconds(ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc())
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    try:
        logs.insert_one(doc)  
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = trunc_seconds(ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc())

  
    rec = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", -1)]
    )
    if rec is None:
        raise HTTPException(status_code=404, detail="no punch-in found to close")
    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="already punched out")

    pin = aware(rec["punch_in"])
    if ts <= pin or ts - pin > MAX_SHIFT:
        unprocessable(["body", "punched_at"], "must be after punch_in and within 24 hours of it")

    derived = derive_fields(rec["status"], pin, ts, rec["date"], emp)
    # Atomic compare-and-set: only succeeds while the record is still open.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"]},
        {
            "$set": {
                "punch_out": ts,
                "work_hours": derived["work_hours"],
                "overtime_minutes": derived["overtime_minutes"],
                "half_day": derived["half_day"],
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="already punched out")
    return record_out(updated)


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    day = date_or_422(date, ["path", "date"]).isoformat()
    emp = get_employee_or_404(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": day})
    if rec is None:
        raise HTTPException(status_code=404, detail="attendance record not found")

    cur_status = rec["status"]
    cur_in, cur_out = aware(rec.get("punch_in")), aware(rec.get("punch_out"))
    new_status = body.status or cur_status
    sup_in = trunc_seconds(ms_to_dt(body.punch_in)) if body.punch_in is not None else None
    sup_out = trunc_seconds(ms_to_dt(body.punch_out)) if body.punch_out is not None else None

    if new_status in NON_PRESENCE:
        if sup_in is not None or sup_out is not None:
            unprocessable(["body"], f"punch times cannot be set while status is {new_status}")
        new_in = new_out = None  # ABSENT / LEAVE clear both punch times
    else:
        new_in = sup_in if sup_in is not None else cur_in
        new_out = sup_out if sup_out is not None else cur_out
        if new_in is None:
            unprocessable(["body", "punch_in"], f"status {new_status} needs a punch_in")
        if sup_in is not None and attendance_date(new_in, emp) != day:
            unprocessable(["body", "punch_in"], "punch_in must stay on this record's date")
        if new_out is not None and (sup_in is not None or sup_out is not None):
            if new_out <= new_in or new_out - new_in > MAX_SHIFT:
                unprocessable(["body", "punch_out"], "must be after punch_in and within 24 hours")

    new_vals = {
        "status": new_status,
        "punch_in": new_in,
        "punch_out": new_out,
        **derive_fields(new_status, new_in, new_out, day, emp),
    }
    cur_vals = {
        "status": cur_status,
        "punch_in": cur_in,
        "punch_out": cur_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }
    changes = {
        key: {"from": cur_vals[key], "to": new_vals[key]}
        for key in new_vals
        if cur_vals[key] != new_vals[key]
    }
    if not changes:
        unprocessable(["body"], "request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    history_len = len(rec.get("history") or [])
   
    updated = logs.find_one_and_update(
        {
            "emp_code": emp_code,
            "date": day,
            "status": cur_status,
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, history_len]},
        },
        {"$set": new_vals, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="record was modified concurrently, retry")
    return record_out(updated)


def plan_attendance_list(
    emp_code: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    status: Optional[str],
) -> dict:
    if date_from is not None:
        date_or_422(date_from, ["query", "date_from"])
    if date_to is not None:
        date_or_422(date_to, ["query", "date_to"])
    if date_from and date_to and date_from > date_to:
        unprocessable(["query", "date_from"], "date_from must not be after date_to")
    query: dict = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    if date_from or date_to:
        query["date"] = {}
        if date_from:
            query["date"]["$gte"] = date_from
        if date_to:
            query["date"]["$lte"] = date_to
    if status is not None:
        query["status"] = status
    return query


ATTENDANCE_SORT = [("date", -1), ("emp_code", 1)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    query = plan_attendance_list(emp_code, date_from, date_to, status)
    total = logs.count_documents(query)
    cursor = logs.find(query).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    return {
        "items": [record_out(d) for d in cursor],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# --------------------------------------------------------------------------
# 4c. Analytics: every endpoint is one aggregation pipeline
# --------------------------------------------------------------------------
MonthQ = Annotated[str, Query(pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")]
DateQ = Annotated[str, Query(pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")]

IS_PRESENT = {"$in": ["$status", list(PRESENCE)]}
IS_HALF_DAY = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
HAS_HOURS = {"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}
IS_LATE = {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}


def present_x2(weekend: list[str]) -> dict:
    """Present-day value in half-days: 2 = full day, 1 = half day, 0 = not counted.

    Counting doubled integers keeps every sum exact (no 0.1 + 0.2 float drift).
    """
    return {
        "$cond": [
            {"$and": [IS_PRESENT, {"$eq": [{"$in": ["$date", weekend]}, False]}]},
            {"$cond": [IS_HALF_DAY, 1, 2]},
            0,
        ]
    }


def plan_employee_monthly(emp_code: str, month: str) -> dict:
    first, last = month_bounds(month, ["query", "month"])
    emp = get_employee_or_404(emp_code)
    start = max(parse_date(first), parse_date(emp["joined_on"]))
    working_days = count_working_days(start, parse_date(last))
    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lte": last}}},
        {
            "$group": {
                "_id": None,
                "present_x2": {"$sum": present_x2(weekend_dates(first, last))},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": IS_LATE},
                "total_late_minutes": {"$sum": "$late_minutes"},
                "total_overtime_minutes": {"$sum": "$overtime_minutes"},
            }
        },
    ]
    return {"collection": "attendance_logs", "pipeline": pipeline, "working_days": working_days}


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthQ):
    plan = plan_employee_monthly(emp_code, month)
    row = next(iter(logs.aggregate(plan["pipeline"])), None) or {}
    x2 = int(row.get("present_x2") or 0)
    working_days = plan["working_days"]
    pct = half_up_div(x2 * 10000, 2 * working_days) / 100 if working_days > 0 else None
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": x2 / 2,
        "attendance_pct": pct,
        "leave_days": int(row.get("leave_days") or 0),
        "late_count": int(row.get("late_count") or 0),
        "total_late_minutes": int(row.get("total_late_minutes") or 0),
        "total_overtime_minutes": int(row.get("total_overtime_minutes") or 0),
    }


def plan_department_summary(month: str, department: Optional[str]) -> dict:
    first, last = month_bounds(month, ["query", "month"])
    weekend = weekend_dates(first, last)
    match: dict = {"joined_on": {"$lte": last}}  
    if department is not None:
        match["department"] = department

    
    per_employee = [
        {"$match": {"date": {"$gte": first, "$lte": last}}},
        {
            "$group": {
                "_id": None,
                "present_x2": {"$sum": present_x2(weekend)},
                "late_count": {"$sum": IS_LATE},
                "total_late_minutes": {"$sum": "$late_minutes"},
                "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
                
                "wh_cents": {
                    "$sum": {
                        "$cond": [HAS_HOURS, {"$toLong": {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}}, 0]
                    }
                },
                "wh_count": {"$sum": {"$cond": [HAS_HOURS, 1, 0]}},
            }
        },
    ]
    avg_hours = {
        "$cond": [
            {"$gt": ["$wh_count", 0]},
            {
                "$divide": [
                    {
                        "$floor": {
                            "$divide": [
                                {"$add": [{"$multiply": [2, "$wh_cents"]}, "$wh_count"]},
                                {"$multiply": [2, "$wh_count"]},
                            ]
                        }
                    },
                    100,
                ]
            },
            None,
        ]
    }
    pipeline = [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": per_employee,
                "as": "s",
            }
        },
       
        {"$unwind": {"path": "$s", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_x2": {"$sum": "$s.present_x2"},
                "late_count": {"$sum": "$s.late_count"},
                "total_late_minutes": {"$sum": "$s.total_late_minutes"},
                "leave_count": {"$sum": "$s.leave_count"},
                "on_duty_count": {"$sum": "$s.on_duty_count"},
                "wh_cents": {"$sum": "$s.wh_cents"},
                "wh_count": {"$sum": "$s.wh_count"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": {"$divide": ["$present_x2", 2]},
                "avg_work_hours": avg_hours,
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]
    return {"collection": "employees", "pipeline": pipeline}


@app.get("/analytics/departments/summary")
def department_summary(month: MonthQ, department: Optional[str] = None):
    plan = plan_department_summary(month, department)
    items = []
    for row in employees.aggregate(plan["pipeline"]):
        items.append(
            {
                "department": row["department"],
                "headcount": int(row["headcount"]),
                "present_days": float(row["present_days"]),
                "avg_work_hours": row.get("avg_work_hours"),
                "late_count": int(row.get("late_count") or 0),
                "total_late_minutes": int(row.get("total_late_minutes") or 0),
                "leave_count": int(row.get("leave_count") or 0),
                "on_duty_count": int(row.get("on_duty_count") or 0),
            }
        )
    return {"month": month, "items": items}


def plan_late_leaderboard(month: str, limit: int, department: Optional[str]) -> dict:
    first, last = month_bounds(month, ["query", "month"])
    pipeline: list[dict] = [
        {"$match": {"date": {"$gte": first, "$lte": last}, "late_minutes": {"$gt": 0}}},
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})
    pipeline += [
        
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # cut AFTER ranking so ties are never split
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return {"collection": "attendance_logs", "pipeline": pipeline}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: MonthQ,
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    plan = plan_late_leaderboard(month, limit, department)
    items = [
        {
            "rank": int(r["rank"]),
            "emp_code": r["emp_code"],
            "name": r["name"],
            "department": r["department"],
            "total_late_minutes": int(r["total_late_minutes"]),
            "late_count": int(r["late_count"]),
        }
        for r in logs.aggregate(plan["pipeline"])
    ]
    return {"month": month, "items": items}


def plan_department_trend(department: str, from_: str, to: str) -> dict:
    d_from = date_or_422(from_, ["query", "from"])
    d_to = date_or_422(to, ["query", "to"])
    if d_to < d_from:
        unprocessable(["query", "to"], "'to' must not be before 'from'")
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        unprocessable(["query", "to"], f"range must not exceed {MAX_TREND_DAYS} days")

   
    groups = list(
        employees.aggregate(
            [
                {"$match": {"department": department}},
                {"$group": {"_id": "$joined_on", "codes": {"$push": "$emp_code"}}},
            ]
        )
    )
    if not groups:
        raise HTTPException(status_code=404, detail="department not found")
    codes = [code for g in groups for code in g["codes"]]
    joins = [{"d": g["_id"], "n": len(g["codes"])} for g in groups]

    lower = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    upper = datetime(d_to.year, d_to.month, d_to.day, tzinfo=UTC) + timedelta(days=1)  # exclusive

    pipeline = [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": from_, "$lte": to}}},
        {
            "$group": {
                "_id": "$date",
                "present_x2": {"$sum": {"$cond": [IS_PRESENT, {"$cond": [IS_HALF_DAY, 1, 2]}, 0]}},
                "late_count": {"$sum": IS_LATE},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date_str": "$_id",
                "d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}},
                "present_x2": 1,
                "late_count": 1,
            }
        },
       
        {
            "$unionWith": {
                "coll": "employees",
                "pipeline": [
                    {"$match": {"department": department}},
                    {"$limit": 1},
                    {"$replaceWith": {"date_str": from_, "d": lower, "present_x2": 0, "late_count": 0}},
                ],
            }
        },
        {
            "$group": {
                "_id": "$date_str",
                "d": {"$first": "$d"},
                "present_x2": {"$sum": "$present_x2"},
                "late_count": {"$sum": "$late_count"},
            }
        },
        
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lower, upper]}}},
        {
            "$addFields": {
                "date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}},
                "dow": {"$dayOfWeek": "$d"},  # 1 = Sunday ... 7 = Saturday
                "present_x2": {"$ifNull": ["$present_x2", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
            }
        },
        {
            "$addFields": {
                "is_working_day": {"$in": ["$dow", [2, 3, 4, 5, 6]]},
                "headcount": {
                    "$sum": {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": {"$literal": joins},
                                    "as": "j",
                                    "cond": {"$lte": ["$$j.d", "$date"]},
                                }
                            },
                            "as": "j",
                            "in": "$$j.n",
                        }
                    }
                },
            }
        },
        {
            
            "$addFields": {
                "rate_int": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": ["$present_x2", 10000]}, "$headcount"]},
                                    {"$multiply": [2, "$headcount"]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {
            
            "$setWindowFields": {
                "sortBy": {"d": 1},
                "output": {
                    "w_sum": {"$sum": "$rate_int", "window": {"documents": [-6, 0]}},
                    "w_cnt": {
                        "$sum": {"$cond": [{"$isNumber": "$rate_int"}, 1, 0]},
                        "window": {"documents": [-6, 0]},
                    },
                },
            }
        },
        {
            "$addFields": {
                "ma_int": {
                    "$cond": [
                        {"$gt": ["$w_cnt", 0]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": [2, "$w_sum"]}, "$w_cnt"]},
                                    {"$multiply": [2, "$w_cnt"]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": {"$divide": ["$present_x2", 2]},
                "late_count": 1,
                "attendance_rate": {
                    "$cond": [{"$isNumber": "$rate_int"}, {"$divide": ["$rate_int", 10000]}, None]
                },
                "moving_avg_7d": {
                    "$cond": [{"$isNumber": "$ma_int"}, {"$divide": ["$ma_int", 10000]}, None]
                },
            }
        },
        {"$sort": {"date": 1}},
    ]
    return {"collection": "attendance_logs", "pipeline": pipeline}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[str, Query(alias="from", pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")],
    to: DateQ,
):
    plan = plan_department_trend(department, from_, to)
    items = [
        {
            "date": r["date"],
            "is_working_day": bool(r["is_working_day"]),
            "headcount": int(r["headcount"]),
            "present_count": float(r["present_count"]),
            "late_count": int(r["late_count"]),
            "attendance_rate": r.get("attendance_rate"),
            "moving_avg_7d": r.get("moving_avg_7d"),
        }
        for r in logs.aggregate(plan["pipeline"])
    ]
    return {"department": department, "from": from_, "to": to, "items": items}


# --------------------------------------------------------------------------
# 4d. Admin: explain
# --------------------------------------------------------------------------
ExplainTarget = Literal[
    "attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"
]


def _need(value: Any, name: str):
    if value is None:
        unprocessable(["query", name], f"{name} is required for this endpoint")
    return value


def _jsonable(doc: Any) -> Any:
    return json.loads(json_util.dumps(doc))


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Optional[MonthQ] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from", pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")] = None,
    to: Optional[DateQ] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    """executionStats explain for the exact query each endpoint runs."""
    if endpoint == "attendance_list":
        query = plan_attendance_list(emp_code, date_from, date_to, status)
        collection = "attendance_logs"
        command = {
            "find": collection,
            "filter": query,
            "sort": dict(ATTENDANCE_SORT),
            "skip": (page - 1) * page_size,
            "limit": page_size,
        }
    else:
        if endpoint == "employee_monthly":
            plan = plan_employee_monthly(_need(emp_code, "emp_code"), _need(month, "month"))
        elif endpoint == "department_summary":
            plan = plan_department_summary(_need(month, "month"), department)
        elif endpoint == "late_leaderboard":
            plan = plan_late_leaderboard(_need(month, "month"), limit, department)
        else:
            plan = plan_department_trend(_need(department, "department"), _need(from_, "from"), _need(to, "to"))
        collection = plan["collection"]
        command = {"aggregate": collection, "pipeline": plan["pipeline"], "cursor": {}}

    result = db.command({"explain": command, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": collection, "explain": _jsonable(result)}
