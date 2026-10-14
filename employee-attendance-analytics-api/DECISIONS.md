# DECISIONS.md

1. **Indexes.** Unique `(emp_code, date)` guarantees one record per day and serves lookups and the monthly report. `(date desc, emp_code, late_minutes)` matches the list sort and lets the leaderboard scan without reading documents. `(status, date desc, emp_code)` serves status filters. `(emp_code, punch_in desc)` finds the record to punch out. Employees: unique `emp_code`, plus department and join-date indexes. Rejected: a separate `late_minutes` index, since the covered compound already helps.

2. **Punch-in race.** Both requests skip any "exists?" check and just insert. The unique index lets exactly one insert succeed; the other gets `DuplicateKeyError`, which I turn into a 409.

3. **Ties.** The cutoff applies to the rank, not the row count. `$rank` gives tied people the same rank (1, 2, 2, 4), then I keep `rank <= limit`, so a tie is never split and may return more than `limit` rows.

4. **Headcount.** The summary starts from `employees`, and `$lookup` attaches logs. An employee with no logs still produces a row, so they are counted.

5. **Change for 100x data.** Store `department` on each log, so analytics never need a join, and pre-aggregate daily totals.
