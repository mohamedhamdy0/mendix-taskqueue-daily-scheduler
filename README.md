# Mendix Task Queue Daily Scheduler

Run a heavy microflow (for example a long calculation) **once a day at a fixed time, in the
background on a Task Queue, exactly once across all app instances**, and save its results in small
batches instead of one huge transaction.

Useful when a daily job takes a long time, holds one long database transaction, or could run
more than once when the app has several instances.

## What is inside

| File | Type | What it does |
|---|---|---|
| `JA_ScheduleDailyMicroflow` | Java action | Puts **one** execution of a microflow on a Task Queue at the next `Hour:Minute` in a time zone. If a future execution is already waiting it adds nothing, so it is safe to call on every instance at every startup. Returns the planned moment. |
| `JA_ClaimDailyRun` | Java action | Returns `true` for exactly **one** caller per job per calendar day across all instances; everyone else gets `false` and skips the work. |
| `JA_CommitInBatches` | Java action | Commits a list (with events) and ends the database transaction every `BatchSize` objects, so locks stay short and finished work is kept. A failing object is logged and skipped; returns the number of failures. |
| `impl/DailyJobSupport` | Helper class | Atomic database "claims", lookup of waiting `System.QueuedTask` rows, and enqueueing in its own committed transaction. |

All logging goes to the log node `TaskQueueScheduler`.

## How it works

- **Scheduling.** The Mendix Task Queue has no "every day at HH:MM". The scheduler books a
  one-off task for the next occurrence, and the job books the next day when it runs, so the
  chain continues.
- **No duplicate bookings.** Before booking, it looks for a waiting future task of the same
  microflow in `System.QueuedTask`. Only one instance can book a given slot: it inserts a row into
  the claim table, and the primary key makes that atomic.
- **Exactly one run per day.** `JA_ClaimDailyRun` uses the same claim table with a per-day key.
  This is the real guarantee: even if two tasks were ever queued, only one does the work.
- **Short transactions.** `JA_CommitInBatches` ends the transaction every N objects instead of
  committing thousands of rows at the end.

The claim table `mx_daily_job_claim` (`job_key`, `slot`, `claimed_at`) is created automatically
on first use. It is not part of the domain model (Mendix ignores tables it does not own). Claims
older than 120 days are cleaned up automatically.

## Setup in Studio Pro

1. Create a module named **`TaskQueueScheduler`** with a folder `JavaActions`.
2. Create the three Java actions with **exactly** these names and parameters:

   | Java action | Parameters | Return type |
   |---|---|---|
   | `JA_ScheduleDailyMicroflow` | `MicroflowName` (String), `QueueName` (String), `Hour` (Integer/Long), `Minute` (Integer/Long), `TimeZoneId` (String) | Date and time |
   | `JA_ClaimDailyRun` | `JobKey` (String), `TimeZoneId` (String) | Boolean |
   | `JA_CommitInBatches` | type parameter `Entity`; `ObjectList` (List of `Entity`), `BatchSize` (Integer/Long) | Integer/Long |

3. Copy the `javasource/taskqueuescheduler` folder of this repo over the generated one in your app
   (Studio Pro keeps the code between `BEGIN USER CODE` and `END USER CODE`).
4. Create a **Task Queue**, for example `MyModule.MyTaskQueue`. Use **1 thread** (cluster-wide)
   if the job must never run in parallel.

## Microflow pattern

**After startup** (do not call the Java actions directly in the after-startup microflow, see the
notes below):

- `ASU_...` → call `SUB_DailyJob_Schedule` **in the task queue**.
- `SUB_DailyJob_Schedule` → `JA_ScheduleDailyMicroflow('MyModule.SE_DailyJob_Run',
  'MyModule.MyTaskQueue', 2, 0, 'UTC')` (hour 0-23, minute 0-59, any IANA time zone).

**The daily job** `SE_DailyJob_Run`:

1. `JA_ScheduleDailyMicroflow(...)` with the same arguments → books tomorrow first, so the chain
   continues even if today's work fails.
2. `JA_ClaimDailyRun('MyDailyJob', 'UTC')` → `false`: end (another instance already ran it).
3. `true`: run each step as its own sub-microflow with error handling (custom without rollback
   plus a log message), so one failing step does not stop the others.
4. For large result lists use `JA_CommitInBatches($List, 500)` instead of one big Commit.

## Notes and limits

- **Requirements:** a Mendix version whose Java API can execute a microflow in a task queue at a
  given start time. Test it on your own Mendix version and database first.
- The database user needs permission to `CREATE TABLE` once, for the claim table.
- Duplicate-key detection covers SQL state `23xxx` (PostgreSQL, HSQLDB and others), SQL Server
  (2627, 2601) and Oracle (1).
- **HSQLDB (local runs):** the one-time `CREATE TABLE` waits for open transactions, so start
  scheduling from a task-queue task, never directly inside a long transaction that has written data.
- **`JA_CommitInBatches` ends the transaction.** Earlier batches stay saved even if a later step
  fails, so don't use it where you rely on a rollback. Run it in a background or system context.
- Removing the module does not drop `mx_daily_job_claim`. Drop it by hand if you need to.

## License

MIT, see [LICENSE](LICENSE).

## Author

Mohamed Hamdy, [LinkedIn](https://www.linkedin.com/in/mohamed-hamdy-425a0211b/)
