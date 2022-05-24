# rundeck-history-cleanup

This Python script is a nuclear option for Rundeck databases that have grown too unwieldy to manage using the RD CLI tool. In my case, the execution table had over 180 million rows, and the execution history cleanup job was no longer functioning. 


## Caveats
- This will be just as inefficient as the RD CLI tool **unless** the `base_report.jc_exec_id` field is altered to `bigint(20)` type. The index does not work otherwise.
- You'll have to delete any execution logs on-disk or in S3 via some other method


## Setup
- Provide a `.env` file following the example.
- Start using `--dry-run` and deleting only a few executions at a time to double-check what will get modified. e.g.: `./cleanup.py --max 10 --months 1 --dry-run`
