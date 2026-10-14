# Faculty Systems Training

October 14, 2026

> **Prefect** is a Python-based pipeline management framework. It is based on the client+server model.

## Topics

1. **DAGs** (Directed Acyclic Graphs) are the canonical, code-derived format for defining workflows or pipelines. Think of a DAG as a recipe that brings together *ingredients* and *processes them* in a particular order, without ever getting cyclically stuck in any particular step.

2. **Prefect Flows**

    - **Tasks** are the units of work: ordinary Python functions marked with `@task`. Each task run is tracked on its own, with its own state (pending, running, completed, failed), logs, and timing. Tasks are also where you attach operational behavior that you would otherwise write by hand:
        - `retries=3, retry_delay_seconds=10` to survive flaky APIs or network blips
        - caching, so an expensive step is skipped if its inputs haven't changed
        - `log_prints=True`, so plain `print()` output shows up in the Prefect UI
    - **Flows** are the container: a function marked with `@flow` that calls tasks. The flow is the thing you run, schedule, and monitor. A flow can call other flows (subflows), which is how larger pipelines are broken into readable pieces. The flow also chooses a *task runner*, which determines whether tasks run one at a time or concurrently.
    - **Pythonic.** There is no separate DAG file or configuration language. The DAG is inferred from the code at runtime: if `task_two` takes the output of `task_one`, Prefect knows `task_two` depends on it. That means:
        - Normal `if`/`else`, loops, and `try`/`except` work as expected, and the graph can change from run to run based on the data.
        - Return values are passed between tasks directly as Python objects.
        - A flow is still a Python script. `python simple_flow.py` runs it locally, with no server required.
        - Parallelism is one change: `task_one.map(numbers)` submits one task run per item instead of looping serially.

    Q: Which of your current scripts already have an extract → transform → load shape, and what happens today when step two fails halfway through?

3. **Prefect vs. Airflow**

    Apache Airflow is the long-standing industry standard (originally from Airbnb, 2014). Prefect was built later, partly as a response to Airflow's friction points. Both define pipelines in Python and both have a scheduler and web UI. They differ in how much infrastructure and ceremony they require.

    | | **Airflow** | **Prefect** |
    |---|---|---|
    | Defining a pipeline | A DAG object declared up front; the scheduler parses DAG files on a loop | Decorated Python functions; the graph is discovered as the code runs |
    | Running locally | Needs a scheduler, metadata database, and API server running | `python my_flow.py` |
    | Passing data between steps | XCom (see below) | Plain function return values |
    | Dynamic / conditional logic | Possible (dynamic task mapping, branching operators) but more constrained | Native Python control flow |
    | Ecosystem | Very large; mature in enterprise and cloud-managed offerings (AWS MWAA, Google Cloud Composer) | Smaller but growing; Prefect Cloud or self-hosted server |

    - **XCom** ("cross-communication") is how Airflow tasks share data. Each task can run in a separate process or on a separate machine, so outputs are written to Airflow's metadata database and read back by downstream tasks. XComs are meant for *small* values such as a file path, row count, or ID, not datasets. The usual pattern is to write the data to S3 or a database and pass only its location through XCom. The newer TaskFlow API (`@task` in Airflow) hides much of this, but XCom is still underneath it. In Prefect, a task's return value is handed directly to the next task. Results can optionally be persisted to storage for caching or recovery, but you don't have to design around it.
    - **Connectors.** Both tools ship prebuilt integrations so you aren't writing raw API calls for common services.
        - Airflow calls these **providers** (`apache-airflow-providers-amazon`, `-postgres`, `-snowflake`, etc.). They supply *operators* (a pre-built task, e.g. "copy this S3 object") and *hooks* (a connection client). Credentials are stored as Airflow *Connections*. The provider catalog is very large, often the deciding factor for enterprise teams.
        - Prefect calls these **integrations** (`prefect-aws`, `prefect-gcp`, `prefect-dbt`, `prefect-snowflake`, etc.). Credentials and configuration are stored as **Blocks**, which are saved server-side and loaded in code (e.g. `AwsCredentials.load("my-creds")`). Because flows are plain Python, you can also just use any Python library directly (`requests`, `boto3`, `duckdb`), as the examples in this repo do.

    In practice: Airflow is a reasonable choice when an organization already runs it or needs a specific provider. For research and teaching, where the goal is to take an existing script and make it observable, retryable, and schedulable, Prefect involves far less setup.

    Q: Where does your data live between steps today (memory, local files, a database), and would that still work if each step ran on a different machine?

4. **SDS Prefect Server** - **https://sds-prefect.pods.uvarc.io/**
    - Username / password on request.
    - Point your local client at the server, then run any flow as usual. The run, its tasks, and its logs appear in the UI:

        ```bash
        prefect config set PREFECT_API_URL=https://sds-prefect.pods.uvarc.io/api
        prefect config set PREFECT_API_AUTH_STRING="username:password"
        python prefect/simple_flow.py
        ```

    - Or add those two ENV variables to your local `~/.prefect/profiles.toml` file.

## For Teaching / Reference

- [`prefect/simple_flow.py`](prefect/simple_flow.py) - two chained tasks; toggle between the serial loop and `.map()` to show concurrency (~60s vs. ~6s).
- [`prefect/github_commits.py`](prefect/github_commits.py) - a full extract → transform → load → plot pipeline against the GitHub API, with retries and DuckDB storage. Requires `GH_PAT`.
- [`prefect/github_activity.py`](prefect/github_activity.py) - extends the above with issues/PRs, a vitality plot, and per-repo fan-out that tolerates individual failures.
- Docs: [Prefect](https://docs.prefect.io/) · [Airflow](https://airflow.apache.org/docs/)
