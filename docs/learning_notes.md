# NYC Taxi Analytics Warehouse — Tooling & Infra Notes

Running notes on tools/infrastructure concepts learned while building Project 1.
(Data engineering concepts like star schema, idempotency, etc. are tracked separately —
this file is just the "how do the tools work" reference.)

---

## 🐳 Docker / Docker Compose

**`docker-compose.yml` structure**
- `services` — each service = one container
- `image` — which Docker image to pull (e.g. `postgres:15`)
- `environment` — env vars passed into the container at startup
- `ports` — exposes a container's internal port to your host machine
- `volumes` — mounts folders/named volumes into the container (for persistence or sharing files)

**Host port vs container (internal) port**
`"host_port:container_port"` — e.g. `"5433:5432"`
- **Container port** — the port the program is actually listening on *inside* its isolated container world (never changes; Postgres always listens on 5432 internally)
- **Host port** — the port exposed on your Mac, so tools *outside* Docker (your terminal, a GUI client) can reach in
- Containers on the *same* Docker Compose network can talk to each other directly via service name + internal port — they don't need the host mapping at all, since they're not "outside," they're already on the same private network

**`depends_on`**
Only controls **startup order** (start container B before container A). It does **not** guarantee the dependency is actually *ready* to accept connections — a Postgres container can still be mid-boot even after "started."

**Named volumes**
```yaml
volumes:
  - postgres_warehouse_data:/var/lib/postgresql/data
```
Persists data outside the container's own lifecycle.
- `docker-compose down` → stops/removes containers, but **volumes persist** — your tables/data are still there when you `up` again
- `docker-compose down -v` → the `-v` flag **also deletes volumes** — this is the destructive version, only use if you actually want to wipe everything

**Why separate Postgres containers for warehouse vs Airflow metadata**
Airflow's internal bookkeeping (DAG run history, task state) is infrastructure — keeping it separate from your actual analytics data means you can wipe/reset Airflow's DB without ever risking your real warehouse tables. Same principle as not mixing an app DB with a logging DB.

**Core commands**
| Command | What it does |
|---|---|
| `docker ps` | List running containers |
| `docker logs <container>` | View a container's output/logs |
| `docker exec -it <container> <cmd>` | Run a command *inside* a running container (`-it` = interactive) |
| `docker cp <local-file> <container>:<path>` | Copy a file from your Mac into a running container |
| `docker stop <container>` | Halt a container (can restart later with `docker start`) |
| `docker rm <container>` | Delete the container itself (image stays; can recreate from compose file) |

---

## 🌬️ Airflow

- **`dags/` folder gets re-parsed every ~30 seconds** by the scheduler, even when nothing is running — so DAG files must stay lightweight (just imports + task wiring). Heavy logic (API calls, transformations) belongs in `src/`, only imported and called *inside* task functions so it runs only when the task actually executes.
- **`catchup=True`** — tells Airflow to automatically run one DAG execution for *every scheduled interval* between `start_date` and now that hasn't run yet (not just "some historical data" — specifically, one run per missed interval, in order).
- Airflow needs its **own metadata Postgres DB**, separate from any data warehouse it orchestrates.
- `standalone` mode — runs webserver + scheduler in one container/process, fine for local learning (production splits these into separate services).

---

## 🐘 PostgreSQL

- **Redshift is historically forked from Postgres** — shares SQL dialect and wire protocol (so `psycopg2` works against it), but internally rearchitected to be **columnar** for analytics. Vanilla Postgres is **row-oriented** (built for OLTP: fast single-record reads/writes), not columnar.
- **`SERIAL`** — Postgres shorthand for an auto-incrementing integer column; each new row gets the next number automatically. Commonly paired with `PRIMARY KEY`.
- **`psql` CLI flags**
  | Flag | Meaning |
  |---|---|
  | `-U` | username |
  | `-d` | database name |
  | `-f` | run SQL from a file |
  | `-c` | run a single command non-interactively, then exit |
- **psql meta-commands** (start with `\`, not SQL):
  | Command | What it does |
  |---|---|
  | `\dt` | list tables in current database |
  | `\d table_name` | describe a table's columns |
  | `\l` | list all databases |
  | `\q` | quit psql |

---

## ☁️ AWS (S3 + IAM)

- **IAM User** — a persistent identity (with its own long-lived credentials), typically for a person or an application needing programmatic access.
- **IAM Role** — a *temporary* identity that something (a person, an app, or an AWS service like EC2/Lambda) can *assume* — gets short-lived credentials that auto-expire. Preferred for services running inside AWS, since there's no long-lived secret to leak.
- **Policy** — the actual JSON document defining *what actions are allowed/denied on which resources*. Attached to either a User or a Role.
- **Principle of least privilege** — scope policies to the specific resource + actions needed (e.g., one S3 bucket, `GetObject`/`PutObject`/`ListBucket` only) instead of broad managed policies like `AmazonS3FullAccess`, which grants access to *every* bucket in the account.
- `boto3` **auto-detects credentials** from environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) — no need to manually pass them into the client.
- **Redshift Spectrum** — lets you run SQL directly against files sitting in S3 (via an "external table") without first loading them into Redshift — pay only for data scanned per query.

---

## 🐍 Python

- **`requests`** (HTTP calls) + **`boto3`** (AWS) — the standard libraries for this kind of pipeline work.
- **Streaming pattern** (avoids loading a whole multi-GB file into memory):
  ```python
  response = requests.get(url, stream=True)
  s3.upload_fileobj(response.raw, bucket, key)
  ```
  `stream=True` tells `requests` not to load the whole response at once; `response.raw` gives boto3 a file-like object it reads from incrementally.
- **`response.raise_for_status()`** — automatically raises an `HTTPError` on any 4xx/5xx response, instead of manually checking `if status_code == 200`.
- **`python-dotenv`** — loads a `.env` file's contents into `os.environ`:
  ```python
  from dotenv import load_dotenv
  load_dotenv(dotenv_path="docker/.env")  # path relative to wherever the script is run from
  ```
- **`if __name__ == "__main__":`** — guard that ensures code only runs when the file is executed directly (`python file.py`), not when it's imported as a module elsewhere (e.g., later by an Airflow DAG).
- **`botocore.exceptions.ClientError`** — the correct way to catch AWS API errors (e.g., a 404 from `head_object` when checking if an S3 key exists yet). Note: this is imported from `botocore.exceptions`, *not* accessed as `s3.exceptions.ClientError` off the client object.

---

## 🔧 Git / GitHub

- **Conventional commit prefixes**
  | Prefix | Meaning |
  |---|---|
  | `feat` | new feature/capability |
  | `fix` | bug fix |
  | `chore` | maintenance (config, dependencies, tooling — not a feature or fix) |
  | `docs` | documentation-only changes |
  | `refactor` | restructuring code, no behavior change |
  | `test` | adding/updating tests |
  | `style` | formatting-only changes (no logic change) |

  Used for fast `git log` scanning, automated changelogs, and semantic version bumps (`feat` → minor, `fix` → patch).

- **Never commit secrets** — even if deleted in a later commit, they remain retrievable in Git history unless you explicitly rewrite history (e.g., `git filter-repo`).
- **`.env` vs `.env.example`** — real secrets go in `.env` (gitignored, never committed); a template with placeholder values goes in `.env.example` (committed, documents what variables are needed).
- **`requirements.txt`** — generated via `pip freeze > requirements.txt`, captures every installed package + exact version so the environment can be recreated elsewhere.

---

*Last updated: Project 1, Sprint 2 (DE-6 in progress)*