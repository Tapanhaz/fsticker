
# Setting up PostgreSQL/TimescaleDB for fsticker's candle sink

This is a one-time setup on whatever machine will run your database —
it has nothing to do with installing `fsticker` itself (`pip install
fsticker` never needs Postgres present). Do this only if you want
completed candles persisted via `timescale=fsticker.TimescaleConfig(...)`.

## Option A: Docker (recommended, fastest)

TimescaleDB publishes an official image with the extension preinstalled,
so there's no separate "install the extension" step.

```bash
docker run -d --name fsticker-timescale \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=changeme \
  -e POSTGRES_DB=fsticker_db \
  -v fsticker_timescale_data:/var/lib/postgresql/data \
  timescale/timescaledb:latest-pg17
```

Or with `docker-compose.yml`:

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg17
    restart: unless-stopped
    environment:
      POSTGRES_PASSWORD: changeme
      POSTGRES_DB: fsticker_db
    ports:
      - "5432:5432"
    volumes:
      - fsticker_timescale_data:/var/lib/postgresql/data

volumes:
  fsticker_timescale_data:
```

```bash
docker compose up -d
```

That's it — no manual `CREATE EXTENSION`, no manual database creation
beyond what `POSTGRES_DB` already did. Point `fsticker` at it:

```python
timescale=fsticker.TimescaleConfig(
    host="localhost", port=5432, dbname="fsticker_db",
    user="postgres", password="changeme",
)
```

`fsticker` creates its own tables and hypertables automatically on
first use (see "Schema" in the main README) — nothing further to run
by hand.

## Option B: A dedicated, non-superuser role (recommended for anything
beyond local experimentation)

Running as the `postgres` superuser works but is not something you
want in a real deployment. From inside the container (or any `psql`
session with superuser rights):

```bash
docker exec -it fsticker-timescale psql -U postgres -d fsticker_db
```

```sql
CREATE ROLE fsticker WITH LOGIN PASSWORD 'changeme';
GRANT ALL PRIVILEGES ON DATABASE fsticker_db TO fsticker;
GRANT ALL ON SCHEMA public TO fsticker;
```

`fsticker`'s `auto_bootstrap_schema=True` (the default) needs
privileges to `CREATE TABLE` and `CREATE EXTENSION IF NOT EXISTS
timescaledb`. If your role can't `CREATE EXTENSION` (common on managed
/ shared hosting where extensions are pre-provisioned by an admin),
that step is skipped automatically with a one-line log message — your
candles still persist, just as plain Postgres tables rather than
hypertables. Ask whoever administers the server to run
`CREATE EXTENSION IF NOT EXISTS timescaledb;` once, as a superuser, if
you want hypertable partitioning and don't have that privilege
yourself.

## Option C: Native install (Ubuntu/Debian, no Docker)

```bash
# Add the TimescaleDB apt repo (see https://docs.timescale.com/self-hosted/latest/install/installation-linux/
# for other distros / current instructions -- this changes over time,
# so that page is the source of truth, not this snippet).
sudo apt install -y postgresql-17 timescaledb-2-postgresql-17
sudo timescaledb-tune --quiet --yes
sudo systemctl restart postgresql

sudo -u postgres psql
```
```sql
CREATE DATABASE fsticker_db;
\c fsticker_db
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE ROLE fsticker WITH LOGIN PASSWORD 'changeme';
GRANT ALL PRIVILEGES ON DATABASE fsticker_db TO fsticker;
GRANT ALL ON SCHEMA public TO fsticker;
```

## Verifying it's working

Start your feed with `timescale=` configured and `candle_timeframes`
including at least one `auto_finalize=True` or naturally-closing
timeframe, let it run a few minutes, then:

```bash
PGPASSWORD=changeme psql -h localhost -U fsticker -d fsticker_db \
  -c '\dt'
```

You should see `candles_<period>` (or whatever `table=` you configured)
tables appear on their own — no migration step, no manual DDL, as long
as `auto_bootstrap_schema=True` (the default). To confirm a table
became a real hypertable (vs. a plain fallback table):

```sql
SELECT * FROM timescaledb_information.hypertables;
```

## Troubleshooting

- **Nothing appears, no error printed** — check you actually passed
  `timescale=` to `MergedFeed`/`AsyncMergedFeed`, and that
  `candle_timeframes` includes at least one entry that will actually
  close a candle (a live-only timeframe with no `auto_finalize` won't
  ever emit a `Complete` candle if the market never completes a
  natural period boundary tick, though in normal trading this isn't
  an issue).
- **One warning printed to stderr, then silence** — connection/auth
  failed on the very first attempt. This is treated as a
  configuration problem, not a transient outage: the sink goes
  permanently inert for the life of that feed rather than retrying
  forever (everything else — ticks, candles, your own callbacks —
  keeps working). Fix the host/port/dbname/user/password and restart
  the feed.
- **Repeated "insert ... failed ... dropping this candle" warnings,
  connection otherwise fine** — a schema mismatch: either
  `auto_bootstrap_schema=False` and you haven't created the table
  yourself. Compare against the schema shown in the main README, 
  or just drop and let `auto_bootstrap_schema=True` recreate it.