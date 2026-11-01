# WebDash

A web-based analytics dashboard for exploring **HTTP access logs** and **authentication events** with **PySpark**, **Plotly**, and **Dash**. Investigate traffic patterns, endpoint performance, geographic distributions, login activity, and potential security threats in one interface.

> **Demo / analysis tool:** The included generator produces **synthetic data**, including simulated suspicious activity. Country attribution uses a **fake IP first-octet mapping**, not a real GeoIP service. Security indicators are heuristic signals, not definitive incident findings.

## Features

- **Traffic overview:** Request volume, status-code trends, user-agent categories, performance, and anomaly visualizations.
- **Content & performance:** Popular paths, response times, and traffic heatmaps.
- **Geography:** Country-level visualizations using the bundled simulated IP-to-country lookup.
- **Security & logins:** Authentication trends, failed-login analysis, targeted accounts, a combined threat leaderboard, possible account compromises, and impossible-travel indicators.
- **Data pipeline:** View parsing totals, rejected records, Spark details, source paths, and last load time.
- **Interactive controls:** Time-range and aggregation filters, user-agent filters, sensitivity settings, and manual reloads.
- **Log refresh:** Optional live polling picks up changes to local log files.
- **Synthetic log generator:** Create historical traffic and login data or continuously append demo events.

## Technology Stack

| Layer | Technologies |
| --- | --- |
| Backend / processing | Python, PySpark (local mode) |
| Dashboard | Dash, Plotly |
| Data handling | pandas, NumPy |
| Serving (optional) | Waitress, systemd |
| Sources | Local `access.log` and newline-delimited JSON `auth.log` |

**Requirements:** Python 3 with `venv` and **Java 17 or newer** for Spark 4. The provided setup script targets Debian 12/13 Linux; manual setup may also work on other platforms with compatible Python and Java versions.

## Project Structure

```text
webdash/
├── dashboard.py                 # Dash UI, PySpark parsing and analytics
├── generate_logs.py             # Synthetic historical/live log generator
├── common.py                    # Simulated country lookup tables
├── requirements.txt             # Python dependencies
├── setup.sh                     # Debian setup helper
├── traffic-dashboard.service    # Optional systemd service template
└── logs/                        # Generated/access and auth log files
    ├── access.log               # Created by generate_logs.py
    └── auth.log                 # Created by generate_logs.py
```

## Quick Start

### Option A — Debian setup script

From the project directory:

```bash
bash setup.sh
.venv/bin/python dashboard.py
```

The script installs missing Debian prerequisites when needed, creates `.venv`, installs dependencies, and generates sample logs if `logs/access.log` is absent.

### Option B — Manual setup

```bash
# From the directory containing dashboard.py
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create the expected log files
python generate_logs.py

# Start the dashboard
python dashboard.py
```

Open **http://localhost:8050** in your browser. The app listens on `0.0.0.0` by default, so a remote machine can use the server's reachable IP address and port 8050 if the network/firewall permits it.

### Windows notes

The Bash setup script and included systemd unit are Linux-oriented. For a Windows installation, use a supported Python environment, install a compatible Java 17+ runtime, activate the virtual environment with its Windows activation command, and run the Python commands above. Spark environment setup may require additional local configuration.

## Generate Demo Data

Generate seven days of historical synthetic data (default settings):

```bash
python generate_logs.py
```

Customize the amount of data:

```bash
python generate_logs.py --days 3 --requests 50000 --logins 5000 --seed 42
```

Continuously append synthetic events to existing logs:

```bash
python generate_logs.py --live --interval 2
```

Stop live generation with `Ctrl+C`. Use a **separate terminal** for the generator while the dashboard runs.

The generator writes `logs/access.log` and `logs/auth.log` by default. It can also write to another directory with `--out /path/to/logs`.

## Dashboard Sections

1. **Overview** — Traffic trends, anomalous patterns, latency, status codes, and user-agent types.
2. **Content & performance** — Request paths, response-time metrics, and a time-based activity heatmap.
3. **Geography** — Geographic chart and country distribution (simulated mapping only).
4. **Security & logins** — Login activity, method and failure analysis, threat scoring, possible compromises, and impossible-travel heuristics.
5. **Pipeline** — Spark configuration, loading statistics, parsed/rejected record counts, and data-source information.

The detection logic uses rule-based and statistical heuristics. For example, the possible-compromise view looks for multiple failed attempts followed by a success under specific same-user/same-IP conditions. Validate suspicious events with independent evidence before acting on them.

## Log Inputs

The dashboard expects **two files** in the selected log directory:

- `access.log` — one HTTP access record per line, in the application's expected combined-log variant with a trailing `rt=<seconds>` request-time field.
- `auth.log` — one JSON object per line, with fields such as `ts`, `user`, `ip`, `result`, `method`, and `reason`.

Example access record:

```text
103.10.20.30 - - [01/Nov/2026:12:00:00 +0000] "GET /products HTTP/1.1" 200 1234 "-" "Mozilla/5.0" rt=0.120
```

Example authentication event:

```json
{"ts":"2026-11-01T12:00:00Z","user":"demo","ip":"103.10.20.30","result":"success","method":"password","reason":null}
```

**Note:** Arbitrary Nginx/Apache logs may not match the expected access-log regex without adjustment. The dashboard reads from **local files**, not directly from HDFS or a remote log service.

## Configuration

The dashboard reads these environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `LOG_DIR` | `logs/` beside `dashboard.py` | Directory containing both input log files |
| `HOST` | `0.0.0.0` | Development server bind address |
| `PORT` | `8050` | Development server port |
| `SPARK_DRIVER_MEMORY` | `2g` | Spark driver memory |
| `MIN_RELOAD_SECS` | `10` | Minimum interval between file-change reloads |

Example:

```bash
LOG_DIR=/data/web-logs PORT=8050 python dashboard.py
```

## Optional Linux Service

`traffic-dashboard.service` is a **template** for running the dashboard with Waitress under systemd.

1. Edit `User`, `WorkingDirectory`, `LOG_DIR`, and `ExecStart` in the service file to match your actual deployment paths.
2. Install/copy the edited service into `/etc/systemd/system/`.
3. Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now traffic-dashboard.service
sudo systemctl status traffic-dashboard.service
```

The supplied service binds Waitress to `0.0.0.0:8050`. For internet-facing deployment, use appropriate network restrictions, access controls, and a reverse proxy with HTTPS; **the dashboard does not provide built-in user authentication**.

## Limitations & Security Notes

- **Simulated geography:** `common.py` maps country codes from the first IP octet. This is not accurate GeoIP or suitable evidence for real impossible-travel claims.
- **Heuristic detections:** Suspicious-path, scanner, brute-force, and compromise indicators may produce false positives or miss incidents.
- **Local Spark execution:** Spark runs with `local[*]`; this repository does not configure a distributed Spark cluster.
- **Sensitive inputs:** Real access and authentication logs may contain personal information, IP addresses, and internal paths. Restrict access and avoid committing real logs.
- **Resource needs:** Large generated datasets and Spark processing can require substantial memory and CPU. Reduce `--requests` and `--logins` for lightweight development.
- **Deployment:** Add authentication, secure log handling, monitoring, and production validation before exposing the dashboard beyond a trusted environment.

## Troubleshooting

**Java / Spark startup error:** Verify `java -version` reports Java 17+ and that `JAVA_HOME` points to the correct installation.

**Missing input file:** Run `python generate_logs.py` or set `LOG_DIR` to a directory containing **both** `access.log` and `auth.log`.

**No data / rejected rows:** Check input formats and timestamps against the examples above. The access-log parser expects an `rt=` field.

**Map not loading:** The code notes that the browser needs to reach Plotly's CDN for the geography map.

**Cannot access from another computer:** Verify the server's IP, port 8050, firewall rules, and bind address.

## Future Improvements

- Replace the fake GeoIP mapping with a real, maintained geolocation data source.
- Add automated tests and documented sample datasets.
- Support standard web-server log variants and configurable parsers.
- Introduce authenticated access and role-based controls.
- Add exportable reports, alert integrations, and hardened deployment configuration.

## License

No license file was present in the supplied project archive. Add a `LICENSE` file if you want to grant others permission to reuse or redistribute the code.
