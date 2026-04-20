# pipewatch

A lightweight CLI for monitoring and alerting on ETL pipeline health metrics.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Monitor a pipeline by pointing pipewatch at your metrics endpoint or log source:

```bash
pipewatch monitor --source postgres://user:pass@host/db --pipeline daily_etl
```

Set alert thresholds and get notified when metrics fall outside expected ranges:

```bash
pipewatch watch \
  --pipeline daily_etl \
  --metric row_count \
  --min 1000 \
  --alert-email ops@example.com
```

Check the status of all tracked pipelines at a glance:

```bash
pipewatch status
```

View details for a specific pipeline:

```bash
pipewatch status --pipeline daily_etl
```

View all available commands:

```bash
pipewatch --help
```

---

## Configuration

Pipewatch can be configured via a `pipewatch.yaml` file in your project root. See [docs/configuration.md](docs/configuration.md) for full reference.

---

## Contributing

Pull requests are welcome. Please open an issue first to discuss any major changes.

---

## License

This project is licensed under the [MIT License](LICENSE).
