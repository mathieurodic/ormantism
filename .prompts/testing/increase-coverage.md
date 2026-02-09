Hello, I would like to improve the test coverage for the `ormantism` module.

Please run...

```bash
source .venv/bin/activate
pytest --cov=ormantism --cov-report=term-missing
```

...and pick the script with the highest number in the `Miss` column. Suggest changes in `tests` to fill in the gaps for the chosen script.