Please update (or generate) `REFERENCE.md` in the project's `ormantism` directory, with the following structure:

```markdown
# Reference

## Code

### Methods

| method path | defined at | used at |
|-------------|------------|---------|
```

If it exists, keep the rest of file (what is not under `### Methods` in the hierarchy described above) intact.

Only reference methods that are directly available under a module or class (available at module-level).

* `method path` is the path of the method, including module (and class, if applies)
* `defined at` is where the method is defined (FILENAME:LINE).
* `used at` is a newline-separated list (no bullet points) of places where the method is called (FILENAME:LINE)

To proceed, please browse Python scripts in `ormantism` directory, and update `ormantism/REFERENCE.md` incrementally after each script read.
