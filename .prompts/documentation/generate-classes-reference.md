Please update (or generate) `REFERENCE.md` in the project's `ormantism` directory, with the following structure:

```markdown
# Reference

## Code

### Classes

| available as | inherits from | defined at | used at |
|--------------|---------------|------------|---------|
```

If class is not directly available under a module, do not mention it.
Of course, only consider classes defined under `ormantism` module.

* `available as` is the path of the class (`ormantism[.SUBMODULE].CLASSNAME`)
* `inherits from` is a newline-separated list of class paths (`MODULE.CLASSNAME`) from which the class inherits (leave empty if only inherits from `builtins.object`)
* `defined at` is where the class is defined (FILENAME:LINE).
* `used at` is a newline-separated list (no bullet points) of places where the class is either instanciated (call to constructor; FILENAME:LINE), or subclassed (definition of a class inheriting from it)

To proceed, please browse every Python script in `ormantism` directory, and update `ormantism/REFERENCE.md` incrementally after each script read.