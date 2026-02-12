"""Shared test helpers."""

from ormantism.table import Table


def assert_table_instance(inst, expected: dict, exclude: set = None):
    """Assert every field on a Table instance matches expected. Raises on mismatch or missing assertion.

    For reference fields, pass the expected Table instance(s); comparison uses .id.
    Use exclude to skip fields (e.g. timestamps when not relevant).
    """
    exclude = exclude or set()
    for name, col in inst.__class__._get_columns().items():
        if name in exclude:
            continue
        assert name in expected, f"Test must assert {name}"
        actual = getattr(inst, name)
        exp = expected[name]
        if exp is None:
            assert actual is None, f"{name}: expected None, got {actual}"
        elif isinstance(exp, Table) and isinstance(actual, Table):
            assert actual.id == exp.id, f"{name}: expected id {exp.id}, got {actual.id}"
        elif isinstance(exp, list) and exp and isinstance(exp[0], Table):
            assert actual is not None, f"{name}: expected list, got None"
            assert len(actual) == len(exp), f"{name}: expected {len(exp)} items, got {len(actual)}"
            for i, (a, e) in enumerate(zip(actual, exp)):
                assert isinstance(a, Table), f"{name}[{i}]: expected Table, got {type(a)}"
                assert a.id == e.id, f"{name}[{i}]: expected id {e.id}, got {a.id}"
        else:
            assert actual == exp, f"{name}: got {actual!r}, expected {exp!r}"
