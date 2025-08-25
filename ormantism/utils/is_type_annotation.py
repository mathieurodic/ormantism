from typing import get_origin, get_args, Union

def is_type_annotation(annotation) -> bool:
    origin = get_origin(annotation)
    args = get_args(annotation)

    # Bare types like `str`, `int`, `MyClass`
    if isinstance(annotation, type):
        return True

    # Generic types like list[int], dict[str, int], MyClass[...]
    if origin is not None and isinstance(origin, type):
        return True

    # Union types like Union[int, str, None]
    if origin is Union:
        return all(is_type_annotation(arg) for arg in args)

    return False


if __name__ == "__main__":
    from typing import Optional, Union, List, Dict

    assert is_type_annotation(int)                     # ✅
    assert is_type_annotation(str)                     # ✅
    assert is_type_annotation(Optional[int])           # ✅
    assert is_type_annotation(Union[int, str])         # ✅
    assert is_type_annotation(Union[int, None])        # ✅
    assert is_type_annotation(list[int])               # ✅
    assert is_type_annotation(Optional[list[str]])     # ✅
    assert is_type_annotation(Dict[str, list[int]])    # ✅
    assert is_type_annotation(Union[list[int], None])  # ✅

    # Invalid/ambiguous edge case
    assert not is_type_annotation(Union[int, "not_a_type"])  # ❌ (if "not_a_type" is a string)
