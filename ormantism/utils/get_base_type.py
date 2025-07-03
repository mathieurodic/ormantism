from typing import Type, Union, get_origin, get_args, List, Dict, Any
from types import UnionType

def get_base_type(complex_type: Type) -> tuple[Type, bool]:
    """
    Extracts the base type from a given complex type and determines if it is optional.

    This function handles both simple types and union types (e.g., types that can be None).
    For container types like `list[str]` or `dict[str, list[int]]`, it extracts the base container type.
    It returns a tuple where the first element is the base type and the second element is a boolean
    indicating whether the type is optional (i.e., can be None).

    Args:
        complex_type (Type): The complex type to be analyzed. This can be a simple type, a container type,
                            or a union type with None.

    Returns:
        tuple[Type, bool]: A tuple containing the base type and a boolean indicating if the type is optional.
                          For example:
                          - For `Something`, it returns `(Something, True)`.
                          - For `Something | None`, it returns `(Something, False)`.
                          - For `list[str]`, it returns `(list, True)`.
                          - For `dict[str, list[int]] | None`, it returns `(dict, False)`.

    Raises:
        TypeError: If the complex type is a union that does not exclusively involve a single type and None.
    """
    origin = get_origin(complex_type)
    args = get_args(complex_type)

    # Check if it's a union type, including the new | syntax
    if origin is Union or origin is UnionType:
        if type(None) not in args:
            raise TypeError("Complex type must be a union with None only.")
        # Filter out None type
        base_type = next(arg for arg in args if arg is not type(None))
        return (get_container_base_type(base_type), False)
    else:
        return (get_container_base_type(complex_type), True)

def get_container_base_type(type_: Type) -> Type:
    """
    Extracts the base container type from a given type.

    This helper function is used to get the base type of container types. For example,
    it extracts `list` from `list[str]` and `dict` from `dict[str, list[int]]`.
    If the input type is not a container type, it returns the type as-is.

    Args:
        type_ (Type): The type to be analyzed. This can be a simple type or a container type.

    Returns:
        Type: The base container type if the input is a container type; otherwise, the input type itself.
             For example:
             - For `list[str]`, it returns `list`.
             - For `dict[str, list[int]]`, it returns `dict`.
             - For `int`, it returns `int`.
    """
    origin = get_origin(type_)
    if origin is not None:
        return origin
    return type_
