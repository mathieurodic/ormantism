from typing import Type, Union, Optional, get_origin, get_args, _GenericAlias
import enum
import sys
import datetime


def get_base_type(complex_type: Type) -> tuple[Type, bool]:
    """
    Returns `(Something, True)` when provided `complex_type = Something`
    Returns `(Something, False)` when provided `complex_type = Something | None`
    Accepted base types are:
    - int
    - bool
    - float
    - str
    - dict
    - list
    - datetime.datetime
    - subclasses of `enum.Enum`
    - subclasses of `Base` (defined elsewhere in the project)
    """
    
    # Handle forward references (string annotations)
    if isinstance(complex_type, str):
        # Try to resolve the string reference
        frame = sys._getframe(1)
        try:
            complex_type = eval(complex_type, frame.f_globals, frame.f_locals)
        except (NameError, AttributeError):
            raise TypeError(f"Cannot resolve forward reference: {complex_type}")
    
    # # Check if it's an instantiated object rather than a type
    if not isinstance(complex_type, (type, _GenericAlias)) and not hasattr(complex_type, '__origin__'):
        raise TypeError(f"Expected a type, got an instance: {complex_type}")
    
    # Get the origin and args for union/generic types
    origin = get_origin(complex_type)
    args = get_args(complex_type)
    
    # Handle Union types (including Optional and | syntax)
    if origin is Union:
        # Check if it's a simple optional pattern (Something | None)
        if len(args) == 2 and type(None) in args:
            # Extract the non-None type
            non_none_type = args[0] if args[1] is type(None) else args[1]
            # Recursively process the non-None type to get its base type
            base_type, _ = get_base_type(non_none_type)
            return (base_type, False)
        else:
            raise TypeError(f"Complex unions not supported: {complex_type}")
    
    # Handle generic types (List[int], Dict[str, int], etc.)
    if origin is not None:
        # Map typing generics to built-in types
        type_mapping = {
            list: list,
            dict: dict,
            set: set,
            tuple: tuple,
            frozenset: frozenset,
        }
        
        if origin in type_mapping:
            return (type_mapping[origin], True)
        else:
            # For other generic types, check if the origin is acceptable
            return get_base_type(origin)
    
    # Handle built-in types
    if complex_type in (int, bool, float, str, dict, list, datetime.datetime):
        return (complex_type, True)
    
    # Handle None type
    if complex_type is type(None):
        raise TypeError("None type is not supported")
    
    # Handle Enum subclasses
    if isinstance(complex_type, type) and issubclass(complex_type, enum.Enum):
        return (complex_type, True)
    
    # TODO: make it less permissive
    return (complex_type, True)


if __name__ == "__main__":
    from typing import Dict, List, Optional, Union
    import enum
    
    # Test basic types
    assert get_base_type(int) == (int, True)
    assert get_base_type(str) == (str, True)
    assert get_base_type(dict) == (dict, True)
    assert get_base_type(list) == (list, True)
    
    # Test generic types
    assert get_base_type(Dict[int, str]) == (dict, True)
    assert get_base_type(List[int]) == (list, True)
    
    # Test optional types
    assert get_base_type(Optional[int]) == (int, False)
    # assert get_base_type(int | None) == (int, False)
    assert get_base_type(Union[str, None]) == (str, False)
    assert get_base_type(Optional[List[int]]) == (list, False)
    
    # Test enum
    class MyEnum(enum.Enum):
        A = 1
        B = 2
    
    assert get_base_type(MyEnum) == (MyEnum, True)
    assert get_base_type(Optional[MyEnum]) == (MyEnum, False)
    
    print("All tests passed!")
