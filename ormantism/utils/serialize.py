from enum import Enum
from datetime import datetime
from pydantic import BaseModel


def serialize(data: any) -> dict | list | int | float | str | bool | None:
    """
    Convert any list or dict of scalars or pydantic.BaseModel instances (even nested)
    to a JSON serializable format using only dict, list, int, float, str, and bool.

    Args:
        data: The data to serialize.

    Returns:
        A JSON serializable version of the data.
    """
    if isinstance(data, BaseModel):
        # Use Pydantic's model_dump method to serialize the model
        return serialize(data.model_dump(mode="json"))
    if isinstance(data, dict):
        # Recursively serialize each value in the dictionary
        return {key: serialize(value) for key, value in data.items()}
    if isinstance(data, (list, tuple, set)):
        # Recursively serialize each item in the list
        return [serialize(item) for item in data]
    if isinstance(data, Enum):
        return data.name
    if isinstance(data, (int, float, str, bool)) or data is None:
        # Return the data as-is if it's already serializable
        return data
    if isinstance(data, datetime):
        return str(data)
    # Unkown other types
    raise ValueError(data)
