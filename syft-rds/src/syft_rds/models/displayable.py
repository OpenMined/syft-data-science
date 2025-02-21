from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import BaseModel


class PydanticFormatter(ABC):
    @abstractmethod
    def format(self, model: BaseModel) -> str: ...


class DefaultPydanticFormatter(PydanticFormatter):
    def format(self, model: BaseModel) -> str:
        # Find BaseModel to get default __repr__ implementation
        for cls in model.__class__.__mro__:
            if cls is BaseModel:
                return cls.__repr__.__get__(model, model.__class__)()
        return str(model.model_dump_json())


class ANSIPydanticFormatter(PydanticFormatter):
    def format_class_name(self, name: str) -> str:
        return f"\033[1;36m{name}\033[0m"

    def format_field(self, key: str, value: Any) -> str:
        value_str = str(value)
        formatted_key = f"\033[1m{key}\033[0m"

        if isinstance(value, (int, float)):
            formatted_value = f"\033[36m{value_str}\033[0m"
        elif isinstance(value, str):
            formatted_value = f"\033[32m{value_str}\033[0m"
        elif isinstance(value, (list, dict)):
            formatted_value = f"\033[33m{value_str}\033[0m"
        else:
            formatted_value = f"\033[34m{value_str}\033[0m"

        return f"  {formatted_key}: {formatted_value}"

    def format(self, model: BaseModel) -> str:
        header = self.format_class_name(model.__class__.__name__) + "\n"

        fields = model.model_dump(mode="json")
        items = [self.format_field(k, v) for k, v in fields.items()]

        return header + "\n".join(items)


class PydanticDisplayableMixin:
    """Mixin to override __str__ and __repr__ methods of Pydantic models.

    In the future, this mixin can provide formatting options for jupyter notebooks, rich, etc.

    Usage:
    ```
    # Mixin goes first to override BaseModel methods
    class MyModel(PydanticDisplayableMixin, BaseModel):
        name: str

    model = MyModel(...)
    # Print with default ANSI formatter
    print(model)

    # switch to pydantic default formatter
    from .displayable import DefaultPydanticFormatter
    model.set_display_formatter(DefaultPydanticFormatter())
    print(model)
    ```
    """

    __display_formatter__: ClassVar[PydanticFormatter] = ANSIPydanticFormatter()

    @classmethod
    def set_display_formatter(cls, formatter: PydanticFormatter) -> None:
        cls.__display_formatter__ = formatter

    @classmethod
    def get_display_formatter(cls) -> PydanticFormatter:
        return cls.__display_formatter__

    def __str__(self) -> str:
        return self.__display_formatter__.format(self)

    def __repr__(self) -> str:
        return self.__str__()
