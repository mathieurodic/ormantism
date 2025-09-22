from typing import Optional
from ormantism.table import Table, Field
from pydantic import Field as PydanticField


def test_from_pydantic_info():

    class Thing(Table):
        pass
    
    class Agent(Table):
        birthed_by: Optional["Agent"]
        name: str
        description: str | None
        thing: Thing
        system_input: str
        bot_name: str
        tools: list[str]
        max_iterations: int = 10
        temperature: float = 0.3
        with_input_improvement: bool = True
        conversation: list[str] = PydanticField(default_factory=list)

    fields = {
        name: Field.from_pydantic_info(Agent, name, info)
        for name, info in Agent.model_fields.items()
    }

    assert fields["birthed_by"] == Field(table=Agent,
                                         name="birthed_by",
                                         base_type=Agent,
                                         secondary_type=None,
                                         full_type=Optional[Agent],
                                         default=None,
                                         is_required=False,
                                         column_is_required=False,
                                         is_reference=True)
    assert fields["name"] == Field(table=Agent,
                                   name="name",
                                   base_type=str,
                                   secondary_type=None,
                                   full_type=str,
                                   default=None, 
                                   is_required=True,
                                   column_is_required=True,
                                   is_reference=False)
    assert fields["description"] == Field(table=Agent,
                                          name="description",
                                          base_type=str,
                                          secondary_type=None,
                                          full_type=Optional[str],
                                          default=None, 
                                          is_required=False,
                                          column_is_required=False,
                                          is_reference=False)
    assert fields["thing"] == Field(table=Agent,
                                    name="thing",
                                    base_type=Thing,
                                    secondary_type=None,
                                    full_type=Thing,
                                    default=None,
                                    is_required=True,
                                    column_is_required=True,
                                    is_reference=True)
    assert fields["with_input_improvement"] == Field(table=Agent,
                                                     name="with_input_improvement",
                                                     base_type=bool,
                                                     secondary_type=None,
                                                     full_type=bool,
                                                     default=True,
                                                     is_required=False,
                                                     column_is_required=True,
                                                     is_reference=False)
