import pydantic
import typing

BackingFile = pydantic.constr(pattern=r"[a-zA-Z0-9-_]+")


class SetupCommand(pydantic.BaseModel):
    cmd: typing.Literal["setup"] = "setup"
    backing_file: BackingFile
    nqn: str


class Status(pydantic.BaseModel):
    pass


AnyCommand = pydantic.RootModel[
    typing.Annotated[
        typing.Union[SetupCommand],
        pydantic.Field(discriminator="cmd"),
    ]
]
