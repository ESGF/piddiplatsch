import pytest
from pydantic import ValidationError

from piddiplatsch.core.handle_models import AliasHandleModel
from piddiplatsch.core.records import AliasHandleRecord

PID = "7c4a583c-0bfe-4517-98fa-325084b02684"
TARGET = "https://hdl.handle.net/21.TEST/1f062cde-b12d-335d-a30b-988188098842"


def test_alias_schema_requires_only_url_and_preserves_validated_pid():
    record = AliasHandleRecord(PID, TARGET)
    record.validate()

    assert isinstance(record.model, AliasHandleModel)
    assert record.model.get_pid() == PID
    assert record.as_record() == {"URL": TARGET}


@pytest.mark.parametrize(
    "values", [{}, {"URL": "relative/path"}, {"URL": "hdl:21.TEST/target"}]
)
def test_alias_schema_rejects_missing_or_invalid_redirect_url(values):
    with pytest.raises(ValidationError):
        AliasHandleModel(**values)


def test_alias_record_rejects_invalid_pid():
    with pytest.raises(ValueError, match="not a valid UUID"):
        AliasHandleRecord("invalid-pid", TARGET).validate()
