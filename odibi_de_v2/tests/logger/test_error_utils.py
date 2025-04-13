
from logger.error_utils import format_error
from core import ErrorType


def test_format_error_output():
    result = format_error(
        module="INGESTION",
        component="ReaderFactory",
        method="get_reader",
        error_type=ErrorType.NOT_IMPLEMENTED,
        message="Reader for 'avro' not yet supported"
    )
    expected_start = "INGESTION.ReaderFactory.get_reader - NOT_IMPLEMENTED: "
    assert result.startswith(expected_start)
    assert "Reader for 'avro' not yet supported" in result


def test_format_error_handles_different_enum_values():
    for error_type in ErrorType:
        result = format_error(
            module="CORE",
            component="EnumTest",
            method="test_case",
            error_type=error_type,
            message="Sample error"
        )
        assert error_type.value in result
        assert "Sample error" in result
