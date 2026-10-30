import string
from datetime import datetime
from uuid import NAMESPACE_URL, UUID, uuid3

import pytest
from hypothesis import given
from hypothesis import strategies as st
from multiformats import multihash

import piddiplatsch.utils.models as utils

# ----------------------
# build_handle tests
# ----------------------


def test_build_handle_with_and_without_uri():
    pid = "12345"
    assert utils.build_handle(pid) == "21.TEST/12345"
    assert utils.build_handle(pid, as_uri=True) == "hdl:21.TEST/12345"


# ----------------------
# item_pid and asset_pid tests
# ----------------------


def test_item_pid_is_deterministic():
    item_id = "item-001"
    pid1 = utils.item_pid(item_id)
    pid2 = utils.item_pid(item_id)

    assert pid1 == pid2
    assert pid1 == str(uuid3(NAMESPACE_URL, item_id))


@given(st.text(alphabet=string.printable))
def test_item_pid_always_valid_uuid(item_id):
    pid = utils.item_pid(item_id)
    assert isinstance(UUID(pid), UUID)


def test_asset_pid_is_deterministic():
    item_id = "item-001"
    asset_key = "asset-a"

    pid1 = utils.asset_pid(item_id, asset_key)
    pid2 = utils.asset_pid(item_id, asset_key)

    assert pid1 == pid2
    assert pid1 == str(uuid3(NAMESPACE_URL, f"{item_id}#{asset_key}"))


@given(
    item_id=st.text(alphabet=string.printable),
    asset_key=st.text(alphabet=string.printable),
)
def test_asset_pid_always_valid_uuid(item_id, asset_key):
    pid = utils.asset_pid(item_id, asset_key)
    assert isinstance(UUID(pid), UUID)


# ----------------------
# drop_empty tests
# ----------------------


@pytest.mark.parametrize(
    "data,expected",
    [
        ({"a": "", "b": None, "c": [], "d": {}, "e": 42}, {"e": 42}),
        ({"nested": {"x": "", "y": "val"}}, {"nested": {"y": "val"}}),
        ({}, {}),
    ],
)
def test_drop_empty(data, expected):
    assert utils.drop_empty(data) == expected


@given(
    st.dictionaries(
        st.text(alphabet=string.printable, min_size=1, max_size=5),
        st.one_of(
            st.none(),
            st.integers(),
            st.text(alphabet=string.printable, min_size=1, max_size=10),
        ),
        max_size=5,
    )
)
def test_drop_empty_no_empty_fields_in_result(d):
    cleaned = utils.drop_empty(d)
    assert all(v not in ("", [], {}, None) for v in cleaned.values())


# ----------------------
# parse_datetime tests
# ----------------------


def test_parse_datetime_valid_and_invalid():
    valid = "2021-09-01T12:34:56Z"
    dt = utils.parse_datetime(valid)
    assert isinstance(dt, datetime)
    assert dt.isoformat() == "2021-09-01T12:34:56+00:00"

    invalid = "not-a-datetime"
    result = utils.parse_datetime(invalid)
    assert result is None

    assert utils.parse_datetime(None) is None


@given(st.datetimes())
def test_parse_datetime_roundtrip(dt):
    iso = dt.isoformat()
    parsed = utils.parse_datetime(iso)
    assert isinstance(parsed, datetime)


# ----------------------
# parse_pid tests
# ----------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("21.TEST/abc123", "abc123"),
        ("abc123", "abc123"),
        ("", ""),
        (None, None),
    ],
)
def test_parse_pid(value, expected):
    assert utils.parse_pid(value) == expected


@given(st.text(alphabet=string.printable))
def test_parse_pid_never_crashes(s):
    result = utils.parse_pid(s)
    assert result is None or isinstance(result, str)


# ----------------------
# parse_multihash_checksum tests
# ----------------------


def test_parse_multihash_checksum_sha256():
    mh_hex = "1220" + "5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5"
    checksum_type, checksum_hex = utils.parse_multihash_checksum(mh_hex)

    assert checksum_type == "sha2-256"
    assert (
        checksum_hex
        == "5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5"
    )


def test_double_sha256_prefix_has_actionable_error():
    checksum = (
        "1220122093a5616803c97b154d6eeba73fffc0b871912a2dea5d7f45c77564023970c477"
    )
    with pytest.raises(
        ValueError, match="32-byte digest but contains 34 bytes"
    ) as error:
        utils.parse_multihash_checksum(checksum)
    assert "'1220' prefix appears to have been added twice" in str(error.value)


@pytest.mark.parametrize("checksum", [None, "", "not-hex"])
def test_malformed_checksum_has_readable_error(checksum):
    with pytest.raises(ValueError, match="expected a hexadecimal multihash"):
        utils.parse_multihash_checksum(checksum)


@pytest.mark.parametrize(
    "digest_bytes,code_name",
    [
        (b"hello world", "sha1"),
        (b"another test", "sha2-256"),
        (b"123456", "sha2-512"),
        (b"hamburg", "sha3-512"),
        (b"berlin", "md5"),
        (b"bremen", "blake2b-256"),
    ],
)
def test_parse_multihash_various(digest_bytes, code_name):
    # Encode a multihash
    mh_hex = multihash.digest(digest_bytes, code_name).hex()

    # Parse
    cmethod, chex = utils.parse_multihash_checksum(mh_hex)

    assert cmethod == code_name
    assert utils.is_hex(chex)
