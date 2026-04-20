from app.api.routes.analyze import _normalize_proxy_list, _normalize_single_proxy


def test_normalize_proxy_add_scheme() -> None:
    assert _normalize_single_proxy("1.2.3.4:8080") == "http://1.2.3.4:8080"


def test_normalize_proxy_host_port_at_user_pass() -> None:
    source = "45.88.208.237:1508@user305829:oksbuf"
    expected = "http://user305829:oksbuf@45.88.208.237:1508"
    assert _normalize_single_proxy(source) == expected


def test_normalize_proxy_list_multiple() -> None:
    values = [
        "45.88.208.237:1508@user305829:oksbuf",
        "http://user:pass@10.1.1.1:3128",
        "10.2.2.2:8888",
    ]
    result = _normalize_proxy_list(values)
    assert result == (
        "http://user305829:oksbuf@45.88.208.237:1508,"
        "http://user:pass@10.1.1.1:3128,"
        "http://10.2.2.2:8888"
    )

