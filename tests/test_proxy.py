from app.services.proxy import ProxyPool


def test_proxy_tier_escalation() -> None:
    pool = ProxyPool(
        datacenter_proxies="http://dc1:8000",
        residential_proxies="http://res1:8000",
        mobile_proxies="http://mob1:8000",
    )
    first = pool.choose(attempt=0, last_status=None)
    assert first.tier == "datacenter"
    second = pool.choose(attempt=2, last_status=429)
    assert second.tier == "residential"
    third = pool.choose(attempt=4, last_status=429)
    assert third.tier == "mobile"
