from __future__ import annotations


def normalize_single_proxy(value: str) -> str:
    raw = value.strip()
    if not raw:
        return raw

    if raw.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return raw

    if "@" in raw:
        left, right = raw.split("@", 1)
        left = left.strip()
        right = right.strip()
        left_parts = left.rsplit(":", 1)
        right_parts = right.split(":", 1)
        if len(left_parts) == 2 and left_parts[1].isdigit() and len(right_parts) == 2:
            user, password = right_parts
            host, port = left_parts
            if user and password and host and port:
                return f"http://{user}:{password}@{host}:{port}"
        return f"http://{left}@{right}"

    if ":" in raw:
        return f"http://{raw}"

    return raw


def normalize_proxy_list(raw_values: list[str] | None) -> str | None:
    if raw_values is None:
        return None
    values = [normalize_single_proxy(item) for item in raw_values if item and item.strip()]
    return ",".join(values)
