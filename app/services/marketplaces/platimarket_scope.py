from __future__ import annotations

from app.services.platimarket_client import PlatiCatalogNode, PlatiCategorySection


def build_scope_from_filters(
    *,
    catalog_tree: list[PlatiCatalogNode],
    category_group_id: int | None,
    category_ids: list[int],
    use_group_scope: bool,
    section_limit: int,
    query_value: str,
) -> tuple[list[PlatiCategorySection], list[str]]:
    warnings: list[str] = []
    section_by_id: dict[int, PlatiCategorySection] = {}
    node_children_by_id: dict[int, list[int]] = {}

    def walk(node: PlatiCatalogNode, root_id: int) -> None:
        section_by_id[node.section_id] = PlatiCategorySection(
            section_id=node.section_id,
            section_slug=node.section_slug,
            section_url=node.url,
            section_name=node.title,
            full_name=" > ".join(node.path) if node.path else node.title,
            counter_total=node.cnt,
            group_id=root_id,
        )
        node_children_by_id[node.section_id] = [child.section_id for child in node.children]
        for child in node.children:
            walk(child, root_id)

    for root in catalog_tree:
        walk(root, root.section_id)

    all_sections = list(section_by_id.values())

    selected_ids: set[int] = set()
    explicit_scope_selected = False
    if use_group_scope and category_group_id is not None:
        if category_group_id in section_by_id:
            selected_ids.add(category_group_id)
            explicit_scope_selected = True
        else:
            warnings.append("group_not_found")

    missing_sections = 0
    for section_id in category_ids:
        section = section_by_id.get(section_id)
        if section is None:
            missing_sections += 1
            continue
        selected_ids.add(section.section_id)
        explicit_scope_selected = True

    if missing_sections > 0:
        warnings.append("sections_not_found")

    expanded_ids: set[int] = set()

    def add_with_descendants(section_id: int) -> None:
        if section_id in expanded_ids:
            return
        expanded_ids.add(section_id)
        for child_id in node_children_by_id.get(section_id, []):
            add_with_descendants(child_id)

    for section_id in selected_ids:
        add_with_descendants(section_id)

    selected: dict[int, PlatiCategorySection] = {}
    for section_id in expanded_ids:
        section = section_by_id.get(section_id)
        if section is not None:
            selected[section.section_id] = section

    if not selected and query_value:
        ranked = sorted(
            all_sections,
            key=lambda item: (-(item.counter_total or 0), item.full_name.lower()),
        )
        for section in ranked[: max(1, section_limit)]:
            selected[section.section_id] = section

    scoped = list(selected.values())
    scoped.sort(key=lambda item: (item.group_id or 0, item.full_name.lower()))
    if not explicit_scope_selected and scoped:
        scoped = scoped[: max(1, section_limit)]
    return scoped, warnings
