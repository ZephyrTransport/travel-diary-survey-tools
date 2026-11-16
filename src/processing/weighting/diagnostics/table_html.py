"""HTML building blocks shared by the report's table modules.

``html_table`` gives every table one interface, from a simple grid to grouped,
spanned headers.
"""

import re


def profile_label(profile: str | None) -> str:
    """How a profile is named to a reader."""
    return profile or "the survey"


def profile_key(profile: str | None) -> str:
    """How a profile is named in markup and keys, for the toggle to address."""
    return profile or "survey"


def tag(name: str, text: str, **attrs: str) -> str:
    """Wrap *text* in an HTML element with optional attributes."""
    attr_str = "".join(f' {k.rstrip("_")}="{v}"' for k, v in attrs.items()) if attrs else ""
    return f"<{name}{attr_str}>{text}</{name}>"


def wbr(text: str) -> str:
    """Insert ``<wbr>`` word-break opportunities after ``/``, ``-``, ``_``, and spaces."""
    return re.sub(r"([/_\- ])", r"\1<wbr>", text)


def html_table(
    headers: list[str],
    rows: list[list[str]],
    *,
    group_row: list[tuple[str, int]] | None = None,
    css_class: str = "",
) -> str:
    r"""Build a ``<table>`` from *headers* and pre-formatted cell strings.

    Provides a consistent interface for generating HTML tables with single or
    multi-tier headers. Cells starting with ``<td`` are inserted as-is to support
    pre-styled cells (e.g., CSS classes for coloring convergence status).

    Args:
        headers: Column header labels for a single-row header, or sub-column labels
            when `group_row` is used.
        rows: Table body, one list per row. Each cell is a pre-formatted string.
        group_row: Optional two-tier header specification. Each tuple is
            ``(label, colspan)``. Columns with ``colspan == 1`` span both header rows
            (``rowspan="2"``). Columns with ``colspan > 1`` become group headers
            spanning multiple sub-columns (labels consumed from `headers` list).
        css_class: Optional CSS class name applied to the ``<table>`` element.

    Returns:
        Complete HTML ``<table>`` string.

    Example:
        Simple table:
        >>> html_table(["Zone", "Count"], [["A", "100"], ["B", "200"]])
        '<table>\n<tr><th>Zone</th><th>Count</th></tr>\n...'

        Grouped header:
        >>> html_table(
        ...     ["Target", "% Error"],  # sub-headers
        ...     [["A", "1000", "+2.5%"]],
        ...     group_row=[("Zone", 1), ("Household", 2)]  # Zone spans both rows
        ... )
    """
    cls = f' class="{css_class}"' if css_class else ""

    if group_row is not None:
        top_cells: list[str] = []
        sub_cells: list[str] = []
        for label, span in group_row:
            if span == 1:
                top_cells.append(f'<th rowspan="2">{label}</th>')
            else:
                top_cells.append(f'<th colspan="{span}">{label}</th>')
                sub_cells.extend(tag("th", headers.pop(0)) for _ in range(span))
        head = "<tr>" + "".join(top_cells) + "</tr>\n<tr>" + "".join(sub_cells) + "</tr>"
    else:
        head = "<tr>" + "".join(tag("th", h) for h in headers) + "</tr>"

    body = "\n".join(
        "<tr>" + "".join(c if c.startswith("<td") else tag("td", c) for c in cells) + "</tr>"
        for cells in rows
    )
    return f"<table{cls}>\n{head}\n{body}\n</table>"
