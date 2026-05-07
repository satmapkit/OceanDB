from __future__ import annotations

from collections.abc import Callable, Iterable

import click

CellStyler = Callable[[str, str], str]


def style_label(text: str, fg: str = "cyan") -> str:
    return click.style(text, fg=fg, bold=True)


def style_value(text: str, fg: str = "white", *, bold: bool = False) -> str:
    return click.style(text, fg=fg, bold=bold)


def format_status_line(
    label: str,
    message: str,
    *,
    label_color: str = "cyan",
    message_color: str | None = None,
) -> str:
    styled_label = style_label(f"[{label}]", fg=label_color)
    if message_color is None:
        return f"{styled_label} {message}"
    return f"{styled_label} {click.style(message, fg=message_color)}"


def format_key_value(
    label: str,
    value: str,
    *,
    label_color: str = "cyan",
    value_color: str = "white",
) -> str:
    return f"{style_label(label, fg=label_color)} {style_value(value, fg=value_color)}"


def render_table(
    headers: Iterable[tuple[str, str]],
    rows: list[dict[str, str]],
    *,
    header_color: str = "cyan",
    separator_color: str = "blue",
    cell_styler: CellStyler | None = None,
) -> list[str]:
    headers = list(headers)
    widths = {
        key: max(len(title), *(len(row[key]) for row in rows)) for title, key in headers
    }

    header_line = "  ".join(
        style_label(title.ljust(widths[key]), fg=header_color) for title, key in headers
    )
    separator_line = "  ".join(
        click.style("-" * widths[key], fg=separator_color) for _, key in headers
    )

    table_lines = [header_line, separator_line]
    for row in rows:
        table_lines.append(
            "  ".join(
                (
                    cell_styler(key, row[key].ljust(widths[key]))
                    if cell_styler
                    else row[key].ljust(widths[key])
                )
                for _, key in headers
            )
        )

    return table_lines
