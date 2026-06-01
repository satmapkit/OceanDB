from __future__ import annotations

import click

from OceanDB.cli_utils import format_status_line, render_table, style_value


def _format_metric(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.3f}"


def create_query_analysis_runner():
    from OceanDB.query_analysis import QueryAnalysisRunner

    return QueryAnalysisRunner()


@click.command("analyze-queries")
def analyze_queries():
    """Run representative queries and show their index usage matrix."""
    runner = create_query_analysis_runner()
    rows = runner.analyze_queries()
    all_indices = runner.index_names
    used_indices = set.union(*[row.used_indices for row in rows])
    unused_indices = all_indices - used_indices

    used_index_names = sorted(used_indices)
    unused_index_names = sorted(unused_indices)

    click.echo(
        format_status_line(
            "ANALYZE",
            f"Analyzed {len(rows)} query scenario(s).",
            label_color="magenta",
        )
    )

    summary_headers = [
        ("Query", "scenario_name"),
        ("Total Cost", "total_cost"),
        ("Total Time (ms)", "total_time"),
    ]
    summary_rows = [
        {
            "scenario_name": row.scenario_name,
            "total_cost": _format_metric(row.total_cost),
            "total_time": _format_metric(row.total_time),
        }
        for row in rows
    ]

    def _summary_cell_styler(column: str, value: str) -> str:
        if column == "scenario_name":
            return style_value(value, fg="cyan", bold=True)
        return style_value(value, fg="white")

    for line in render_table(
        summary_headers,
        summary_rows,
        cell_styler=_summary_cell_styler,
    ):
        click.echo(line)

    click.echo()

    headers = [("Query", "scenario_name")] + [
        (f"{i}", index_name) for i, index_name in enumerate(used_index_names)
    ]
    formatted_rows = []
    for row in rows:
        formatted_row = {"scenario_name": row.scenario_name}
        for index_name in used_index_names:
            formatted_row[index_name] = "X" if index_name in row.used_indices else ""
        formatted_rows.append(formatted_row)

    def _analysis_cell_styler(column: str, value: str) -> str:
        if column == "scenario_name":
            return style_value(value, fg="cyan", bold=True)
        if value.strip() == "X":
            return style_value(value, fg="green", bold=True)
        return style_value(value, fg="white")

    for line in render_table(
        headers, formatted_rows, cell_styler=_analysis_cell_styler
    ):
        click.echo(line)

    click.echo(
        format_status_line(
            "USED",
            "\n" + "\n".join(f"{i}: {n}" for i, n in enumerate(used_index_names))
            or "-",
            label_color="green",
            message_color="green",
        )
    )
    click.echo(
        format_status_line(
            "UNUSED",
            "\n" + "\n".join(unused_index_names) or "-",
            label_color="yellow",
            message_color="yellow",
        )
    )
