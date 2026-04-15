from __future__ import annotations

from dataclasses import dataclass
from html import escape
from importlib import resources
from pathlib import Path
import textwrap
import xml.etree.ElementTree as ET
import zipfile


KML_NS = {"kml": "http://www.opengis.net/kml/2.2"}
SVG_WIDTH = 1440
SVG_HEIGHT = 720


@dataclass(frozen=True)
class BasinPolygon:
    basin_id: int
    name: str
    rings: list[list[tuple[float, float]]]


@dataclass(frozen=True)
class BasinLabel:
    basin_id: int
    name: str
    x: float
    y: float


@dataclass(frozen=True)
class BasinBounds:
    basin_id: int
    min_x: float
    min_y: float
    max_x: float
    max_y: float


def _parse_coordinates(raw_coordinates: str) -> list[tuple[float, float]]:
    coordinates: list[tuple[float, float]] = []
    for point in raw_coordinates.split():
        lon_text, lat_text, *_ = point.split(",")
        coordinates.append((float(lon_text), float(lat_text)))
    return coordinates


def _polygon_area(points: list[tuple[float, float]]) -> float:
    if len(points) < 3:
        return 0.0

    area = 0.0
    for index, (x1, y1) in enumerate(points):
        x2, y2 = points[(index + 1) % len(points)]
        area += (x1 * y2) - (x2 * y1)
    return abs(area) / 2.0


def _polygon_centroid(points: list[tuple[float, float]]) -> tuple[float, float]:
    if len(points) < 3:
        xs = [point[0] for point in points] or [0.0]
        ys = [point[1] for point in points] or [0.0]
        return sum(xs) / len(xs), sum(ys) / len(ys)

    area_factor = 0.0
    centroid_x = 0.0
    centroid_y = 0.0
    for index, (x1, y1) in enumerate(points):
        x2, y2 = points[(index + 1) % len(points)]
        cross = (x1 * y2) - (x2 * y1)
        area_factor += cross
        centroid_x += (x1 + x2) * cross
        centroid_y += (y1 + y2) * cross

    if area_factor == 0:
        xs = [point[0] for point in points]
        ys = [point[1] for point in points]
        return sum(xs) / len(xs), sum(ys) / len(ys)

    area_factor *= 0.5
    return centroid_x / (6 * area_factor), centroid_y / (6 * area_factor)


def _project(lon: float, lat: float) -> tuple[float, float]:
    x = ((lon + 180.0) / 360.0) * SVG_WIDTH
    y = ((90.0 - lat) / 180.0) * SVG_HEIGHT
    return x, y


def parse_basin_kml(kml_text: str) -> list[BasinPolygon]:
    root = ET.fromstring(kml_text)
    basins: list[BasinPolygon] = []

    for placemark in root.findall(".//kml:Placemark", KML_NS):
        feature_id = placemark.findtext(
            ".//kml:SimpleData[@name='feature_id']", namespaces=KML_NS
        )
        name = placemark.findtext(
            ".//kml:SimpleData[@name='name']", namespaces=KML_NS
        )

        if feature_id is None or name is None:
            continue

        rings: list[list[tuple[float, float]]] = []
        for coordinates in placemark.findall(
            ".//kml:Polygon//kml:LinearRing/kml:coordinates", KML_NS
        ):
            points = _parse_coordinates(coordinates.text or "")
            if len(points) >= 3:
                rings.append(points)

        if rings:
            basins.append(BasinPolygon(basin_id=int(feature_id), name=name, rings=rings))

    basins.sort(key=lambda basin: basin.basin_id)
    return basins


def load_packaged_basins() -> list[BasinPolygon]:
    kmz_path = resources.files("OceanDB.data").joinpath(
        "basin_masks/NASA-SSH Basins.kmz"
    )
    with zipfile.ZipFile(kmz_path) as kmz_file:
        kml_text = kmz_file.read("doc.kml").decode("utf-8")
    return parse_basin_kml(kml_text)


def basin_labels(basins: list[BasinPolygon]) -> list[BasinLabel]:
    labels: list[BasinLabel] = []
    for basin in basins:
        largest_ring = max(basin.rings, key=_polygon_area)
        projected_ring = [_project(lon, lat) for lon, lat in largest_ring]
        label_x, label_y = _polygon_centroid(projected_ring)
        labels.append(
            BasinLabel(
                basin_id=basin.basin_id,
                name=basin.name,
                x=label_x,
                y=label_y,
            )
        )
    return labels


def basin_bounds(basins: list[BasinPolygon]) -> list[BasinBounds]:
    bounds: list[BasinBounds] = []
    for basin in basins:
        projected_points = [
            _project(lon, lat) for ring in basin.rings for lon, lat in ring
        ]
        xs = [point[0] for point in projected_points]
        ys = [point[1] for point in projected_points]
        bounds.append(
            BasinBounds(
                basin_id=basin.basin_id,
                min_x=min(xs),
                min_y=min(ys),
                max_x=max(xs),
                max_y=max(ys),
            )
        )
    return bounds


def basin_svg_paths(basins: list[BasinPolygon]) -> str:
    path_elements: list[str] = []

    for basin in basins:
        commands: list[str] = []
        for ring in basin.rings:
            projected = [_project(lon, lat) for lon, lat in ring]
            first_x, first_y = projected[0]
            commands.append(f"M {first_x:.2f} {first_y:.2f}")
            for x, y in projected[1:]:
                commands.append(f"L {x:.2f} {y:.2f}")
            commands.append("Z")

        basin_title = escape(f"{basin.basin_id}: {basin.name}")
        path_elements.append(
            (
                f'<path class="basin" d="{" ".join(commands)}" '
                f'data-basin-id="{basin.basin_id}" data-basin-name="{escape(basin.name)}">'
                f"<title>{basin_title}</title></path>"
            )
        )

    return "\n".join(path_elements)


def basin_svg_labels(basins: list[BasinPolygon]) -> str:
    label_elements: list[str] = []
    for label in basin_labels(basins):
        title = escape(f"{label.basin_id}: {label.name}")
        label_elements.append(
            (
                f'<g class="basin-label" data-basin-id="{label.basin_id}">'
                f'<title>{title}</title>'
                f'<text x="{label.x:.2f}" y="{label.y:.2f}" class="basin-id">'
                f"{label.basin_id}</text>"
                f"</g>"
            )
        )
    return "\n".join(label_elements)


def build_basin_map_html(basins: list[BasinPolygon]) -> str:
    bounds_map = {
        basin_bound.basin_id: {
            "min_x": round(basin_bound.min_x, 2),
            "min_y": round(basin_bound.min_y, 2),
            "max_x": round(basin_bound.max_x, 2),
            "max_y": round(basin_bound.max_y, 2),
        }
        for basin_bound in basin_bounds(basins)
    }
    basin_rows = "\n".join(
        (
            f'<tr data-basin-row="{basin.basin_id}"><td>{basin.basin_id}</td><td>{escape(basin.name)}</td></tr>'
            for basin in basins
        )
    )

    return textwrap.dedent(
        f"""\
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>OceanDB Basin Map</title>
          <style>
            :root {{
              color-scheme: light;
              --ink: #082032;
              --accent: #1c6e8c;
              --bg: #f8fbfd;
              --panel: #ffffff;
              --grid: #c3d9e2;
              --fill: rgba(28, 110, 140, 0.18);
            }}
            * {{
              box-sizing: border-box;
            }}
            body {{
              margin: 0;
              font-family: "Avenir Next", "Segoe UI", sans-serif;
              color: var(--ink);
              background:
                radial-gradient(circle at top left, #dff4ff 0%, transparent 28%),
                linear-gradient(180deg, #eef7fb 0%, #f8fbfd 100%);
            }}
            main {{
              display: grid;
              grid-template-columns: minmax(0, 1fr) 320px;
              gap: 1rem;
              padding: 1rem;
              min-height: 100vh;
            }}
            .panel {{
              background: rgba(255, 255, 255, 0.88);
              border: 1px solid rgba(8, 32, 50, 0.08);
              border-radius: 18px;
              box-shadow: 0 14px 40px rgba(8, 32, 50, 0.08);
              backdrop-filter: blur(8px);
            }}
            .map-panel {{
              padding: 1rem;
            }}
            .toolbar {{
              display: flex;
              gap: 0.75rem;
              flex-wrap: wrap;
              align-items: end;
              margin-bottom: 1rem;
            }}
            .toolbar-group {{
              display: flex;
              flex-direction: column;
              gap: 0.35rem;
            }}
            label {{
              font-size: 0.82rem;
              font-weight: 700;
              letter-spacing: 0.02em;
            }}
            input {{
              min-width: 10rem;
              padding: 0.65rem 0.8rem;
              border: 1px solid rgba(8, 32, 50, 0.18);
              border-radius: 10px;
              font: inherit;
              background: rgba(255, 255, 255, 0.95);
            }}
            button {{
              padding: 0.65rem 0.9rem;
              border: 1px solid rgba(8, 32, 50, 0.16);
              border-radius: 10px;
              background: #ffffff;
              color: var(--ink);
              font: inherit;
              font-weight: 700;
              cursor: pointer;
            }}
            button:hover {{
              border-color: rgba(28, 110, 140, 0.45);
              background: #f1f8fb;
            }}
            .status {{
              min-height: 1.2rem;
              font-size: 0.9rem;
              color: rgba(8, 32, 50, 0.82);
            }}
            h1 {{
              margin: 0 0 0.35rem;
              font-size: 1.4rem;
              letter-spacing: 0.02em;
            }}
            p {{
              margin: 0 0 1rem;
              color: rgba(8, 32, 50, 0.8);
            }}
            svg {{
              width: 100%;
              height: auto;
              border-radius: 14px;
              background:
                linear-gradient(180deg, rgba(28, 110, 140, 0.08), rgba(28, 110, 140, 0.03));
            }}
            .graticule {{
              stroke: var(--grid);
              stroke-width: 1;
              fill: none;
              opacity: 0.7;
            }}
            .basin {{
              fill: var(--fill);
              stroke: var(--accent);
              stroke-width: 1;
              vector-effect: non-scaling-stroke;
              transition: fill 140ms ease, stroke 140ms ease;
            }}
            .basin:hover {{
              fill: rgba(28, 110, 140, 0.35);
              stroke: #0b4f6c;
            }}
            .basin.selected {{
              fill: rgba(228, 137, 39, 0.45);
              stroke: #d96c06;
              stroke-width: 2;
            }}
            .basin-label.selected .basin-id {{
              fill: #d96c06;
            }}
            .basin-id {{
              font-size: 10px;
              font-weight: 700;
              fill: #04151f;
              text-anchor: middle;
              paint-order: stroke;
              stroke: rgba(255, 255, 255, 0.95);
              stroke-width: 3px;
              stroke-linejoin: round;
            }}
            .table-panel {{
              padding: 1rem;
              display: flex;
              flex-direction: column;
              min-height: 0;
              max-height: calc(100vh - 2rem);
            }}
            .table-wrap {{
              overflow: auto;
              border-radius: 12px;
              min-height: 0;
            }}
            table {{
              width: 100%;
              border-collapse: collapse;
              font-size: 0.94rem;
            }}
            th, td {{
              text-align: left;
              padding: 0.5rem 0;
              border-bottom: 1px solid rgba(8, 32, 50, 0.08);
              vertical-align: top;
            }}
            th {{
              position: sticky;
              top: 0;
              background: var(--panel);
              z-index: 1;
            }}
            tr.selected {{
              background: rgba(228, 137, 39, 0.14);
            }}
            td:first-child {{
              width: 4rem;
              font-variant-numeric: tabular-nums;
              font-weight: 700;
            }}
            @media (max-width: 1024px) {{
              main {{
                grid-template-columns: 1fr;
              }}
              .table-panel {{
                max-height: none;
              }}
              .table-wrap {{
                max-height: 28rem;
              }}
            }}
          </style>
        </head>
        <body>
          <main>
            <section class="panel map-panel">
              <h1>OceanDB Basins</h1>
              <p>Packaged NASA SSH basin polygons with basin IDs rendered from the repository KMZ asset.</p>
              <div class="toolbar">
                <div class="toolbar-group">
                  <label for="basin-id-input">Select Basin ID</label>
                  <input id="basin-id-input" type="number" min="1" placeholder="Enter a basin ID">
                </div>
                <button type="button" id="select-basin-button">Find Basin</button>
                <button type="button" id="zoom-in-button">Zoom In</button>
                <button type="button" id="zoom-out-button">Zoom Out</button>
                <button type="button" id="reset-view-button">Reset View</button>
              </div>
              <div class="status" id="map-status">Tip: use the mouse wheel to zoom and drag to pan.</div>
              <svg id="basin-map" viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}" role="img" aria-label="OceanDB basin map">
                <title>OceanDB basin map with basin IDs</title>
                <g id="viewport">
                <g>
                  <path class="graticule" d="M 0 180 L {SVG_WIDTH} 180 M 0 360 L {SVG_WIDTH} 360 M 0 540 L {SVG_WIDTH} 540" />
                  <path class="graticule" d="M 360 0 L 360 {SVG_HEIGHT} M 720 0 L 720 {SVG_HEIGHT} M 1080 0 L 1080 {SVG_HEIGHT}" />
                </g>
                <g>
                  {basin_svg_paths(basins)}
                </g>
                <g>
                  {basin_svg_labels(basins)}
                </g>
                </g>
              </svg>
            </section>
            <aside class="panel table-panel">
              <h1>Basin Index</h1>
              <p>{len(basins)} basins</p>
              <div class="table-wrap" id="table-wrap">
                <table>
                  <thead>
                    <tr><th>ID</th><th>Name</th></tr>
                  </thead>
                  <tbody>
                    {basin_rows}
                  </tbody>
                </table>
              </div>
            </aside>
          </main>
          <script>
            const svg = document.getElementById("basin-map");
            const viewport = document.getElementById("viewport");
            const basinInput = document.getElementById("basin-id-input");
            const status = document.getElementById("map-status");
            const tableWrap = document.getElementById("table-wrap");
            const boundsById = {bounds_map};
            const defaultView = {{ x: 0, y: 0, scale: 1 }};
            const view = {{ ...defaultView }};
            const minScale = 1;
            const maxScale = 18;
            let dragState = null;

            function clampScale(nextScale) {{
              return Math.min(maxScale, Math.max(minScale, nextScale));
            }}

            function applyView() {{
              viewport.setAttribute(
                "transform",
                `translate(${{view.x}} ${{view.y}}) scale(${{view.scale}})`
              );
            }}

            function setStatus(message) {{
              status.textContent = message;
            }}

            function clearSelection() {{
              document.querySelectorAll(".basin.selected").forEach((node) => node.classList.remove("selected"));
              document.querySelectorAll(".basin-label.selected").forEach((node) => node.classList.remove("selected"));
              document.querySelectorAll("tr.selected").forEach((node) => node.classList.remove("selected"));
            }}

            function highlightBasin(basinId) {{
              clearSelection();
              const basin = document.querySelector(`.basin[data-basin-id="${{basinId}}"]`);
              const label = document.querySelector(`.basin-label[data-basin-id="${{basinId}}"]`);
              const row = document.querySelector(`tr[data-basin-row="${{basinId}}"]`);
              if (basin) basin.classList.add("selected");
              if (label) label.classList.add("selected");
              if (row) row.classList.add("selected");
              if (row && tableWrap) {{
                row.scrollIntoView({{ block: "nearest", inline: "nearest" }});
              }}
            }}

            function zoomAtPoint(factor, screenX, screenY) {{
              const rect = svg.getBoundingClientRect();
              const anchorX = ((screenX - rect.left) / rect.width) * {SVG_WIDTH};
              const anchorY = ((screenY - rect.top) / rect.height) * {SVG_HEIGHT};
              const nextScale = clampScale(view.scale * factor);
              const worldX = (anchorX - view.x) / view.scale;
              const worldY = (anchorY - view.y) / view.scale;
              view.x = anchorX - (worldX * nextScale);
              view.y = anchorY - (worldY * nextScale);
              view.scale = nextScale;
              applyView();
            }}

            function fitToBasin(basinId) {{
              const bounds = boundsById[basinId];
              if (!bounds) {{
                setStatus(`Basin ID ${{basinId}} was not found.`);
                return;
              }}

              const basinWidth = Math.max(bounds.max_x - bounds.min_x, 1);
              const basinHeight = Math.max(bounds.max_y - bounds.min_y, 1);
              const basinAreaFraction = (basinWidth * basinHeight) / ({SVG_WIDTH} * {SVG_HEIGHT});
              const margin = basinAreaFraction < 0.01
                ? 240
                : basinAreaFraction < 0.03
                  ? 210
                  : basinAreaFraction < 0.08
                    ? 180
                    : 150;
              const scaleX = ({SVG_WIDTH} - margin * 2) / basinWidth;
              const scaleY = ({SVG_HEIGHT} - margin * 2) / basinHeight;
              const fitScale = Math.min(scaleX, scaleY);
              const adaptiveScaleCap = basinAreaFraction < 0.01
                ? 2.2
                : basinAreaFraction < 0.03
                  ? 3.0
                  : basinAreaFraction < 0.08
                    ? 4.2
                    : 6.0;
              view.scale = clampScale(Math.min(fitScale, adaptiveScaleCap));
              const centerX = (bounds.min_x + bounds.max_x) / 2;
              const centerY = (bounds.min_y + bounds.max_y) / 2;
              view.x = ({SVG_WIDTH} / 2) - (centerX * view.scale);
              view.y = ({SVG_HEIGHT} / 2) - (centerY * view.scale);
              applyView();
            }}

            function selectBasinById(rawValue) {{
              const basinId = Number(rawValue);
              if (!Number.isInteger(basinId)) {{
                setStatus("Enter a valid integer basin ID.");
                basinInput.focus();
                return;
              }}

              if (!boundsById[basinId]) {{
                clearSelection();
                setStatus(`Basin ID ${{basinId}} was not found.`);
                return;
              }}

              highlightBasin(basinId);
              fitToBasin(basinId);
              const basinName = document.querySelector(`.basin[data-basin-id="${{basinId}}"]`)?.dataset.basinName;
              setStatus(`Selected basin ${{basinId}}${{basinName ? `: ${{basinName}}` : ""}}.`);
            }}

            document.getElementById("select-basin-button").addEventListener("click", () => {{
              selectBasinById(basinInput.value);
            }});

            basinInput.addEventListener("keydown", (event) => {{
              if (event.key === "Enter") {{
                selectBasinById(basinInput.value);
              }}
            }});

            document.getElementById("zoom-in-button").addEventListener("click", () => {{
              zoomAtPoint(1.25, svg.clientWidth / 2, svg.clientHeight / 2);
            }});

            document.getElementById("zoom-out-button").addEventListener("click", () => {{
              zoomAtPoint(0.8, svg.clientWidth / 2, svg.clientHeight / 2);
            }});

            document.getElementById("reset-view-button").addEventListener("click", () => {{
              Object.assign(view, defaultView);
              applyView();
              clearSelection();
              setStatus("View reset.");
            }});

            svg.addEventListener("wheel", (event) => {{
              event.preventDefault();
              const factor = event.deltaY < 0 ? 1.12 : 0.88;
              zoomAtPoint(factor, event.clientX, event.clientY);
            }}, {{ passive: false }});

            svg.addEventListener("pointerdown", (event) => {{
              dragState = {{
                pointerId: event.pointerId,
                startX: event.clientX,
                startY: event.clientY,
                originX: view.x,
                originY: view.y,
              }};
              svg.setPointerCapture(event.pointerId);
            }});

            svg.addEventListener("pointermove", (event) => {{
              if (!dragState || event.pointerId !== dragState.pointerId) {{
                return;
              }}
              view.x = dragState.originX + (event.clientX - dragState.startX);
              view.y = dragState.originY + (event.clientY - dragState.startY);
              applyView();
            }});

            svg.addEventListener("pointerup", (event) => {{
              if (dragState && event.pointerId === dragState.pointerId) {{
                svg.releasePointerCapture(event.pointerId);
                dragState = null;
              }}
            }});

            svg.addEventListener("pointerleave", () => {{
              dragState = null;
            }});

            document.querySelectorAll(".basin").forEach((node) => {{
              node.addEventListener("click", () => {{
                const basinId = node.dataset.basinId;
                basinInput.value = basinId;
                selectBasinById(basinId);
              }});
            }});

            document.querySelectorAll("tr[data-basin-row]").forEach((row) => {{
              row.addEventListener("click", () => {{
                const basinId = row.dataset.basinRow;
                basinInput.value = basinId;
                selectBasinById(basinId);
              }});
            }});

            applyView();
          </script>
        </body>
        </html>
        """
    )


def write_basin_map(output_path: Path) -> Path:
    basins = load_packaged_basins()
    html = build_basin_map_html(basins)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path
