from OceanDB.utils.basin_visualization import build_basin_map_html, parse_basin_kml


SAMPLE_KML = """\
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <Placemark>
      <ExtendedData>
        <SchemaData schemaUrl="#demo">
          <SimpleData name="feature_id">7</SimpleData>
          <SimpleData name="name">Demo Basin</SimpleData>
        </SchemaData>
      </ExtendedData>
      <Polygon>
        <outerBoundaryIs>
          <LinearRing>
            <coordinates>
              -10,10,0 10,10,0 10,-10,0 -10,-10,0 -10,10,0
            </coordinates>
          </LinearRing>
        </outerBoundaryIs>
      </Polygon>
    </Placemark>
  </Document>
</kml>
"""


def test_parse_basin_kml_extracts_polygon_metadata():
    basins = parse_basin_kml(SAMPLE_KML)

    assert len(basins) == 1
    assert basins[0].basin_id == 7
    assert basins[0].name == "Demo Basin"
    assert len(basins[0].rings) == 1


def test_build_basin_map_html_contains_label_and_index():
    basins = parse_basin_kml(SAMPLE_KML)

    html = build_basin_map_html(basins)

    assert "OceanDB Basins" in html
    assert 'data-basin-id="7"' in html
    assert ">7</text>" in html
    assert "<td>Demo Basin</td>" in html
    assert 'id="basin-id-input"' in html
    assert "Select Basin ID" in html
    assert "zoom-in-button" in html
    assert "function selectBasinById" in html
