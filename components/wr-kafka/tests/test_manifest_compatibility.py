import unittest

from keboola.component.dao import TableMetadata


class TestManifestCompatibility(unittest.TestCase):
    """Regression test for CFTL-725 / SUPPORT-16970.

    Projects on Native Data Types produce input manifests that contain BOTH the
    new ``schema`` block and the legacy ``metadata``/``column_metadata``/``columns``
    keys. keboola.component <1.11 rejected such hybrid manifests with
    ``UserException: Manifest can't contain new 'schema' and old ...``. This test
    guards that the pinned SDK (>=1.11.0) parses a hybrid manifest without raising.
    """

    def test_input_manifest_with_schema_and_legacy_keys(self):
        manifest = {
            "columns": ["row_number", "payload"],
            "metadata": [{"key": "source", "value": "storage"}],
            "column_metadata": {"payload": [{"key": "type", "value": "STRING"}]},
            "schema": [
                {
                    "name": "row_number",
                    "data_type": {"base": {"type": "INTEGER"}},
                    "nullable": False,
                    "primary_key": False,
                },
                {
                    "name": "payload",
                    "data_type": {"base": {"type": "STRING"}},
                    "nullable": True,
                    "primary_key": False,
                },
            ],
        }

        # Must not raise on a hybrid manifest.
        table_metadata = TableMetadata(manifest)
        self.assertEqual({"source": "storage"}, table_metadata.table_metadata)


if __name__ == "__main__":
    unittest.main()
