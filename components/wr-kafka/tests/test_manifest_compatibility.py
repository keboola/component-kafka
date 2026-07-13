import unittest

from keboola.component.dao import TableMetadata


class TestManifestCompatibility(unittest.TestCase):
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

        table_metadata = TableMetadata(manifest)

        self.assertEqual({"source": "storage"}, table_metadata.table_metadata)
        self.assertEqual({}, table_metadata.column_metadata)
