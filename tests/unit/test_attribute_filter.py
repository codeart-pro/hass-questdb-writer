"""Tests for the attribute allow/deny filter."""

from __future__ import annotations

import unittest

from custom_components.hass_questdb_writer.attribute_filter import (
    AttributeFilter,
    parse_attribute_patterns,
)


class AttributeFilterTests(unittest.TestCase):
    def test_empty_filter_writes_everything(self) -> None:
        attribute_filter = AttributeFilter()
        self.assertTrue(attribute_filter("friendly_name"))
        self.assertTrue(attribute_filter("rssi"))
        self.assertTrue(attribute_filter(""))

    def test_allow_list_is_strict(self) -> None:
        attribute_filter = AttributeFilter(allow=("friendly_name", "unit_of_measurement"))
        self.assertTrue(attribute_filter("friendly_name"))
        self.assertTrue(attribute_filter("unit_of_measurement"))
        self.assertFalse(attribute_filter("rssi"))
        self.assertFalse(attribute_filter("device_class"))

    def test_allow_wildcards(self) -> None:
        attribute_filter = AttributeFilter(allow=("unit_*",))
        self.assertTrue(attribute_filter("unit_of_measurement"))
        self.assertTrue(attribute_filter("unit_price"))
        self.assertFalse(attribute_filter("friendly_name"))

    def test_question_mark_wildcard(self) -> None:
        attribute_filter = AttributeFilter(allow=("unit_of_measuremen?",))
        self.assertTrue(attribute_filter("unit_of_measurement"))
        self.assertFalse(attribute_filter("unit_of_measurements"))

    def test_deny_list_removes(self) -> None:
        attribute_filter = AttributeFilter(deny=("rssi", "linkquality"))
        self.assertFalse(attribute_filter("rssi"))
        self.assertFalse(attribute_filter("linkquality"))
        self.assertTrue(attribute_filter("friendly_name"))

    def test_deny_wildcard(self) -> None:
        attribute_filter = AttributeFilter(deny=("update.*",))
        self.assertFalse(attribute_filter("update.available"))
        self.assertFalse(attribute_filter("update.installed_version"))
        self.assertTrue(attribute_filter("friendly_name"))

    def test_deny_wins_over_allow(self) -> None:
        attribute_filter = AttributeFilter(
            allow=("friendly_name", "rssi"),
            deny=("rssi",),
        )
        self.assertTrue(attribute_filter("friendly_name"))
        self.assertFalse(attribute_filter("rssi"))

    def test_matching_is_case_sensitive(self) -> None:
        attribute_filter = AttributeFilter(allow=("friendly_name",))
        self.assertFalse(attribute_filter("Friendly_Name"))

    def test_brackets_are_fnmatch_classes(self) -> None:
        attribute_filter = AttributeFilter(allow=("attr[0-9]",))
        self.assertTrue(attribute_filter("attr5"))
        self.assertFalse(attribute_filter("attrx"))


class ParseAttributePatternsTests(unittest.TestCase):
    def test_none_and_empty(self) -> None:
        self.assertEqual(parse_attribute_patterns(None), ())
        self.assertEqual(parse_attribute_patterns(""), ())
        self.assertEqual(parse_attribute_patterns("   , , "), ())

    def test_strips_and_splits(self) -> None:
        self.assertEqual(
            parse_attribute_patterns(" friendly_name , unit_* , rssi "),
            ("friendly_name", "unit_*", "rssi"),
        )

    def test_deduplicates_keeping_order(self) -> None:
        self.assertEqual(
            parse_attribute_patterns("rssi, linkquality, rssi"),
            ("rssi", "linkquality"),
        )


if __name__ == "__main__":
    unittest.main()
