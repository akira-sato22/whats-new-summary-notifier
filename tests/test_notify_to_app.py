import unittest

from tests.util import load_lambda_module

notify = load_lambda_module(
    "notify_to_app_index",
    "notify-to-app",
    env={
        "MODEL_ID": "us.amazon.nova-pro-v1:0",
        "MODEL_REGION": "us-east-1",
        "NOTIFIERS": "{}",
        "SUMMARIZERS": "{}",
        "DDB_TABLE_NAME": "AWSUpdatesRSSHistory",
    },
)


def make_record(event_name, new_image, old_image=None):
    record = {
        "eventName": event_name,
        "dynamodb": {"NewImage": new_image},
    }
    if old_image is not None:
        record["dynamodb"]["OldImage"] = old_image
    return record


def make_image(title, url, category="Whats new", pubtime="2024-01-01T00:00:00"):
    return {
        "category": {"S": category},
        "pubtime": {"S": pubtime},
        "title": {"S": title},
        "url": {"S": url},
        "notifier_name": {"S": "AwsWhatsNew"},
    }


class TestGetNewEntries(unittest.TestCase):
    def test_insert_record_is_extracted(self):
        records = [make_record("INSERT", make_image("Title A", "https://example.com/a"))]
        result = notify.get_new_entries(records)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["rss_title"], "Title A")
        self.assertEqual(result[0]["rss_link"], "https://example.com/a")
        self.assertEqual(result[0]["rss_notifier_name"], "AwsWhatsNew")

    def test_remove_record_is_ignored(self):
        records = [make_record("REMOVE", make_image("Title A", "https://example.com/a"))]
        self.assertEqual(notify.get_new_entries(records), [])

    def test_modify_without_key_field_change_is_skipped(self):
        image = make_image("Title A", "https://example.com/a")
        records = [make_record("MODIFY", image, old_image=image)]
        self.assertEqual(notify.get_new_entries(records), [])

    def test_modify_with_title_change_is_extracted(self):
        new_image = make_image("New Title", "https://example.com/a")
        old_image = make_image("Old Title", "https://example.com/a")
        records = [make_record("MODIFY", new_image, old_image=old_image)]
        result = notify.get_new_entries(records)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["rss_title"], "New Title")


class TestCreateTeamsMessage(unittest.TestCase):
    def test_message_contains_title_summary_and_link(self):
        item = {
            "rss_title": "Title A",
            "rss_link": "https://example.com/a",
            "summary": "summary text",
            "detail": "detail text",
        }
        message = notify.create_teams_message(item)
        card = message["attachments"][0]["content"]
        self.assertEqual(card["actions"][0]["url"], "https://example.com/a")
        body_text = str(card["body"])
        self.assertIn("Title A", body_text)
        self.assertIn("summary text", body_text)
        self.assertIn("detail text", body_text)


if __name__ == "__main__":
    unittest.main()
