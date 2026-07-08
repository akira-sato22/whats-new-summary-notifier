import unittest

from tests.util import load_lambda_module

generator = load_lambda_module(
    "markdown_generator_index",
    "markdown-generator",
    env={
        "DDB_TABLE_NAME": "AWSUpdatesRSSHistory",
        "S3_BUCKET_NAME": "test-bucket",
        "SLACK_BOT_TOKEN_PARAMETER": "/WhatsNew/SLACK_BOT_TOKEN",
        "SLACK_CHANNEL_ID_PARAMETER": "/WhatsNew/SLACK_CHANNEL_ID",
    },
)


def make_item(title, category, pubtime="2024-01-01T00:00:00", detail=""):
    return {
        "title": title,
        "url": f"https://example.com/{title}",
        "category": category,
        "pubtime": pubtime,
        "detail": detail,
    }


class TestGroupNewsByCategoryType(unittest.TestCase):
    def test_whats_new_and_others_are_split(self):
        items = [
            make_item("a", "Whats new"),
            make_item("b", "AWS blog"),
            make_item("c", "Whats new"),
        ]
        groups = generator.group_news_by_category_type(items)
        self.assertEqual(len(groups["whats-new"]), 2)
        self.assertEqual(len(groups["others"]), 1)

    def test_item_without_category_goes_to_others(self):
        item = make_item("a", "Whats new")
        del item["category"]
        groups = generator.group_news_by_category_type([item])
        self.assertEqual(len(groups["whats-new"]), 0)
        self.assertEqual(len(groups["others"]), 1)


class TestCategorizeNews(unittest.TestCase):
    def test_items_are_grouped_by_category(self):
        items = [
            make_item("a", "AWS blog"),
            make_item("b", "AWS Security Blog"),
            make_item("c", "AWS blog"),
        ]
        categories = generator.categorize_news(items)
        self.assertEqual(sorted(categories.keys()), ["AWS Security Blog", "AWS blog"])
        self.assertEqual(len(categories["AWS blog"]), 2)


class TestGenerateMarkdown(unittest.TestCase):
    def test_markdown_contains_header_title_and_detail(self):
        items = [
            make_item("Title A", "Whats new", detail="detail text"),
        ]
        markdown = generator.generate_markdown(items, "whats-new", days=7)
        self.assertIn("# AWS 週間アップデート情報 - What's New", markdown)
        self.assertIn("## Whats new", markdown)
        self.assertIn("[Title A](https://example.com/Title A)", markdown)
        self.assertIn("**公開日:** 2024-01-01", markdown)
        self.assertIn("detail text", markdown)

    def test_invalid_pubtime_falls_back_to_raw_string(self):
        items = [make_item("Title A", "Whats new", pubtime="not-a-date")]
        markdown = generator.generate_markdown(items, "whats-new", days=7)
        self.assertIn("**公開日:** not-a-date", markdown)


class TestCategoryGroups(unittest.TestCase):
    def test_groups_define_required_keys(self):
        for group_info in generator.CATEGORY_GROUPS.values():
            for key in ("display_name", "description", "emoji", "file_suffix", "categories"):
                self.assertIn(key, group_info)


if __name__ == "__main__":
    unittest.main()
