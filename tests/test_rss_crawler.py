import datetime
import unittest

from tests.util import load_lambda_module

crawler = load_lambda_module(
    "rss_crawler_index",
    "rss-crawler",
    env={"DDB_TABLE_NAME": "AWSUpdatesRSSHistory"},
)


class TestStr2Datetime(unittest.TestCase):
    def test_naive_string_is_returned_as_is(self):
        result = crawler.str2datetime("2024-01-01T12:00:00")
        self.assertEqual(result, datetime.datetime(2024, 1, 1, 12, 0, 0))

    def test_timezone_aware_string_is_normalized_to_utc(self):
        result = crawler.str2datetime("Tue, 20 Sep 2022 16:05:47 +0900")
        self.assertEqual(result, datetime.datetime(2022, 9, 20, 7, 5, 47))
        self.assertIsNone(result.tzinfo)

    def test_utc_string_keeps_time(self):
        result = crawler.str2datetime("Tue, 20 Sep 2022 16:05:47 +0000")
        self.assertEqual(result, datetime.datetime(2022, 9, 20, 16, 5, 47))


class TestRecentlyPublished(unittest.TestCase):
    def test_recent_date_is_recent(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        self.assertTrue(crawler.recently_published(now.isoformat()))

    def test_old_date_is_not_recent(self):
        old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=30
        )
        self.assertFalse(crawler.recently_published(old.isoformat()))

    def test_date_within_recent_days_is_recent(self):
        within = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=crawler.RECENT_DAYS - 1
        )
        self.assertTrue(crawler.recently_published(within.isoformat()))


if __name__ == "__main__":
    unittest.main()
