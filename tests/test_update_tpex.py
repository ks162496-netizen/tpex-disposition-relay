"""Offline download regression tests; never call official data endpoints."""
import http.client
import unittest
from unittest.mock import Mock, patch

from scripts import update_tpex


BODY = b'[{"code":"1234"}]'
URL = "https://example.invalid/synthetic-json"


def response(body=BODY, content_length=None):
    result = Mock()
    result.__enter__ = Mock(return_value=result)
    result.__exit__ = Mock(return_value=False)
    result.read.return_value = body
    result.geturl.return_value = URL
    result.headers = {"Content-Type": "application/json"}
    if content_length is not None:
        result.headers["Content-Length"] = content_length
    return result


class DownloadJsonDatasetTests(unittest.TestCase):
    def download(self):
        return update_tpex.download_json_dataset(URL, "synthetic JSON", minimum_bytes=1)

    @patch.object(update_tpex.time, "sleep")
    @patch.object(update_tpex.urllib.request, "urlopen")
    def test_incomplete_read_restarts_full_response_then_succeeds(self, open_url, sleep):
        partial = response()
        partial.read.side_effect = http.client.IncompleteRead(b"partial", 20)
        complete = response(content_length=str(len(BODY)))
        open_url.side_effect = [partial, complete]
        raw, metadata, final_url = self.download()
        self.assertEqual(raw, BODY)
        self.assertEqual(metadata["content_type"], "application/json")
        self.assertEqual(final_url, URL)
        self.assertEqual(open_url.call_count, 2)
        self.assertEqual(partial.read.call_count, 1)
        self.assertEqual(complete.read.call_count, 1)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(3)])

    @patch.object(update_tpex.time, "sleep")
    @patch.object(update_tpex.urllib.request, "urlopen")
    def test_incomplete_read_stops_after_original_three_attempts(self, open_url, sleep):
        open_url.side_effect = [http.client.IncompleteRead(b"x", 1) for _ in range(3)]
        with self.assertRaisesRegex(RuntimeError, "Unable to download synthetic JSON"):
            self.download()
        self.assertEqual(open_url.call_count, 3)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(3), unittest.mock.call(6)])

    @patch.object(update_tpex.time, "sleep")
    @patch.object(update_tpex.urllib.request, "urlopen")
    def test_short_content_length_retries_without_partial_append(self, open_url, sleep):
        open_url.side_effect = [response(b"[", str(len(BODY))), response(content_length=str(len(BODY)))]
        self.assertEqual(self.download()[0], BODY)
        self.assertEqual(open_url.call_count, 2)
        sleep.assert_called_once_with(3)

    @patch.object(update_tpex.time, "sleep")
    @patch.object(update_tpex.urllib.request, "urlopen")
    def test_persistent_length_mismatch_is_bounded(self, open_url, sleep):
        open_url.side_effect = [response(content_length=str(len(BODY) + 1)) for _ in range(3)]
        with self.assertRaisesRegex(RuntimeError, "does not match Content-Length"):
            self.download()
        self.assertEqual(open_url.call_count, 3)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(3), unittest.mock.call(6)])

    @patch.object(update_tpex.time, "sleep")
    @patch.object(update_tpex.urllib.request, "urlopen")
    def test_absent_content_length_preserves_existing_acceptance(self, open_url, sleep):
        open_url.return_value = response()
        self.assertEqual(self.download()[0], BODY)
        open_url.assert_called_once()
        sleep.assert_not_called()

    @patch.object(update_tpex.time, "sleep")
    @patch.object(update_tpex.urllib.request, "urlopen")
    def test_invalid_or_excess_body_lengths_are_not_accepted(self, open_url, sleep):
        open_url.side_effect = [response(content_length="invalid"), response(content_length="0"), response(content_length=str(len(BODY)))]
        self.assertEqual(self.download()[0], BODY)
        self.assertEqual(open_url.call_count, 3)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(3), unittest.mock.call(6)])


if __name__ == "__main__":
    unittest.main()
