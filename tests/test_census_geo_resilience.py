"""The Census API fails; asking it for less, and asking again, must not fail the run.

The 2020 block-population request asks for every block in a state at once --
around 520,000 rows for California -- and the API returns 500 for it often
enough that it cannot be the only route. It killed a run that had already read
the survey, linked trips, built tours and weighted them.

Two responses. A statewide failure falls through to the county-by-county walk
that the 2010 vintage requires anyway, which asks for far less at a time. And a
county request retries a server-side failure, since a run makes 58 of them and
one 500 should not end it.

Retries are for server faults only. A 4xx is our request being wrong and will
fail identically however many times it is sent.
"""

import pytest
import requests

from processing.weighting.data_prep import census_geo


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"{status} simulated", response=response)


class TestOnlyServerFaultsAreRetried:
    """Retrying a request the server has correctly rejected just wastes time."""

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    def test_server_errors_are_transient(self, status):
        """5xx means the server faltered; the same request may yet succeed."""
        assert census_geo._is_transient(_http_error(status))

    @pytest.mark.parametrize("status", [400, 401, 403, 404])
    def test_client_errors_are_not(self, status):
        """These mean the request is wrong, and it will stay wrong."""
        assert not census_geo._is_transient(_http_error(status))

    def test_connection_and_timeout_failures_are_transient(self):
        """The request never got a verdict, so it is worth asking again."""
        assert census_geo._is_transient(requests.ConnectionError("dropped"))
        assert census_geo._is_transient(requests.Timeout("slow"))

    def test_an_unrelated_error_is_not_retried(self):
        """A bug in our own parsing must surface, not be retried four times."""
        assert not census_geo._is_transient(ValueError("bad payload"))


class TestACountyRequestRetries:
    """One 500 among 58 counties must not end the run."""

    def _serve(self, monkeypatch, outcomes):
        """Each call raises or returns the next outcome; sleeping is skipped."""
        served = iter(outcomes)
        calls = {"n": 0}

        def fake_get(*_args, **_kwargs):
            calls["n"] += 1
            outcome = next(served)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(census_geo.requests, "get", fake_get)
        monkeypatch.setattr(census_geo, "_census_json", lambda resp: resp)
        monkeypatch.setattr(census_geo, "_parse_block_response", lambda payload, _var: payload)
        monkeypatch.setattr(census_geo.time, "sleep", lambda _s: None)
        return calls

    def test_it_recovers_after_a_server_error(self, monkeypatch):
        """The case that ended a real run at step 8 of 12."""
        calls = self._serve(monkeypatch, [_http_error(500), {"060010001": 42}])

        result = census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert result == {"060010001": 42}
        assert calls["n"] == 2

    def test_a_sound_response_is_not_retried(self, monkeypatch):
        """Retrying success would double 58 requests for nothing."""
        calls = self._serve(monkeypatch, [{"060010001": 42}])

        census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert calls["n"] == 1

    def test_it_gives_up_rather_than_looping(self, monkeypatch):
        """Persistent failure is systematic; looping only delays reporting it."""
        attempts = census_geo._MAX_CENSUS_ATTEMPTS
        calls = self._serve(monkeypatch, [_http_error(503)] * attempts)

        with pytest.raises(requests.HTTPError):
            census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert calls["n"] == attempts

    def test_a_client_error_fails_immediately(self, monkeypatch):
        """No point asking three more times for something we asked for wrongly."""
        calls = self._serve(monkeypatch, [_http_error(404)])

        with pytest.raises(requests.HTTPError):
            census_geo._fetch_county_blocks("u", "P1_001N", "06", "001", "k")

        assert calls["n"] == 1
