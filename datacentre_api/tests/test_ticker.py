"""Regression tests for modules/ticker.py.

These tests cover the stream cursor leak fixed in PR #38: previously,
``get_share_trades`` did ``result = await session.stream(query)`` followed
by an early ``break`` out of the iteration loop, which left the underlying
streaming cursor + connection orphaned. The fix wraps the iteration in a
``try/finally`` with explicit ``await result.close()``.

The key invariant under test: **the result object's ``close()`` MUST be
awaited before the function returns**, on every exit path (early break,
normal completion, exception).
"""
import os

# modules/base.py builds SQLAlchemy engines at import time using these env
# vars. Provide bogus-but-parseable values so importing modules.ticker
# doesn't blow up under pytest. The tests below mock the session entirely;
# the engines are never actually used.
os.environ.setdefault("MYSQL_HOST", "localhost")
os.environ.setdefault("MYSQL_PORT", "3306")
os.environ.setdefault("MYSQL_USER", "test")
os.environ.setdefault("MYSQL_PASSWORD", "test")
os.environ.setdefault("MYSQL_DB", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_HOST", "localhost")
os.environ.setdefault("MYSQL_ARCHIVAL_PORT", "3306")
os.environ.setdefault("MYSQL_ARCHIVAL_USER", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_PASSWORD", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_DB", "test")
os.environ.setdefault("SQLITE_DB_PATH", "/tmp/test.sqlite")
os.environ.setdefault("PLAYERHISTORY_SQLITE_DB_PATH", "/tmp/test_ph.sqlite")

from unittest.mock import AsyncMock, MagicMock  # noqa: E402

import pytest  # noqa: E402

from modules.ticker import get_share_trades  # noqa: E402


class FakeRow:
    """Stand-in for a SQLAlchemy Row attribute-accessed in get_share_trades."""

    def __init__(self, share_type: str, share_id: int, price: int,
                 market_buy: bool, date: int) -> None:
        self.share_type = share_type
        self.share_id = share_id
        self.price = price
        self.market_buy = market_buy
        self.date = date


class FakeAsyncResult:
    """Minimal stand-in for sqlalchemy AsyncResult.

    - async-iterable (the only thing get_share_trades requires)
    - tracks whether close() was awaited
    - mirrors the AsyncResult API quirk that close() is async but the
      object itself is NOT an async context manager
    """

    def __init__(self, rows):
        self._iter = iter(rows)
        self.close_called = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    async def close(self):
        self.close_called = True


def _mock_session_returning(rows):
    """Build a mock AsyncSession whose .stream() returns a FakeAsyncResult."""
    fake_result = FakeAsyncResult(rows)
    session = MagicMock()
    # AsyncMock makes `await session.stream(query)` resolve to fake_result.
    session.stream = AsyncMock(return_value=fake_result)
    return session, fake_result


@pytest.mark.asyncio
async def test_stream_closed_on_early_break():
    """Regression for the leak: when num is reached partway through the
    stream, the result must still be closed before returning."""
    rows = [
        FakeRow("club", 1, 100, True, 1000),
        FakeRow("club", 2, 200, False, 1100),
        FakeRow("club", 3, 300, True, 1200),
        FakeRow("club", 4, 400, False, 1300),  # never reached when num=3
        FakeRow("club", 5, 500, True, 1400),
    ]
    session, fake_result = _mock_session_returning(rows)

    trades = await get_share_trades(session, share_type="club", num=3)

    assert len(trades) == 3, "should stop after collecting num distinct shares"
    assert fake_result.close_called, (
        "stream cursor was not closed on early break — this is the leak "
        "that caused the red-datacentre-api OOM-restart sawtooth on 2026-04-08"
    )


@pytest.mark.asyncio
async def test_stream_closed_on_normal_completion():
    """When the stream is exhausted naturally (fewer rows than num), the
    result must still be closed."""
    rows = [
        FakeRow("player", 10, 100, True, 1000),
        FakeRow("player", 11, 200, False, 1100),
    ]
    session, fake_result = _mock_session_returning(rows)

    trades = await get_share_trades(session, share_type="player", num=10)

    assert len(trades) == 2, "should return all available rows when num is not reached"
    assert fake_result.close_called, "stream cursor was not closed on normal completion"


@pytest.mark.asyncio
async def test_stream_closed_when_inner_loop_raises():
    """If something inside the iteration raises (e.g. a malformed row),
    the result must still be closed via the finally block."""

    class ExplodingRow:
        share_type = "club"
        share_id = 1
        price = 100
        # market_buy missing on purpose — accessing it raises AttributeError

        @property
        def market_buy(self):
            raise RuntimeError("synthetic explosion")

        @property
        def date(self):
            return 1000

    session, fake_result = _mock_session_returning([ExplodingRow()])

    with pytest.raises(RuntimeError, match="synthetic explosion"):
        await get_share_trades(session, share_type="club", num=10)

    assert fake_result.close_called, (
        "stream cursor was not closed when inner loop raised — finally block "
        "is not in place"
    )


@pytest.mark.asyncio
async def test_dedupes_by_share_id():
    """The function deduplicates by share_id (most recent trade wins because
    the underlying query orders by id desc). Verify the dedup logic and
    that close() still happens after dedup early-breaks."""
    rows = [
        FakeRow("club", 1, 100, True, 1000),
        FakeRow("club", 1, 90, False, 990),   # duplicate share_id — skipped
        FakeRow("club", 2, 200, True, 1100),
        FakeRow("club", 1, 80, True, 980),    # another dup of #1 — skipped
        FakeRow("club", 3, 300, False, 1200),
    ]
    session, fake_result = _mock_session_returning(rows)

    trades = await get_share_trades(session, share_type="club", num=10)

    share_ids = [t.share.id for t in trades]
    assert share_ids == [1, 2, 3], "should keep first occurrence of each share_id"
    assert fake_result.close_called
