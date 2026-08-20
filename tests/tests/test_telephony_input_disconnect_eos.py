"""Regression: a normal telephony hangup must emit EOS and stop the receive loop.

Before the fix, `_listen()` caught WebSocketDisconnect, logged it, and looped back
to `websocket.receive_text()` on an already-closed socket. Starlette raises a bare
RuntimeError for that second call, which the loop's generic `except Exception` then
reported as a traceback on every normal hangup - and the transcriber queue never saw
an EOS packet for the WebSocketDisconnect path, so downstream teardown had nothing
telling it the call had ended.
"""

import asyncio

import pytest
from starlette.websockets import WebSocketDisconnect

from bolna.input_handlers.telephony import TelephonyInputHandler


class _DisconnectingWebSocket:
    """Raises WebSocketDisconnect on the first receive, then RuntimeError like a real
    closed starlette socket would if receive_text() were called again."""

    def __init__(self, code):
        self.code = code
        self.calls = 0

    async def receive_text(self):
        self.calls += 1
        if self.calls == 1:
            raise WebSocketDisconnect(code=self.code)
        raise RuntimeError("Cannot call 'receive' once a disconnect message has been received.")


def _make_handler(websocket):
    handler = TelephonyInputHandler.__new__(TelephonyInputHandler)
    handler.websocket = websocket
    handler.io_provider = "twilio"
    handler.call_sid = "CA123"
    handler._stream_sid = "ST123"
    handler.queues = {"transcriber": asyncio.Queue()}
    return handler


@pytest.mark.asyncio
async def test_normal_disconnect_emits_eos_and_stops_listening():
    ws = _DisconnectingWebSocket(code=1000)
    handler = _make_handler(ws)

    await handler._listen()

    assert ws.calls == 1  # loop broke instead of calling receive_text() again
    packet = handler.queues["transcriber"].get_nowait()
    assert packet["meta_info"]["eos"] is True


@pytest.mark.asyncio
async def test_abnormal_disconnect_also_emits_eos_and_stops_listening():
    ws = _DisconnectingWebSocket(code=1006)
    handler = _make_handler(ws)

    await handler._listen()

    assert ws.calls == 1
    packet = handler.queues["transcriber"].get_nowait()
    assert packet["meta_info"]["eos"] is True
