#!/usr/bin/env python3
"""CLI P2P chat client using WebRTC Data Channel."""

import argparse
import asyncio
import json
import logging
import sys

import websockets
from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription
from aiortc.sdp import candidate_from_sdp, candidate_to_sdp

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

STUN_SERVER = "stun:stun.l.google.com:19302"


class ChatClient:
    def __init__(self, room: str, signaling_url: str) -> None:
        self.room = room
        self.signaling_url = signaling_url
        self.pc: RTCPeerConnection | None = None
        self.channel = None
        self.ws = None
        self.role: str | None = None
        self.channel_ready = asyncio.Event()
        self.peer_joined = asyncio.Event()
        self._pending_candidates: list = []

    def _rtc_config(self) -> RTCConfiguration:
        return RTCConfiguration(iceServers=[RTCIceServer(urls=[STUN_SERVER])])

    async def connect_signaling(self) -> None:
        self.ws = await websockets.connect(self.signaling_url)
        await self.ws.send(json.dumps({"type": "join", "room": self.room}))

        response = json.loads(await self.ws.recv())
        if response.get("type") == "error":
            raise RuntimeError(response.get("message", "Failed to join room"))

        self.role = response["role"]
        print(f"Joined room '{self.room}' as {self.role}")

        if self.role == "offerer":
            print("Waiting for peer to join...")

    async def listen_signaling(self) -> None:
        assert self.ws is not None
        async for raw in self.ws:
            msg = json.loads(raw)
            msg_type = msg.get("type")

            if msg_type == "peer_joined":
                self.peer_joined.set()
            elif msg_type == "peer_left":
                print("\n[system] Peer disconnected")
                return
            elif msg_type == "signal":
                await self._handle_signal(msg["payload"])

    async def _handle_signal(self, payload: dict) -> None:
        assert self.pc is not None
        sig_type = payload.get("type")

        if sig_type == "offer":
            await self.pc.setRemoteDescription(
                RTCSessionDescription(sdp=payload["sdp"], type=payload["type"])
            )
            answer = await self.pc.createAnswer()
            await self.pc.setLocalDescription(answer)
            await self._send_signal(
                {"type": self.pc.localDescription.type, "sdp": self.pc.localDescription.sdp}
            )
            await self._flush_pending_candidates()

        elif sig_type == "answer":
            await self.pc.setRemoteDescription(
                RTCSessionDescription(sdp=payload["sdp"], type=payload["type"])
            )
            await self._flush_pending_candidates()

        elif sig_type == "candidate":
            if not payload.get("candidate"):
                await self.pc.addIceCandidate(None)
                return

            raw = payload["candidate"]
            if raw.startswith("candidate:"):
                raw = raw.split(":", 1)[1]
            candidate = candidate_from_sdp(raw)
            candidate.sdpMid = payload.get("sdpMid")
            candidate.sdpMLineIndex = payload.get("sdpMLineIndex")

            if self.pc.remoteDescription is None:
                self._pending_candidates.append(candidate)
            else:
                await self.pc.addIceCandidate(candidate)

    async def _flush_pending_candidates(self) -> None:
        assert self.pc is not None
        for candidate in self._pending_candidates:
            await self.pc.addIceCandidate(candidate)
        self._pending_candidates.clear()

    async def _send_signal(self, payload: dict) -> None:
        assert self.ws is not None
        await self.ws.send(json.dumps({"type": "signal", "payload": payload}))

    def _setup_channel(self, channel) -> None:
        self.channel = channel

        @channel.on("open")
        def on_open():
            print("[system] P2P connection established. Type messages ( /quit to exit )")
            self.channel_ready.set()

        @channel.on("message")
        def on_message(message):
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            print(f"\n[peer] {message}")
            print("> ", end="", flush=True)

    async def _init_peer_connection(self) -> None:
        self.pc = RTCPeerConnection(configuration=self._rtc_config())

        @self.pc.on("datachannel")
        def on_datachannel(channel):
            self._setup_channel(channel)

        @self.pc.on("icecandidate")
        async def on_icecandidate(candidate):
            if candidate:
                await self._send_signal(
                    {
                        "type": "candidate",
                        "candidate": "candidate:" + candidate_to_sdp(candidate),
                        "sdpMid": candidate.sdpMid,
                        "sdpMLineIndex": candidate.sdpMLineIndex,
                    }
                )
            else:
                await self._send_signal({"type": "candidate", "candidate": None})

    async def _start_offer(self) -> None:
        assert self.pc is not None
        await self.peer_joined.wait()

        channel = self.pc.createDataChannel("chat")
        self._setup_channel(channel)

        offer = await self.pc.createOffer()
        await self.pc.setLocalDescription(offer)
        await self._send_signal(
            {"type": self.pc.localDescription.type, "sdp": self.pc.localDescription.sdp}
        )

    async def chat_loop(self) -> None:
        await self.channel_ready.wait()
        loop = asyncio.get_event_loop()

        while True:
            try:
                line = await loop.run_in_executor(None, lambda: input("> "))
            except EOFError:
                break

            line = line.strip()
            if not line:
                continue
            if line == "/quit":
                break

            if self.channel and self.channel.readyState == "open":
                self.channel.send(line)
            else:
                print("[system] Channel not ready")

    async def run(self) -> None:
        await self.connect_signaling()
        await self._init_peer_connection()

        signaling_task = asyncio.create_task(self.listen_signaling())

        if self.role == "offerer":
            await self._start_offer()

        try:
            await self.chat_loop()
        finally:
            signaling_task.cancel()
            if self.pc:
                await self.pc.close()
            if self.ws:
                await self.ws.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="P2P chat client")
    parser.add_argument("--room", required=True, help="Room name to join")
    parser.add_argument(
        "--signaling",
        required=True,
        help="Signaling server URL (e.g. ws://localhost:8765)",
    )
    args = parser.parse_args()

    try:
        client = ChatClient(args.room, args.signaling)
        await client.run()
    except KeyboardInterrupt:
        print("\n[system] Goodbye")
    except ImportError as e:
        print(
            "Failed to import aiortc. Install dependencies:\n"
            "  pip install -r requirements.txt\n"
            "On Windows use Python 3.10+ for prebuilt PyAV wheels.",
            file=sys.stderr,
        )
        raise SystemExit(1) from e
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        raise SystemExit(1) from e


if __name__ == "__main__":
    asyncio.run(main())
