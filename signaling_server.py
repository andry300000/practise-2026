#!/usr/bin/env python3
"""Minimal WebSocket signaling server for P2P chat (SDP/ICE relay)."""

import argparse
import asyncio
import json
import logging
from typing import Dict

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# room_id -> list of connected websockets (max 2)
rooms: Dict[str, list] = {}
# websocket -> room_id
client_rooms: Dict[object, str] = {}


def get_peer(ws: object, room: str):
    peers = rooms.get(room, [])
    for peer in peers:
        if peer is not ws:
            return peer
    return None


async def handle_client(ws) -> None:
    room: str | None = None

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send(json.dumps({"type": "error", "message": "Invalid JSON"}))
                continue

            msg_type = msg.get("type")

            if msg_type == "join":
                room = msg.get("room", "").strip()
                if not room:
                    await ws.send(json.dumps({"type": "error", "message": "Room name required"}))
                    continue

                if room not in rooms:
                    rooms[room] = []

                if len(rooms[room]) >= 2:
                    await ws.send(json.dumps({"type": "error", "message": "Room is full"}))
                    continue

                rooms[room].append(ws)
                client_rooms[ws] = room
                role = "offerer" if len(rooms[room]) == 1 else "answerer"
                await ws.send(json.dumps({"type": "joined", "room": room, "role": role}))
                logger.info("Client joined room '%s' as %s", room, role)

                if role == "answerer":
                    peer = get_peer(ws, room)
                    if peer:
                        await peer.send(json.dumps({"type": "peer_joined"}))

            elif msg_type == "signal":
                if ws not in client_rooms:
                    await ws.send(json.dumps({"type": "error", "message": "Join a room first"}))
                    continue

                room = client_rooms[ws]
                peer = get_peer(ws, room)
                if peer:
                    await peer.send(json.dumps({"type": "signal", "payload": msg.get("payload")}))

            else:
                await ws.send(json.dumps({"type": "error", "message": f"Unknown type: {msg_type}"}))

    finally:
        if ws in client_rooms:
            room = client_rooms.pop(ws)
            if room in rooms:
                rooms[room] = [p for p in rooms[room] if p is not ws]
                if not rooms[room]:
                    del rooms[room]
                else:
                    peer = rooms[room][0]
                    try:
                        await peer.send(json.dumps({"type": "peer_left"}))
                    except Exception:
                        pass
            logger.info("Client left room '%s'", room)


async def main(host: str, port: int) -> None:
    async with websockets.serve(handle_client, host, port):
        logger.info("Signaling server listening on ws://%s:%d", host, port)
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="P2P chat signaling server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8765, help="Bind port (default: 8765)")
    args = parser.parse_args()
    asyncio.run(main(args.host, args.port))
