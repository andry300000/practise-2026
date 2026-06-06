# P2P чат (Python CLI)

Простой P2P-чат через WebRTC Data Channel. Сообщения после установки соединения идут напрямую между участниками.

Два режима:

- **С сервером сигнализации** — автоматический обмен SDP/ICE через WebSocket
- **Ручной** — обмен двумя строками через мессенджер (сервер не нужен)

## Требования

- Python **3.10+** (на Windows нужны готовые wheels для PyAV)
- Зависимости из `requirements.txt`

## Установка

```bash
pip install -r requirements.txt
```

## Режим 1: с сервером сигнализации

### Локально (два терминала на одном ПК)

**Терминал 1 — сервер:**

```bash
python signaling_server.py
```

По умолчанию слушает `ws://0.0.0.0:8765`.

**Терминалы 2 и 3 — клиенты (одна комната):**

```bash
python chat_client.py --room test --signaling ws://localhost:8765
```

Первый клиент ждёт второго. После сообщения `P2P connection established` можно писать сообщения.

### Через интернет (два разных устройства)

1. Запустите сервер на VPS или домашнем ПК с публичным IP:

```bash
python signaling_server.py --host 0.0.0.0 --port 8765
```

2. Откройте порт **8765/TCP** в фаерволе (и проброс на роутере, если сервер дома).

3. На обоих устройствах:

```bash
python chat_client.py --room secret123 --signaling ws://PUBLIC_IP:8765
```

Замените `PUBLIC_IP` на внешний IP сервера. Имя комнаты (`--room`) должно совпадать.

## Режим 2: ручной обмен (без сервера)

**Устройство 1 — инициатор (host):**

```bash
python chat_client.py --manual --role host
```

Скопируйте строку `P2PCHAT1:...` и отправьте второму участнику (Telegram, email и т.д.).

**Устройство 2 — guest:**

```bash
python chat_client.py --manual --role guest
```

1. Вставьте строку от host.
2. Скопируйте свою строку `P2PCHAT1:...` и отправьте обратно host.

После обмена **двумя** строками установится P2P-соединение.

## Управление в чате

| Команда     | Действие        |
|-------------|-----------------|
| Текст       | Отправить сообщение |
| `/quit`     | Выйти           |
| `Ctrl+C`    | Выйти           |

## Параметры

### `signaling_server.py`

```bash
python signaling_server.py --host 0.0.0.0 --port 8765
```

### `chat_client.py`

```bash
# С сервером
python chat_client.py --room ROOM --signaling ws://HOST:PORT

# Ручной режим
python chat_client.py --manual --role host
python chat_client.py --manual --role guest
```

## Ограничения

- Без TURN-сервера часть пар за жёстким NAT может не соединиться — используйте режим с сервером или ручной обмен.
- В комнате с сервером — не более **2** участников.
- STUN: `stun.l.google.com:19302` (встроен в клиент).

## Структура проекта

| Файл                 | Описание                          |
|----------------------|-----------------------------------|
| `signaling_server.py`| WebSocket relay для SDP/ICE       |
| `chat_client.py`     | CLI-клиент                        |
| `requirements.txt`   | Зависимости                       |
