# WEB bridge harness

A single static page that plays the role of a Telegram client against a
deployed mtg WEB proxy. It embeds the bridge page of your site in an
iframe, hands it a `MessageChannel` port, sends a relay handshake and logs
everything that comes back. Use it to check a deployment without Telegram.

The bridge accepts a parent only from `http://127.0.0.1:<port>`, so serve
the page locally:

```console
$ cd contrib/web-bridge-harness
$ python3 -m http.server 8099 --bind 127.0.0.1
```

## Browser steps

1. Open `http://127.0.0.1:8099/` in a browser.
2. Enter the hostname (`web.hostname`) and the secret (`web.secret` from
   `mtg access`, the `dd...` hex string).
3. Press Connect.

The log should show, in order:

```
status: connecting
status: connected
frames: WELCOME stream=0 len=...
frames: CLOSE stream=1 len=...
```

`CLOSE stream=1` is expected: the harness sends `OPEN 1` and a `DATA 1`
with 64 zero bytes, which is not a valid obfuscated2 handshake, so the
relay closes the stream. Other lines (`traffic`, `PING`) may appear.

## Telegram Desktop

1. Add a proxy of type WEB with the hostname and the `dd...` secret from
   `mtg access`. The client should connect.
2. Stop the reverse proxy. The client should show that it is reconnecting.
3. Start it again. The client should recover within the reconnect grace
   (`web.timeouts.reconnect-grace`, 2 minutes by default).
