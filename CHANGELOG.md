# Changelog

## 0.1.0

- Premier portage Node.js de solid-jobs : `worker_threads` (un thread V8 par worker), API `Task` async/await,
  CLI `solid-jobs-node`.
- Redis passe exclusivement par le sidecar Ruby `solid-redis` (socket Unix 0600, répertoire 0700, JSON par ligne).
- Mêmes scripts Lua et mêmes clés Redis que les ports Ruby et Python : réservation atomique avec fencing token,
  ACK différé fusionné avec la réservation suivante, récupération des claims de nœuds morts, heartbeat, timer.
- Option `--warm-ipc` : connexion unique multiplexée par le thread principal (`lib/warm_ipc.js`).
- Un thread worker qui plante est relancé avec le même identifiant et reprend son claim (at-least-once).
