# Changelog

## Non publié

- `--gateway-processes` : défaut automatique `min(concurrence, 4)` (1 avec `--warm-ipc`).
- Supervision des sidecars : un sidecar mort est relancé sur la même socket ; les workers se reconnectent seuls.
- Option `--prefetch` / `SOLID_JOBS_PREFETCH` : deuxième slot de réservation indépendant par worker (au plus un job
  supplémentaire détenu, fencing et récupération inchangés). Désactivé par défaut.
- Sondage adaptatif des files vides (5 ms doublés jusqu'à 250 ms, avec jitter) à la place d'un sommeil fixe de 250 ms.
- Prefetch : la réservation anticipée est écrite avant l'exécution du job (un tour de boucle). Auparavant, un job
  synchrone la retardait jusqu'à sa fin (25 % de successeurs déjà réservés, ~100 % désormais).
- Messages `work`/`done` regroupés vers le thread principal.

## 0.1.0

- Premier portage Node.js de solid-jobs : `worker_threads` (un thread V8 par worker), API `Task` async/await,
  CLI `solid-jobs-node`.
- Redis passe exclusivement par le sidecar Ruby `solid-redis` (socket Unix 0600, répertoire 0700, JSON par ligne).
- Mêmes scripts Lua et mêmes clés Redis que les ports Ruby et Python : réservation atomique avec fencing token,
  ACK différé fusionné avec la réservation suivante, récupération des claims de nœuds morts, heartbeat, timer.
- Option `--gateway-processes N` : N sidecars Ruby indépendants, un worker par sidecar (`worker % N`).
- Option `--warm-ipc` : connexion unique multiplexée par le thread principal (`lib/warm_ipc.js`).
- Un thread worker qui plante est relancé avec le même identifiant et reprend son claim (at-least-once).
