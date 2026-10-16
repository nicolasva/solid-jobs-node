# solid-jobs-node

[![Build Status](https://github.com/nicolasva/solid-jobs-node/actions/workflows/ci.yml/badge.svg)](https://github.com/nicolasva/solid-jobs-node/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/solid-jobs-node.svg)](https://www.npmjs.com/package/solid-jobs-node)
[![Downloads](https://img.shields.io/npm/dm/solid-jobs-node.svg)](https://www.npmjs.com/package/solid-jobs-node)
[![License: LGPL v3+](https://img.shields.io/npm/l/solid-jobs-node.svg)](LICENSE.txt)
[![Documentation](https://img.shields.io/badge/docs-README-blue.svg)](https://github.com/nicolasva/solid-jobs-node#readme)

Port Node.js de [solid-jobs](../solid-jobs) (Ruby) et [solid-jobs-py](../solid-jobs-py) (Python) : jobs fiables sur Redis,
exécutés dans des **`worker_threads`** (un isolat V8 par worker, la boucle d'événements principale n'est jamais bloquée).

**Redis n'est pas recodé en JavaScript.** Tout l'accès Redis passe par un processus compagnon Ruby
(*sidecar*) qui embarque la gemme [`solid-redis`](https://github.com/nicolasva/solid-redis). Zéro dépendance npm.

```
[ Processus Node.js ]
   |-- Worker Thread 1 (isolat V8) --\
   |-- Worker Thread 2 (isolat V8) --+--> Socket Unix (0600) --> Sidecar Ruby (solid-redis) --> Redis
   |-- Worker Thread N (isolat V8) --/        JSON lines
```

## Prérequis

- Node.js >= 20
- Ruby + `gem install solid-redis` (>= 1.0.13) ; `bundle install` fonctionne aussi via le `Gemfile`
- Un serveur Redis

## Installation

```sh
npm install solid-jobs-node     # embarque lib/solid_redis_gateway.rb
```

## Utilisation

```js
// myapp/jobs.js  (chargé dans le processus principal ET dans chaque worker thread)
const { Task } = require('solid-jobs-node');

class RecalculateAccount extends Task {
  static channel = 'critical';
  static maxFailures = 10;

  async executeTask(accountId) {
    // logique métier intensive ou asynchrone
  }

  static async retryDelay(failureCount, error, envelope) {
    return 5.0; // secondes, 'drop', 'archive' ou null (backoff par défaut)
  }
}

module.exports = { RecalculateAccount };
```

```js
const { RecalculateAccount } = require('./myapp/jobs');

await RecalculateAccount.enqueue(42);
await RecalculateAccount.enqueueMany([[1], [2], [3]]);
await RecalculateAccount.enqueueAfter(30, 42);        // 30 secondes de délai
await RecalculateAccount.scheduleAt(dateOrTimestamp, 42);
await RecalculateAccount.withOptions({ channel: 'low' }).enqueue(42);
```

Les tâches sont retrouvées par `static taskName` (défaut : nom de la classe) parmi les exports des fichiers `--require`.
Les arguments doivent être JSON-natifs (`onComplexArguments` : `'raise'` par défaut, `'warn'` ou `'ignore'`).

Configuration programmatique :

```js
const { configure } = require('solid-jobs-node');
configure({ redisUrl: 'redis://127.0.0.1:6379/0', concurrency: 4, channels: ['critical,3', 'default'] });
```

Exécuter les workers :

```sh
REDIS_URL=redis://127.0.0.1:6379/0 solid-jobs-node -r ./myapp/jobs.js -c 4 --channel critical,3 --channel default -t 25
```

Options : `-r/--require` (répétable), `-C/--config` (JSON ou `.js`/`.cjs`), `-c/--concurrency`, `--channel nom[,poids]`,
`-t/--timeout`, `--redis-url`, `--gateway-processes`, `--prefetch`, `--warm-ipc`. Signaux : `SIGTERM`/`SIGINT` arrêt gracieux (les jobs en cours sont terminés),
`SIGTSTP` pause des réservations (quiet), `SIGTTIN` statut.

Variables d'environnement : `REDIS_URL`, `SOLID_JOBS_RUBY` (commande ruby, ex. `bundle exec ruby`),
`SOLID_JOBS_GATEWAY_SOCKET` (utiliser un sidecar déjà lancé), `SOLID_JOBS_GATEWAY_PROCESSES`, `SOLID_JOBS_PREFETCH` (`1`/`true`), `SOLID_JOBS_LOG_LEVEL`.

Tests : `await Task.execute(...)` exécute en ligne ; `await Lab.capture(async () => { ... })` capture au lieu d'écrire
dans Redis (`Task.captured()`, `Task.executeNext()`).

## Concurrence

Le processus principal (Conductor, heartbeat, scheduler, récupération) pilote N `worker_threads`. Chaque thread a sa propre
connexion au sidecar (donc son propre `SolidRedis::Client`) et exécute une boucle claim → exécution → ack. Les workers ne
partagent aucune mémoire avec le thread principal (*shared-nothing*) ; heartbeat et statistiques remontent par `postMessage`.
Une tâche `async` n'occupe pas le worker pendant ses `await`, mais un job synchrone gourmand en CPU bloque son worker (pas les autres).

## Plusieurs sidecars (`--gateway-processes N`)

Un sidecar Ruby est limité par son GVL : sur des jobs très courts il sature un cœur (~6 000 jobs/s mesurés). Avec
`--gateway-processes N` (ou `SOLID_JOBS_GATEWAY_PROCESSES`, ou `config.gatewayProcesses`), N sidecars indépendants
démarrent chacun sur sa propre socket 0600 et le worker `i` utilise le sidecar `i % N` : la charge est répartie de façon
déterministe, sans changer le protocole ni le fencing. Défaut : `min(concurrence, 4)` (1 avec `--warm-ipc`). Coût : environ +30 à 40 MiB de RAM par sidecar. Sans
effet avec `--warm-ipc` (une seule connexion). Utile surtout pour des jobs courts ; pour des jobs CPU le gain est faible.

Les sidecars sont supervisés : le thread principal vérifie chaque seconde qu'ils tournent et relance un sidecar mort **sur la
même socket**. Les workers qui y étaient connectés se reconnectent seuls et reprennent leur claim (testé : un sidecar tué
en cours de run, 300 jobs sur 300 terminés).

## Prefetch (`--prefetch`)

Désactivé par défaut. Avec `--prefetch` (ou `SOLID_JOBS_PREFETCH=1`, ou `config.prefetch`), chaque worker dispose de deux
slots de réservation indépendants : pendant que le job du slot A s'exécute, la réservation du slot B est déjà en vol. Chaque
slot a sa propre liste `claimed`, son propre claim et son propre fencing token ; les scripts Lua et la récupération après
crash sont inchangés. Au plus **un** job supplémentaire est détenu par worker, et seulement avec la réservation fiable.
Un job préchargé reste donc visible et récupérable par la reprise après crash. Un job synchrone bloquant le thread, la
réservation anticipée est écrite avant le démarrage du job (un tour de boucle d'événements) : sans cela, elle n'était
envoyée qu'à la fin du job et le prefetch restait sans effet sur les jobs CPU (25 % de jobs déjà réservés au lieu de ~100 %).

## Sondage des files vides

Quand aucune file n'a de job, un worker réessaie après 5 ms, puis double le délai (avec ±25 % de jitter) jusqu'à 250 ms ;
il revient à 5 ms dès qu'un job est trouvé. Après une longue inactivité, un premier job peut donc attendre jusqu'à 250 ms.
Au repos, 8 workers génèrent ~70 commandes Redis/s.

## Warm IPC (connexion unique multiplexée)

Option `--warm-ipc` (ou `configure({ warmIpc: true })`, ou `"warmIpc": true` dans le fichier de config) : le thread principal
ouvre **une seule connexion persistante** vers le sidecar et multiplexe dessus tout le trafic Redis des worker threads
(workers, heartbeat, scheduler). Les workers envoient leurs requêtes par `postMessage` (clonage structuré V8) avec un
identifiant de requête ; les réponses sont routées par cet identifiant, avec timeout de 30 s. Si la socket tombe, les
requêtes en attente sont rejetées (le claim est alors rejoué) et la connexion se rétablit au appel suivant.

```
Sidecar Ruby (solid-redis) <== socket 0600, JSON/ligne ==> Thread principal (multiplexeur)
                                                              |-- postMessage --> Worker 1
                                                              |-- postMessage --> Worker 2
```

Différences avec la spec proposée : le sidecar parle le protocole commande Redis (`command`/`pipeline`), pas des actions
métier `reserve`; la réservation reste donc atomique côté Redis (script Lua) et faite par chaque worker, avec ack
fenced. Le moteur de réservation n'est pas dupliqué dans le thread principal, ce qui conserve les garanties at-least-once.
Contrepartie : tout le trafic est sérialisé sur une seule connexion Redis (moins de connexions, un peu plus de latence en charge).

## Garanties (identiques aux versions Ruby et Python)

Même clavier de clés Redis (`solid_jobs:*`) et mêmes scripts Lua : réservation atomique avec jeton de claim
(*fencing token*) dans une liste *claimed* par worker, ack/requeue protégés par ce jeton, ack différé fusionné avec la
réservation suivante, récupération des claims de nœuds morts (pid disparu ou heartbeat > 60 s), retries avec backoff,
archive des échecs, tâches planifiées, barrière de publication, `IntegrityCheck`.
Livraison **at-least-once**, jamais exactly-once : rendez vos jobs idempotents.

**Résilience aux crashs (chaos proof)** : si un thread worker meurt (exception non interceptée, `process.exit` dans une
tâche), le Conductor le relance (backoff 100 ms → 5 s) avec le même identifiant ; le nouveau thread reprend le claim laissé
en suspens et le job est rejoué. Si un worker ne s'arrête pas dans `timeout + 5 s`, il est terminé (`worker.terminate()`)
et ses claims sont remis en file ; son ack tardif serait rejeté par le jeton. Si tout le processus meurt, les autres
nœuds (ou le redémarrage) récupèrent les claims dès que le pid a disparu ou que le heartbeat a expiré.
Limite connue : un job « poison » qui plante le thread à chaque exécution sera rejoué en boucle (avec backoff).

## Protocole du sidecar

Une requête JSON par ligne, une réponse par ligne :

```
{"command": "RPOP", "args": ["queue"]}      -> {"status": "OK", "data": ...}
{"pipeline": [["SET","a",1],["INCR","a"]]}  -> {"status": "OK", "data": [...]}
erreur                                      -> {"status": "ERROR", "error": {"class": "...", "message": "..."}}
```

Socket `0600` dans un répertoire temporaire `0700` ; le sidecar s'arrête quand le parent meurt (EOF sur stdin).

## Benchmark : BullMQ vs solid-jobs-node

Mesures sur une machine (macOS Apple Silicon, 12 CPU logiques, Node v26.7.0, Redis 8, BullMQ 6.3.15). **Tableau 1 (avant
correction du prefetch sur jobs synchrones)**, médianes de
5 répétitions de 20 000 jobs sur une file préremplie. Job CPU : boucle entière de 100 000 itérations. Les sidecars Ruby
sont inclus dans la RAM et le CPU de solid-jobs-node. `xN` = N sidecars, `pf` = `--prefetch`. Harnais : dépôt
`benchmark-solid-jobs-node-vs-bullmq`.

| Charge | c | BullMQ (jobs/s) | solid-jobs-node x4+pf | x8+pf | RAM BullMQ | RAM x4+pf / x8+pf |
|---|---:|---:|---:|---:|---:|---:|
| noop | 8 | 18 352 | 18 033 (0,98×) | **19 577 (1,07×)** | 886 MiB | 329 / 448 MiB |
| noop | 4 | 15 312 | 14 444 (0,94×) | 14 382 (0,94×) | 544 MiB | 258 / 367 MiB |
| cpu | 8 | 15 348 | 12 516 (0,82×) | 13 613 (0,89×) | 877 MiB | 336 / 455 MiB |
| cpu | 4 | 11 935 | 9 902 (0,83×) | 9 793 (0,82×) | 501 MiB | 263 / 368 MiB |

À retenir :

- **Devant** sur les jobs courts à concurrence 8 avec x8+pf (1,07×, sur 5 répétitions sur 5), avec environ 49 % de RAM en moins ;
  avec x4+pf, 0,98× avec environ 63 % de RAM en moins.
- **Derrière** sur les jobs CPU (0,82–0,89×) et à concurrence 4 (0,82–0,94×). Le coût CPU par job est plus élevé
  (cpu c=8 : ~0,47 CPU-s pour 1 000 jobs contre ~0,30), dont environ 135 µs par job pour les sidecars Ruby.
- Les chiffres de BullMQ varient d'une session à l'autre (jobs CPU c=8 : 14 255 puis 15 348) : ne comparer qu'au sein d'une même session.
- Les modèles diffèrent : BullMQ utilise N processus, solid-jobs-node N `worker_threads` + sidecars. BullMQ en `worker_threads` n'a pas été comparé.
- Les latences file → démarrage d'une file préremplie ne sont pas représentatives ; voir ci-dessous.

**Tableau 2 : après correction du prefetch** (réservation anticipée écrite avant le job). Médianes de 3 répétitions de
20 000 jobs (sauf cpu c=4 : 5 répétitions), session distincte du tableau 1 (BullMQ varie d'une session à l'autre : comparer BullMQ et solid dans le même tableau, pas d'un tableau à l'autre) :

| Charge | c | BullMQ (jobs/s) | solid-jobs-node x4+pf | x8+pf |
|---|---:|---:|---:|---:|
| cpu | 4 (5 rép. alternées, voir ci-dessous) | 12 145 | **12 126 (1,00×)** | 11 919 (0,99×) |
| cpu | 8 | 15 537 | 13 045 (0,84×) | 13 283 (0,85×) |
| cpu | 12 | 16 586 | 12 637 (0,76×, avant correction) | 14 097 (0,85×, avant correction) |
| noop | 4 | 15 312 (tableau 1) | 15 045 (0,98×) | 15 308 (1,00×) |
| noop | 8 | 18 352 (tableau 1) | 17 347 (0,95×) | 19 308 (1,05×) |

La ligne cpu c=4 vient d'une campagne dédiée de 5 répétitions alternées avec BullMQ (20 000 jobs, 0 erreur) :
BullMQ 12 145 jobs/s (11 649–12 188, 502 MiB) ; x4+pf 12 126 (11 810–12 238, 259 MiB), ratio apparié médian 1,004
(0,97× à 1,02× selon la paire), soit −48 % de RAM ; x8+pf 11 919 (11 024–12 174), ratio apparié médian 0,989, plus dispersé.
C'est une parité mesurée sur une machine et un benchmark, pas une supériorité. À c=8 le correctif n'apporte pas de gain
visible ; la limite restante n'est pas identifiée. Cycle mesuré fin du job N → début du job N+1 (cpu c=4, prefetch) :
~90 µs avant (25 % de successeurs déjà réservés), ~25 µs après (100 %).

**Latence file → démarrage** (arrivées à cadence fixe, 8 workers, jobs CPU de ~0,5 ms, une exécution par point, 20 % de
lignes de démarrage exclues). Le sondage adaptatif a fait passer p50 de ~125 ms (sommeil fixe de 250 ms) à quelques ms :

| Cadence | p50 | p95 | p99 |
|---|---:|---:|---:|
| 1 000 jobs/s | 3 ms | 5 ms | 7 ms |
| 3 000 jobs/s | 3,3 ms | 5,7 ms | 6,7 ms |
| 6 000 jobs/s | 5,3 ms | 9–13 ms | 15–24 ms |

À partir de ~8 000 jobs/s la capacité de ce scénario est dépassée (la file s'allonge). BullMQ n'a pas été mesuré sur cette latence ;
l'attente jusqu'à 250 ms après une longue inactivité subsiste. Le prefetch n'a pas montré d'effet sur la latence à ces charges.

## Écarts par rapport à la spec

- Le sidecar Ruby est démarré une fois par processus et partagé par tous les threads (une connexion par thread).
- Fichier de configuration : JSON ou module JS (pas de TOML dans Node).
- Pas de Warm IPC Pool (spécifique à Python) ni d'adaptateur Rails.

## Développement

```sh
bundle install
redis-server --port 6390 --save "" --appendonly no --daemonize yes
REDIS_URL=redis://127.0.0.1:6390/15 npm test
```

## Licence

LGPL-3.0-or-later — voir [LICENSE.txt](LICENSE.txt).
