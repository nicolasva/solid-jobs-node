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
`-t/--timeout`, `--redis-url`, `--warm-ipc`. Signaux : `SIGTERM`/`SIGINT` arrêt gracieux (les jobs en cours sont terminés),
`SIGTSTP` pause des réservations (quiet), `SIGTTIN` statut.

Variables d'environnement : `REDIS_URL`, `SOLID_JOBS_RUBY` (commande ruby, ex. `bundle exec ruby`),
`SOLID_JOBS_GATEWAY_SOCKET` (utiliser un sidecar déjà lancé), `SOLID_JOBS_LOG_LEVEL`.

Tests : `await Task.execute(...)` exécute en ligne ; `await Lab.capture(async () => { ... })` capture au lieu d'écrire
dans Redis (`Task.captured()`, `Task.executeNext()`).

## Concurrence

Le processus principal (Conductor, heartbeat, scheduler, récupération) pilote N `worker_threads`. Chaque thread a sa propre
connexion au sidecar (donc son propre `SolidRedis::Client`) et exécute une boucle claim → exécution → ack. Les workers ne
partagent aucune mémoire avec le thread principal (*shared-nothing*) ; heartbeat et statistiques remontent par `postMessage`.
Une tâche `async` n'occupe pas le worker pendant ses `await`, mais un job synchrone gourmand en CPU bloque son worker (pas les autres).

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
