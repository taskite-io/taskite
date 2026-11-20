# Taskite Go Client

The Go client implements every RPC currently exposed by the Taskite Gateway
Protocol and includes the `tkctl` command line tool.

## Client

Create a subscription for the Job type and tenant before creating a Job, and wait
for distribution to its Broker partitions. Missing subscriptions return `NOT_FOUND`.
Only `JobType` is required for a subscription. Name, variables, retries and
priority default to an empty name, `{}`, 3 and 50.

```go
package main

import (
	"context"
	"log"

	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
)

func main() {
	client, err := taskite.NewClient(taskite.ClientConfig{
		Gateway:                taskite.GatewayConfig{Address: "127.0.0.1:26500"},
		PlacementDriver:        taskite.PlacementDriverConfig{Address: "127.0.0.1:26501"},
		ClusterID:              "demo",
		TenantID:               "<default>",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	response, err := client.
		NewCreateJobCommand().
		JobType("payment").
		Variables(map[string]any{"orderId": 42}).
		Send(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("created job %d", response.GetJobKey())
}
```

The client supports job subscription creation/deletion, timer subscription
creation/deletion, job creation/activation/completion/failure/query/cancellation,
retry/timeout/priority updates, incident query/resolution, and variable updates.

Subscription creation and deletion responses expose `GetKey()`.
Use that key with `NewDeleteJobSubscriptionCommand().Key(key)` or
`NewDeleteTimerSubscriptionCommand().Key(key)`:

```go
subscription, err := client.NewCreateJobSubscriptionCommand().
	JobType("payment").
	Send(context.Background())
if err != nil {
	log.Fatal(err)
}
_, err = client.NewDeleteJobSubscriptionCommand().
	Key(subscription.GetKey()).Send(context.Background())
if err != nil {
	log.Fatal(err)
}
```

Deleting a job subscription confirms PD persistence first. Each Broker partition
then physically removes the subscription and all in-flight jobs associated with
its subscription key, including their incidents and variables.
Jobs created directly or by timers require a subscription matching their job type
and tenant. Broker merges its template variables with request variables; duplicate
top-level keys use the request value, including nested objects, arrays and null.

Connection zero values can be supplied through:

|                   Environment variable                    |      Default      |                    Purpose                    |
|-----------------------------------------------------------|-------------------|-----------------------------------------------|
| `TASKITE_GATEWAY_ADDRESS`                                 | `127.0.0.1:26500` | Gateway gRPC address                          |
| `TASKITE_PLACEMENT_DRIVER_ADDRESS`                        | `127.0.0.1:26501` | Placement Driver gRPC address used by workers |
| `TASKITE_CLUSTER_ID`                                      | none, required    | Cluster identifier sent through gRPC metadata |
| `TASKITE_TENANT_ID`                                       | `<default>`       | Default request tenant and Worker tenant      |
| `TASKITE_TRANSPORT_KEEP_ALIVE`                            | `45s`             | gRPC keepalive interval                       |
| `TASKITE_WORKER_REGISTRATION_RENEW_INTERVAL`              | `5s`              | Worker renewal interval                       |
| `TASKITE_WORKER_REGISTRATION_RENEWAL_TIMEOUT`             | `30s`             | Local PD RPC timeout, also reported to PD     |
| `TASKITE_WORKER_REGISTRATION_BACKPRESSURE_MIN_DELAY`      | `50ms`            | Initial retry delay                           |
| `TASKITE_WORKER_REGISTRATION_BACKPRESSURE_MAX_DELAY`      | `5s`              | Maximum retry delay                           |
| `TASKITE_WORKER_REGISTRATION_BACKPRESSURE_BACKOFF_FACTOR` | `1.6`             | Retry growth factor                           |
| `TASKITE_WORKER_REGISTRATION_BACKPRESSURE_JITTER_FACTOR`  | `0.1`             | Symmetric jitter ratio                        |

The client writes `x-cluster-id` to gRPC Metadata and supplies `taskite-tenant-id`
from `ClientConfig.TenantID` or `TASKITE_TENANT_ID` when the request has no explicit
tenant. This default applies to all Gateway commands, including subscriptions,
job creation and activation. Explicit request Metadata, Job tenant values
and activation/Worker `TenantIDs` take precedence. For multiple activation
tenants, the first tenant selects the request context and the full list remains
the scan scope. Workers use the same default for Placement Driver registration
when `TenantIDs` is not set.

Tenant selection remains subject to the access established by the configured
Gateway authentication interceptor. With multi-tenancy disabled, only
`<default>` is authorized; selecting another tenant is rejected. The current
client uses plaintext gRPC and does not expose TLS configuration.

Connection fields are grouped under `Gateway.Address`, `PlacementDriver.Address`
and `Transport.KeepAlive`. Explicit Go values override environment variables, then
defaults apply. SDK environment names omit `CLIENT`.

Gateway and Placement Driver addresses accept IPv4, DNS names and IPv6.
Use `[IPv6]:port` for an explicit IPv6 port, for example
`[2001:db8::1]:26500`. A host without a port
(including `::1` or `[::1]`) uses port `26500` for Gateway and `26501` for PD.
Complete bare IPv6 literals are always treated as hosts; use brackets to avoid
ambiguity between a final IPv6 segment and a port. Legacy bare IPv6 endpoints
such as `::1:26500` are also accepted when the whole value is not a valid IPv6
literal. Scoped addresses such as `[fe80::1%eth0]:26500` retain their interface
zone. Explicit gRPC resolver targets such as `dns:///[::1]:26500` and
`passthrough:///custom-target` keep their original semantics.

## Job Worker

```go
jobWorker, err := client.
	NewJobWorker().
	JobType("payment").
	Handler(func(jobClient worker.JobClient, job entities.Job) {
		_, completeErr := jobClient.
			NewCompleteJobCommand().
			Job(job).
			Variables(map[string]any{"paid": true}).
			Send(context.Background())
		if completeErr != nil {
			log.Printf("complete job: %v", completeErr)
		}
	}).
	Name("payment-worker").
	MaxJobsActive(32).
	Concurrency(4).
	Open()
if err != nil {
	log.Fatal(err)
}

shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := jobWorker.Close(shutdown); err != nil {
	log.Printf("close worker: %v", err)
}
```

Handlers explicitly complete or fail their jobs. Closing a worker stops new
activation requests and waits for already activated jobs to finish.
An optional `ShutdownHandler` returns jobs that have not started, concurrently
with running handlers. Without it, the queue is drained by the normal handler.

`Open()` waits for registration to commit before activating jobs. Each opened
worker has its own Worker ID and keeps the returned positive `workerLeaseToken`
for its entire session; all its handlers share that session. An open worker then sends
`RENEW` commands at `ClientConfig.Worker.Registration.RenewInterval` (5 seconds by default). Renewals
report the worker name, job type, cluster, tenants, active/max job capacity,
concurrency, polling timeouts, fetched variables, client version and the configured
`RenewalTimeout`. Registration, renewal and deregistration each use this value
as their local PD RPC timeout (30 seconds by default); an earlier caller deadline
or cancellation takes precedence. The timeout is independent of the renewal
interval and does not change PD's authoritative offline lease. `Close` stops
polling, keeps renewing while handlers and queued jobs drain, then stops renewals
and deregisters. A close deadline cancels queued dispatch and ends the session;
before returning the deadline error, `Close` waits for one bounded deregistration
attempt (up to `RenewalTimeout` beyond the handler grace period). Running SDK
handlers may still finish. An ungraceful exit is marked
offline after the PD-configured `taskite.pd.worker.lease-timeout`.
Registration, renewal and deregistration use the generated
`placementDriverProtocol.PlacementDriver` client on the Placement
Driver's existing gRPC port. Heartbeats run in a dedicated goroutine with their
own cancellable context. A renewal rejection or an elapsed lease deadline enters
`FENCED`, stops polling and discards queued jobs; the SDK never registers a new
session from its heartbeat loop. Technical renewal failures use bounded backoff.
A client-side long-poll deadline is
treated as an empty activation result and immediately continues polling.

Complete and fail builders accept `Job(job)` to copy the Worker ID, token and
tenant from the activation response. The handler's `JobClient` also binds these
fields when `JobKey` names its current job. Outside a handler, callers using
`JobKey` can bind `WorkerSession(workerID, token).TenantID(tenantID)` explicitly.
The Job tenant is selected with `taskite-tenant-id` Metadata and checked against
the Gateway's authenticated tenant access. Token propagation always runs,
regardless of the Broker's `worker-job-lease-token.enabled` setting.

For one-shot CLI operations, supply `--worker-id` and `--worker-lease-token`
to `activate jobs`, `complete job` or `fail job`; complete/fail also accept
`--tenant`. The continuous `--handler` worker obtains and binds its session
automatically.

`tkctl activate jobs --handler` handles Ctrl+C and SIGTERM by stopping polling and
failing queued jobs with their current retry count and `--retry-backoff` (1 second
by default). Running handlers can finish within `--shutdown-timeout` (30 seconds
by default). When that grace period expires, their contexts and Unix process
groups are canceled; the CLI waits up to one additional second to reap processes.
It deregisters the session before closing RPC connections, so PD can yield
unfinished jobs. If deregistration fails, it reports the error and stops renewing
so PD lease expiry can take over. Job yielding is enabled by default on all
Brokers. Set `TASKITE_BROKER_WORKER_JOB_LEASE_TOKEN_ENABLED=false` to disable it.

Heartbeat and registration backpressure are configured together under
`ClientConfig.Worker.Registration`:

```go
jitter := 0.1
config := client.ClientConfig{
	ClusterID: "demo",
	Worker: client.WorkerConfig{
		Registration: worker.RegistrationConfig{
			RenewInterval:  5 * time.Second,
			RenewalTimeout: 30 * time.Second,
			Backpressure: worker.BackpressureConfig{
				MinDelay:      50 * time.Millisecond,
				MaxDelay:      5 * time.Second,
				BackoffFactor: 1.6,
				JitterFactor:  &jitter,
			},
		},
	},
}
```

Omitted parameters use these defaults. Explicit code values override
`TASKITE_WORKER_REGISTRATION_BACKPRESSURE_MIN_DELAY`, `MAX_DELAY`,
`BACKOFF_FACTOR`, and `JITTER_FACTOR` (each with the same full prefix).
`JitterFactor: nil` selects the default; a pointer to zero disables jitter.
`TASKITE_WORKER_REGISTRATION_RENEW_INTERVAL` and `TASKITE_WORKER_REGISTRATION_RENEWAL_TIMEOUT`
configure the two durations. The interval must be at least 1ms and less than the
RPC timeout. Custom registry builders accept the same group in `RegistryConfig.Registration`.
The old flat `WorkerHeartbeatInterval` and heartbeat configuration group have
been merged into registration.

Only `RESOURCE_EXHAUSTED` errors prefixed with `BACKPRESSURE:` trigger immediate
serial retries. Each delay multiplies the previous actual delay by the growth
factor, applies symmetric jitter, and is capped by both `MaxDelay` and
`Registration.RenewInterval`. A successful registration or renewal resets the delay;
the initial registration keeps using `REGISTER` until it succeeds.
`worker.NewExponentialBackoffBuilder()` provides `MinDelay`, `MaxDelay`,
`BackoffFactor`, `JitterFactor`, `Random`, and `Build` for the same retry algorithm.
Job activation retains its existing deterministic retry defaults.

## tkctl

Build the binary:

```bash
go build -o bin/tkctl ./cmd/tkctl
```

For a local Gateway:

```bash
export TASKITE_GATEWAY_ADDRESS=127.0.0.1:26500
export TASKITE_PLACEMENT_DRIVER_ADDRESS=127.0.0.1:26501
export TASKITE_CLUSTER_ID=demo


bin/tkctl create subscription payment
# 确认订阅已分发至 Broker 分区后，再并发创建 1000 个任务
seq 1 1000 | xargs -n 1 -P 10 sh -c 'bin/tkctl create job payment --variables "{\"name\":\"lizhi-$1\"}"' _

bin/tkctl create job payment --variables '{"orderId":42}'
bin/tkctl create timer payment --name payment-timer --type duration --expression PT30S
bin/tkctl activate jobs payment --max-jobs 10
bin/tkctl activate jobs payment --handler cat
```

For an IPv6 Gateway and Placement Driver:

```bash
export TASKITE_GATEWAY_ADDRESS='[2001:db8::1]:26500'
export TASKITE_PLACEMENT_DRIVER_ADDRESS='[2001:db8::2]:26501'
export TASKITE_CLUSTER_ID=demo

bin/tkctl query job 42
bin/tkctl --address '[::1]:26500' query job 42
bin/tkctl activate jobs payment --handler cat
```

`--address` overrides `TASKITE_GATEWAY_ADDRESS`; both use the client's address parser.
`--request-timeout` overrides `TASKITE_CLI_REQUEST_TIMEOUT`, then defaults to `30s`.
Invalid or non-positive durations are rejected before opening a client.
Handler workers use `TASKITE_PLACEMENT_DRIVER_ADDRESS` for registration,
renewal and deregistration.

With `--handler`, `tkctl` runs a continuous worker until `Ctrl-C` or `SIGTERM`.
Job variables are written to the command's stdin. A successful command
completes the job using its JSON stdout; a failed command fails the job using
stderr as the error message. The terminal logs worker startup, activated job
keys and variables, and handler completion.

Run `bin/tkctl --help` for the complete command tree.

## Protocol Generation

Generated Gateway protobuf and gRPC files are committed under `pkg/pb`, and
Placement Driver files under `pkg/pb/placementdriver`. Regenerate
them with:

```bash
make generate
```

The Makefile installs pinned generator versions into the ignored local
`.tools` directory when they are not already available there.

The canonical protocol sources are:

```text
../../gateway-protocol/src/main/resources/gateway.proto
../../placement-driver-protocol/src/main/resources/placement_driver.proto
```

