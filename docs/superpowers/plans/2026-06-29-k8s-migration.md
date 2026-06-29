# Kubernetes Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the wiki-stream-lab stack on Kubernetes (kind locally, cloud-portable) and demonstrate Redpanda consumer-group scaling via `kubectl scale`.

**Architecture:** A single Helm chart under `deploy/k8s/chart` packages every Compose service as a StatefulSet (stateful: redpanda, clickhouse, rustfs), Deployment (apps + UIs), or Helm hook Job (topics-init). Kustomize overlays under `deploy/k8s/overlays/{local,cloud}` run as Helm post-renderers to patch per-environment config (storage class, ingress, secrets). Docker Compose is left untouched.

**Tech Stack:** Helm 3, Kustomize (kubectl built-in / standalone), kind, podman, existing `wiki-stream-lab:local` image (one image, binary selected per `command`).

## Global Constraints

- Implementation is manifests + Helm templates; no Go source changes expected. Copy exact values from the spec at `docs/superpowers/specs/2026-06-29-k8s-migration-design.md`.
- Kafka stays central; producer is a singleton (`replicas: 1`, never scaled).
- No real secrets committed. Dev creds only (`rustfsadmin`/`rustfsadmin`); cloud overlay references an externally-created Secret by name.
- App image is distroless static — **no shell in app pods**. No shell-based probes on producer/validator/projector/archiver/laker. Redpanda/clickhouse/rustfs images have their own tooling for probes.
- Topics `wikimedia.recentchange.raw` and `.validated` have **6 partitions** (set by `internal/kafka/topics.go`); do not change.
- Kafka broker DNS in-cluster: `redpanda:9092`. S3 endpoint in-cluster: `http://rustfs:9000`.
- **Commits:** Do NOT commit unless Mike explicitly asks (CLAUDE.md overrides the plan's commit steps). Each Task ends at a STOP gate for Mike's review per `docs/PR_CONSTITUTION.md`; commit only on his go-ahead.
- Compose remains the documented local path; never edit `docker-compose.yml` in this plan.
- Helm `.Files.Get` can only read inside the chart dir — config files from `deploy/clickhouse` and `deploy/grafana` are copied into `chart/files/` (duplication is intentional; note it).

## File Structure

```text
deploy/k8s/
  chart/
    Chart.yaml
    values.yaml
    files/                       # copied configs (Helm .Files can't reach outside chart)
      clickhouse-init.sql
      clickhouse-kafka.xml
      grafana/...                # provisioning + dashboards
    templates/
      _helpers.tpl
      secret.yaml                # S3 creds from values
      redpanda.yaml              # StatefulSet + headless Service
      topics-init-job.yaml       # Helm hook
      producer.yaml validator.yaml
      projector.yaml             # + sqlite-web sidecar + PVC
      laker.yaml archiver.yaml
      rustfs.yaml                # StatefulSet + Service
      clickhouse.yaml clickhouse-config.yaml
      grafana.yaml console.yaml
      duckdb-ui.yaml             # gated by .Values.tools.enabled
  overlays/
    local/{kustomization.yaml,kustomize.sh}
    cloud/{kustomization.yaml,kustomize.sh,ingress.yaml}
  kind-cluster.yaml
  README.md
```

Each Task below is one PR (one reviewable unit) and leaves Compose working.

---

### Task 1 (PR-k1): Chart skeleton + Redpanda + topics-init + kind

**Files:**
- Create: `deploy/k8s/kind-cluster.yaml`
- Create: `deploy/k8s/chart/Chart.yaml`
- Create: `deploy/k8s/chart/values.yaml`
- Create: `deploy/k8s/chart/templates/_helpers.tpl`
- Create: `deploy/k8s/chart/templates/redpanda.yaml`
- Create: `deploy/k8s/chart/templates/topics-init-job.yaml`
- Create: `deploy/k8s/README.md`

**Interfaces:**
- Produces: Service `redpanda` (ClusterIP None, port 9092) — every later app sets `KAFKA_BROKERS=redpanda:9092`.
- Produces: chart name `wiki-stream-lab`, release name convention `wsl`, helper `wsl.image` returning `{{repo}}:{{tag}}`.
- Produces: `values.yaml` keys `image.*`, `kafka.brokers`, `s3.*`, `tools.enabled`, `storage.*` consumed by all later tasks.

- [ ] **Step 1: Write the kind cluster config**

Create `deploy/k8s/kind-cluster.yaml`:

```yaml
# 1 control-plane + 2 workers so scaled consumer pods can land on different
# nodes — makes the rebalance demo visibly multi-node.
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
```

- [ ] **Step 2: Write Chart.yaml**

Create `deploy/k8s/chart/Chart.yaml`:

```yaml
apiVersion: v2
name: wiki-stream-lab
description: Local Kafka/Redpanda learning lab on Kubernetes
type: application
version: 0.1.0
appVersion: "0.1.0"
```

- [ ] **Step 3: Write values.yaml**

Create `deploy/k8s/chart/values.yaml`:

```yaml
image:
  repository: wiki-stream-lab
  tag: local
  pullPolicy: IfNotPresent

kafka:
  brokers: "redpanda:9092"

s3:
  endpoint: "http://rustfs:9000"
  bucket: "wiki-stream-lab"
  accessKey: "rustfsadmin"
  secretKey: "rustfsadmin"

# Per-consumer slowdown for the lag demo (ms). 0 = full speed.
slowConsumerMs: 0

tools:
  enabled: false

storage:
  # Empty = use the cluster default StorageClass (local-path on kind).
  # Cloud overlay sets a real class.
  className: ""
  redpanda: 2Gi
  clickhouse: 2Gi
  rustfs: 5Gi
  sqlite: 1Gi
```

- [ ] **Step 4: Write _helpers.tpl**

Create `deploy/k8s/chart/templates/_helpers.tpl`:

```yaml
{{- define "wsl.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}

{{- define "wsl.storageClass" -}}
{{- if .Values.storage.className }}
storageClassName: {{ .Values.storage.className }}
{{- end }}
{{- end -}}
```

- [ ] **Step 5: Write the Redpanda StatefulSet + headless Service**

Create `deploy/k8s/chart/templates/redpanda.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redpanda
  labels: { app: redpanda }
spec:
  clusterIP: None          # headless: stable pod DNS redpanda-0.redpanda
  selector: { app: redpanda }
  ports:
    - { name: kafka, port: 9092, targetPort: 9092 }
    - { name: admin, port: 9644, targetPort: 9644 }
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redpanda
spec:
  serviceName: redpanda
  replicas: 1
  selector: { matchLabels: { app: redpanda } }
  template:
    metadata:
      labels: { app: redpanda }
    spec:
      containers:
        - name: redpanda
          image: redpandadata/redpanda:v24.2.7
          args:
            - redpanda
            - start
            - --mode=dev-container
            - --smp=1
            - --default-log-level=info
            - --kafka-addr=internal://0.0.0.0:9092
            - --advertise-kafka-addr=internal://redpanda:9092
            - --rpc-addr=0.0.0.0:33145
            - --advertise-rpc-addr=redpanda:33145
          ports:
            - { containerPort: 9092, name: kafka }
            - { containerPort: 9644, name: admin }
          readinessProbe:
            exec:
              command: ["sh","-c","rpk cluster health | grep -q 'Healthy:.*true'"]
            initialDelaySeconds: 10
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 12
          volumeMounts:
            - { name: data, mountPath: /var/lib/redpanda/data }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: ["ReadWriteOnce"]
        {{- include "wsl.storageClass" . | nindent 8 }}
        resources: { requests: { storage: {{ .Values.storage.redpanda }} } }
```

- [ ] **Step 6: Write the topics-init Helm hook Job**

Create `deploy/k8s/chart/templates/topics-init-job.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: topics-init
  annotations:
    "helm.sh/hook": post-install,post-upgrade
    "helm.sh/hook-weight": "-5"
    "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: 10        # retry until redpanda is reachable
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: topics-init
          image: {{ include "wsl.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["/app/cli","topics","create"]
          env:
            - { name: KAFKA_BROKERS, value: "{{ .Values.kafka.brokers }}" }
```

- [ ] **Step 7: Write the README**

Create `deploy/k8s/README.md`:

```markdown
# wiki-stream-lab on Kubernetes

Helm chart (packaging) + Kustomize overlays (per-env config). Docker Compose
is unchanged and still the simplest local path; this is the cloud-ready route
and the home of the consumer-group scaling demo.

## Local quickstart (kind)

    kind create cluster --config deploy/k8s/kind-cluster.yaml
    podman build -t wiki-stream-lab:local .
    kind load docker-image wiki-stream-lab:local
    helm install wsl deploy/k8s/chart \
      --post-renderer deploy/k8s/overlays/local/kustomize.sh
    kubectl get pods -w

## Scaling demo (added in PR-k5)

    kubectl scale deploy/laker --replicas=4
    kubectl run lag --rm -it --image=wiki-stream-lab:local -- \
      /app/cli lag lake-parquet wikimedia.recentchange.validated
```

- [ ] **Step 8: Lint and render — verify it fails first if broken, then passes**

Run: `helm lint deploy/k8s/chart`
Expected: `1 chart(s) linted, 0 chart(s) failed`

Run: `helm template wsl deploy/k8s/chart | kubectl apply --dry-run=client -f -`
Expected: redpanda Service, StatefulSet, and topics-init Job all print `(dry run)` with no schema errors.

- [ ] **Step 9: Live verify on kind**

```bash
kind create cluster --config deploy/k8s/kind-cluster.yaml
podman build -t wiki-stream-lab:local .
kind load docker-image wiki-stream-lab:local
helm install wsl deploy/k8s/chart
kubectl rollout status statefulset/redpanda --timeout=120s
kubectl wait --for=condition=complete job/topics-init --timeout=120s
kubectl run rpk --rm -it --restart=Never --image=redpandadata/redpanda:v24.2.7 \
  --command -- rpk topic list --brokers redpanda:9092
```
Expected: `rollout status` reports ready; job completes; topic list shows `wikimedia.recentchange.raw` and `.validated` with 6 partitions, `.dead_letter` with 1. Paste real output into the handoff.

- [ ] **Step 10: Commit (only if Mike asks)**

```bash
git add deploy/k8s/kind-cluster.yaml deploy/k8s/chart deploy/k8s/README.md
git commit -m "feat(k8s): redpanda StatefulSet + topics-init + chart skeleton"
```

- [ ] **Step 11: STOP — hand off PR-k1 for review per PR_CONSTITUTION.md. Do not start Task 2 until Mike approves.**

---

### Task 2 (PR-k2): Pipeline apps — producer, validator, projector (+ sqlite-web sidecar)

**Files:**
- Create: `deploy/k8s/chart/templates/producer.yaml`
- Create: `deploy/k8s/chart/templates/validator.yaml`
- Create: `deploy/k8s/chart/templates/projector.yaml`

**Interfaces:**
- Consumes: Service `redpanda` and `values.kafka.brokers` from Task 1; `wsl.image` helper; `wsl.storageClass` helper.
- Produces: Deployment `producer` (1 replica), `validator`, `projector`; PVC `projector-data`; Service `sqlite-web` (port 8081).

- [ ] **Step 1: Write the producer Deployment**

Create `deploy/k8s/chart/templates/producer.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: producer
spec:
  replicas: 1               # singleton firehose — NEVER scale (would duplicate data)
  selector: { matchLabels: { app: producer } }
  template:
    metadata:
      labels: { app: producer }
    spec:
      containers:
        - name: producer
          image: {{ include "wsl.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["/app/producer"]
          env:
            - { name: KAFKA_BROKERS, value: "{{ .Values.kafka.brokers }}" }
            - { name: PRODUCER_MAX_SECONDS, value: "0" }
```

- [ ] **Step 2: Write the validator Deployment**

Create `deploy/k8s/chart/templates/validator.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: validator
spec:
  replicas: 1               # scalable consumer group; 1 is fine by default
  selector: { matchLabels: { app: validator } }
  template:
    metadata:
      labels: { app: validator }
    spec:
      containers:
        - name: validator
          image: {{ include "wsl.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["/app/validator"]
          env:
            - { name: KAFKA_BROKERS, value: "{{ .Values.kafka.brokers }}" }
```

- [ ] **Step 3: Write the projector Deployment with sqlite-web sidecar + PVC**

Create `deploy/k8s/chart/templates/projector.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: projector-data
spec:
  accessModes: ["ReadWriteOnce"]
  {{- include "wsl.storageClass" . | nindent 2 }}
  resources: { requests: { storage: {{ .Values.storage.sqlite }} } }
---
apiVersion: v1
kind: Service
metadata:
  name: sqlite-web
  labels: { app: projector }
spec:
  selector: { app: projector }
  ports:
    - { name: http, port: 8081, targetPort: 8080 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: projector
spec:
  replicas: 1               # singleton: one logical SQLite projection (RWO PVC)
  strategy: { type: Recreate }   # RWO volume can't be held by two pods during rollout
  selector: { matchLabels: { app: projector } }
  template:
    metadata:
      labels: { app: projector }
    spec:
      containers:
        - name: projector
          image: {{ include "wsl.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["/app/projector"]
          env:
            - { name: KAFKA_BROKERS, value: "{{ .Values.kafka.brokers }}" }
            - { name: SQLITE_PATH, value: "/data/wiki-stream-lab.sqlite" }
            - { name: SLOW_CONSUMER_MS, value: "{{ .Values.slowConsumerMs }}" }
          volumeMounts:
            - { name: data, mountPath: /data }
        - name: sqlite-web
          image: coleifer/sqlite-web:latest
          args: ["sqlite_web","--host","0.0.0.0","--port","8080","--read-only","/data/wiki-stream-lab.sqlite"]
          ports:
            - { containerPort: 8080, name: http }
          volumeMounts:
            - { name: data, mountPath: /data, readOnly: true }
      volumes:
        - name: data
          persistentVolumeClaim: { claimName: projector-data }
```

- [ ] **Step 4: Lint and render**

Run: `helm lint deploy/k8s/chart`
Expected: `0 chart(s) failed`.

Run: `helm template wsl deploy/k8s/chart | kubectl apply --dry-run=client -f -`
Expected: producer, validator, projector Deployments + PVC + sqlite-web Service validate with no errors.

- [ ] **Step 5: Live verify on kind**

```bash
helm upgrade wsl deploy/k8s/chart
kubectl rollout status deploy/projector --timeout=120s
kubectl rollout status deploy/validator --timeout=120s
sleep 20
kubectl port-forward svc/sqlite-web 8081:8081 &
curl -s localhost:8081 | head -n 20
```
Expected: pods Running; projector logs show rows projected (`kubectl logs deploy/projector -c projector`); sqlite-web HTTP responds. Paste real output.

- [ ] **Step 6: Commit (only if Mike asks)**

```bash
git add deploy/k8s/chart/templates/producer.yaml deploy/k8s/chart/templates/validator.yaml deploy/k8s/chart/templates/projector.yaml
git commit -m "feat(k8s): producer, validator, projector with sqlite-web sidecar"
```

- [ ] **Step 7: STOP — hand off PR-k2 for review. Do not start Task 3 until Mike approves.**

---

### Task 3 (PR-k3): Object store + lake — rustfs, S3 secret, archiver, laker

**Files:**
- Create: `deploy/k8s/chart/templates/secret.yaml`
- Create: `deploy/k8s/chart/templates/rustfs.yaml`
- Create: `deploy/k8s/chart/templates/archiver.yaml`
- Create: `deploy/k8s/chart/templates/laker.yaml`

**Interfaces:**
- Consumes: `values.s3.*`, `wsl.image`, `wsl.storageClass`, Service `redpanda`.
- Produces: Secret `s3-creds` (keys `accessKey`,`secretKey`); Service `rustfs` (port 9000 API, 9001 console); Deployments `archiver`, `laker` (consumer groups `archiver-raw`, `lake-parquet`).

- [ ] **Step 1: Write the S3 credentials Secret**

Create `deploy/k8s/chart/templates/secret.yaml`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3-creds
type: Opaque
stringData:
  accessKey: {{ .Values.s3.accessKey | quote }}
  secretKey: {{ .Values.s3.secretKey | quote }}
```

- [ ] **Step 2: Write the rustfs StatefulSet + Service**

Create `deploy/k8s/chart/templates/rustfs.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rustfs
  labels: { app: rustfs }
spec:
  selector: { app: rustfs }
  ports:
    - { name: s3, port: 9000, targetPort: 9000 }
    - { name: console, port: 9001, targetPort: 9001 }
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rustfs
spec:
  serviceName: rustfs
  replicas: 1
  selector: { matchLabels: { app: rustfs } }
  template:
    metadata:
      labels: { app: rustfs }
    spec:
      containers:
        - name: rustfs
          image: rustfs/rustfs:latest
          env:
            - name: RUSTFS_ACCESS_KEY
              valueFrom: { secretKeyRef: { name: s3-creds, key: accessKey } }
            - name: RUSTFS_SECRET_KEY
              valueFrom: { secretKeyRef: { name: s3-creds, key: secretKey } }
          ports:
            - { containerPort: 9000, name: s3 }
            - { containerPort: 9001, name: console }
          volumeMounts:
            - { name: data, mountPath: /data }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: ["ReadWriteOnce"]
        {{- include "wsl.storageClass" . | nindent 8 }}
        resources: { requests: { storage: {{ .Values.storage.rustfs }} } }
```

- [ ] **Step 3: Write the archiver Deployment**

Create `deploy/k8s/chart/templates/archiver.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: archiver
spec:
  replicas: 1               # scalable consumer group (archiver-raw)
  selector: { matchLabels: { app: archiver } }
  template:
    metadata:
      labels: { app: archiver }
    spec:
      containers:
        - name: archiver
          image: {{ include "wsl.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["/app/archiver"]
          env:
            - { name: KAFKA_BROKERS, value: "{{ .Values.kafka.brokers }}" }
            - { name: S3_ENDPOINT, value: "{{ .Values.s3.endpoint }}" }
            - { name: S3_BUCKET, value: "{{ .Values.s3.bucket }}" }
            - name: S3_ACCESS_KEY
              valueFrom: { secretKeyRef: { name: s3-creds, key: accessKey } }
            - name: S3_SECRET_KEY
              valueFrom: { secretKeyRef: { name: s3-creds, key: secretKey } }
```

- [ ] **Step 4: Write the laker Deployment**

Create `deploy/k8s/chart/templates/laker.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: laker
spec:
  replicas: 1               # SCALE TARGET for the demo (group lake-parquet)
  selector: { matchLabels: { app: laker } }
  template:
    metadata:
      labels: { app: laker }
    spec:
      containers:
        - name: laker
          image: {{ include "wsl.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["/app/laker"]
          env:
            - { name: KAFKA_BROKERS, value: "{{ .Values.kafka.brokers }}" }
            - { name: S3_ENDPOINT, value: "{{ .Values.s3.endpoint }}" }
            - { name: S3_BUCKET, value: "{{ .Values.s3.bucket }}" }
            - name: S3_ACCESS_KEY
              valueFrom: { secretKeyRef: { name: s3-creds, key: accessKey } }
            - name: S3_SECRET_KEY
              valueFrom: { secretKeyRef: { name: s3-creds, key: secretKey } }
```

- [ ] **Step 5: Lint and render**

Run: `helm lint deploy/k8s/chart && helm template wsl deploy/k8s/chart | kubectl apply --dry-run=client -f -`
Expected: Secret, rustfs Service + StatefulSet, archiver + laker Deployments validate; no errors.

- [ ] **Step 6: Live verify on kind**

```bash
helm upgrade wsl deploy/k8s/chart
kubectl rollout status statefulset/rustfs --timeout=120s
kubectl rollout status deploy/laker --timeout=120s
sleep 30
kubectl exec statefulset/rustfs -- ls -R /data | grep -E 'lake|backup' | head
```
Expected: rustfs Running; laker/archiver logs show objects written; `/data` contains `lake/` Parquet objects and `backup/` JSONL. Paste real output.

- [ ] **Step 7: Commit (only if Mike asks)**

```bash
git add deploy/k8s/chart/templates/secret.yaml deploy/k8s/chart/templates/rustfs.yaml deploy/k8s/chart/templates/archiver.yaml deploy/k8s/chart/templates/laker.yaml
git commit -m "feat(k8s): rustfs object store + archiver + laker with S3 secret"
```

- [ ] **Step 8: STOP — hand off PR-k3 for review. Do not start Task 4 until Mike approves.**

---

### Task 4 (PR-k4): OLAP + UIs — clickhouse, grafana, console

**Files:**
- Create: `deploy/k8s/chart/files/clickhouse-init.sql` (copy of `deploy/clickhouse/init.sql`)
- Create: `deploy/k8s/chart/files/clickhouse-kafka.xml` (copy of `deploy/clickhouse/config.d/kafka.xml`)
- Create: `deploy/k8s/chart/files/grafana/` (copy of `deploy/grafana/provisioning` + `deploy/grafana/dashboards`)
- Create: `deploy/k8s/chart/templates/clickhouse-config.yaml`
- Create: `deploy/k8s/chart/templates/clickhouse.yaml`
- Create: `deploy/k8s/chart/templates/grafana.yaml`
- Create: `deploy/k8s/chart/templates/console.yaml`

**Interfaces:**
- Consumes: Service `redpanda`, `wsl.image`, `wsl.storageClass`, `.Files.Get`.
- Produces: Service `clickhouse` (8123 HTTP, 9000 native), `grafana` (3000), `console` (8080).

- [ ] **Step 1: Copy config files into the chart**

```bash
mkdir -p deploy/k8s/chart/files/grafana
cp deploy/clickhouse/init.sql            deploy/k8s/chart/files/clickhouse-init.sql
cp deploy/clickhouse/config.d/kafka.xml  deploy/k8s/chart/files/clickhouse-kafka.xml
cp -R deploy/grafana/provisioning        deploy/k8s/chart/files/grafana/provisioning
cp -R deploy/grafana/dashboards          deploy/k8s/chart/files/grafana/dashboards
```
Note in the handoff: these are intentional copies because Helm `.Files.Get` cannot read outside the chart directory.

- [ ] **Step 2: Write the ClickHouse ConfigMap template**

Create `deploy/k8s/chart/templates/clickhouse-config.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-init
data:
  init.sql: |
{{ .Files.Get "files/clickhouse-init.sql" | indent 4 }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-kafka
data:
  kafka.xml: |
{{ .Files.Get "files/clickhouse-kafka.xml" | indent 4 }}
```

- [ ] **Step 3: Write the ClickHouse StatefulSet + Service**

Create `deploy/k8s/chart/templates/clickhouse.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: clickhouse
  labels: { app: clickhouse }
spec:
  selector: { app: clickhouse }
  ports:
    - { name: http, port: 8123, targetPort: 8123 }
    - { name: native, port: 9000, targetPort: 9000 }
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse
spec:
  serviceName: clickhouse
  replicas: 1
  selector: { matchLabels: { app: clickhouse } }
  template:
    metadata:
      labels: { app: clickhouse }
    spec:
      containers:
        - name: clickhouse
          image: clickhouse/clickhouse-server:24.8
          env:
            - { name: CLICKHOUSE_SKIP_USER_SETUP, value: "1" }
          ports:
            - { containerPort: 8123, name: http }
            - { containerPort: 9000, name: native }
          volumeMounts:
            - { name: init, mountPath: /docker-entrypoint-initdb.d/init.sql, subPath: init.sql, readOnly: true }
            - { name: kafka, mountPath: /etc/clickhouse-server/config.d/kafka.xml, subPath: kafka.xml, readOnly: true }
            - { name: data, mountPath: /var/lib/clickhouse }
      volumes:
        - name: init
          configMap: { name: clickhouse-init }
        - name: kafka
          configMap: { name: clickhouse-kafka }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: ["ReadWriteOnce"]
        {{- include "wsl.storageClass" . | nindent 8 }}
        resources: { requests: { storage: {{ .Values.storage.clickhouse }} } }
```

- [ ] **Step 4: Write the Grafana Deployment + provisioning ConfigMaps**

Create `deploy/k8s/chart/templates/grafana.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
data:
{{ (.Files.Glob "files/grafana/provisioning/datasources/*").AsConfig | indent 2 }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboards-provider
data:
{{ (.Files.Glob "files/grafana/provisioning/dashboards/*").AsConfig | indent 2 }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboards
data:
{{ (.Files.Glob "files/grafana/dashboards/*").AsConfig | indent 2 }}
---
apiVersion: v1
kind: Service
metadata:
  name: grafana
  labels: { app: grafana }
spec:
  selector: { app: grafana }
  ports:
    - { name: http, port: 3000, targetPort: 3000 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
spec:
  replicas: 1
  selector: { matchLabels: { app: grafana } }
  template:
    metadata:
      labels: { app: grafana }
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:11.2.0
          env:
            - { name: GF_AUTH_ANONYMOUS_ENABLED, value: "true" }
            - { name: GF_AUTH_ANONYMOUS_ORG_ROLE, value: "Admin" }
            - { name: GF_AUTH_DISABLE_LOGIN_FORM, value: "true" }
            - { name: GF_INSTALL_PLUGINS, value: "grafana-clickhouse-datasource" }
          ports:
            - { containerPort: 3000, name: http }
          volumeMounts:
            - { name: datasources, mountPath: /etc/grafana/provisioning/datasources }
            - { name: dashprovider, mountPath: /etc/grafana/provisioning/dashboards }
            - { name: dashboards, mountPath: /var/lib/grafana/dashboards }
      volumes:
        - { name: datasources, configMap: { name: grafana-datasources } }
        - { name: dashprovider, configMap: { name: grafana-dashboards-provider } }
        - { name: dashboards, configMap: { name: grafana-dashboards } }
```

Note: the dashboard provider yaml references path `/var/lib/grafana/dashboards`; confirm the copied `dashboards.yml` uses that path (it does in the Compose setup). If a datasource points at `clickhouse:9000`, that matches this Service.

- [ ] **Step 5: Write the Redpanda Console Deployment**

Create `deploy/k8s/chart/templates/console.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: console
  labels: { app: console }
spec:
  selector: { app: console }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: console
spec:
  replicas: 1
  selector: { matchLabels: { app: console } }
  template:
    metadata:
      labels: { app: console }
    spec:
      containers:
        - name: console
          image: redpandadata/console:v2.7.2
          env:
            - { name: KAFKA_BROKERS, value: "{{ .Values.kafka.brokers }}" }
          ports:
            - { containerPort: 8080, name: http }
```

- [ ] **Step 6: Lint and render**

Run: `helm lint deploy/k8s/chart && helm template wsl deploy/k8s/chart | kubectl apply --dry-run=client -f -`
Expected: ConfigMaps carry the init.sql / kafka.xml / grafana files inline; clickhouse, grafana, console validate.

- [ ] **Step 7: Live verify on kind**

```bash
helm upgrade wsl deploy/k8s/chart
kubectl rollout status statefulset/clickhouse --timeout=180s
kubectl rollout status deploy/grafana --timeout=120s
kubectl exec statefulset/clickhouse -- clickhouse-client -q \
  "SELECT count() FROM wiki.recentchange_validated"   # adjust to actual table name in init.sql
kubectl port-forward svc/grafana 3000:3000 &
curl -s localhost:3000/api/health
```
Expected: clickhouse table populated (non-zero after a minute); Grafana `/api/health` returns `"database": "ok"`. Paste real output.

- [ ] **Step 8: Commit (only if Mike asks)**

```bash
git add deploy/k8s/chart/files deploy/k8s/chart/templates/clickhouse-config.yaml deploy/k8s/chart/templates/clickhouse.yaml deploy/k8s/chart/templates/grafana.yaml deploy/k8s/chart/templates/console.yaml
git commit -m "feat(k8s): clickhouse + grafana + console with provisioning configmaps"
```

- [ ] **Step 9: STOP — hand off PR-k4 for review. Do not start Task 5 until Mike approves.**

---

### Task 5 (PR-k5): Kustomize overlays + ingress + tools + scaling demo docs

**Files:**
- Create: `deploy/k8s/chart/templates/duckdb-ui.yaml`
- Create: `deploy/k8s/overlays/local/kustomization.yaml`
- Create: `deploy/k8s/overlays/local/kustomize.sh`
- Create: `deploy/k8s/overlays/cloud/kustomization.yaml`
- Create: `deploy/k8s/overlays/cloud/kustomize.sh`
- Create: `deploy/k8s/overlays/cloud/ingress.yaml`
- Modify: `deploy/k8s/README.md` (add scaling demo + cloud deploy sections)

**Interfaces:**
- Consumes: all Services from Tasks 1–4; `.Values.tools.enabled`.
- Produces: post-renderer scripts; cloud Ingress; documented scale-out demo.

- [ ] **Step 1: Write the duckdb-ui Deployment (tools-gated)**

Create `deploy/k8s/chart/templates/duckdb-ui.yaml`:

```yaml
{{- if .Values.tools.enabled }}
apiVersion: v1
kind: Service
metadata:
  name: duckdb-ui
  labels: { app: duckdb-ui }
spec:
  selector: { app: duckdb-ui }
  ports:
    - { name: http, port: 4213, targetPort: 4213 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: duckdb-ui
spec:
  replicas: 1
  selector: { matchLabels: { app: duckdb-ui } }
  template:
    metadata:
      labels: { app: duckdb-ui }
    spec:
      containers:
        - name: duckdb-ui
          image: wiki-stream-lab-duckdb-ui:local   # built from deploy/duckdb-ui
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          env:
            - { name: HOME, value: "/duckhome" }
          ports:
            - { containerPort: 4213, name: http }
{{- end }}
```
Note: the duckdb-ui image must also be `kind load`ed (`podman build -t wiki-stream-lab-duckdb-ui:local deploy/duckdb-ui && kind load docker-image wiki-stream-lab-duckdb-ui:local`). Document it.

- [ ] **Step 2: Write the local overlay post-renderer**

Create `deploy/k8s/overlays/local/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - all.yaml
# local (kind): no patches needed — default StorageClass, access via port-forward.
```

Create `deploy/k8s/overlays/local/kustomize.sh` (chmod +x):

```sh
#!/usr/bin/env sh
# Helm post-renderer: receives rendered manifests on stdin, emits final on stdout.
set -e
dir="$(cd "$(dirname "$0")" && pwd)"
cat > "$dir/all.yaml"
kustomize build "$dir"
rm -f "$dir/all.yaml"
```

- [ ] **Step 3: Write the cloud overlay + ingress**

Create `deploy/k8s/overlays/cloud/ingress.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: wsl-uis
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  rules:
    - host: wsl.example.com         # replace per environment
      http:
        paths:
          - { path: /grafana, pathType: Prefix, backend: { service: { name: grafana, port: { number: 3000 } } } }
          - { path: /console, pathType: Prefix, backend: { service: { name: console, port: { number: 8080 } } } }
```

Create `deploy/k8s/overlays/cloud/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - all.yaml
  - ingress.yaml
# Cloud: set a real StorageClass via values (--set storage.className=gp3),
# and supply the S3 secret externally (--set s3.secretKey=... or a sealed secret).
```

Create `deploy/k8s/overlays/cloud/kustomize.sh` (chmod +x): same body as local's `kustomize.sh` but pointing at the cloud dir.

```sh
#!/usr/bin/env sh
set -e
dir="$(cd "$(dirname "$0")" && pwd)"
cat > "$dir/all.yaml"
kustomize build "$dir"
rm -f "$dir/all.yaml"
```

- [ ] **Step 4: Document the scaling demo + cloud deploy in README**

Modify `deploy/k8s/README.md` — append:

```markdown
## Consumer-group scaling demo (the point of the k8s route)

`wikimedia.recentchange.validated` has 6 partitions. Scaling the `laker`
Deployment adds pods to the consumer group `lake-parquet`; Redpanda rebalances
the 6 partitions across the live pods.

    # baseline: 1 pod owns all 6 partitions
    kubectl run lag --rm -it --image=wiki-stream-lab:local --restart=Never -- \
      /app/cli lag lake-parquet wikimedia.recentchange.validated

    # scale out — watch pods land (possibly on different worker nodes)
    kubectl scale deploy/laker --replicas=4
    kubectl get pods -l app=laker -o wide -w

    # re-check lag: partitions now spread, total lag drains faster
    kubectl run lag --rm -it --image=wiki-stream-lab:local --restart=Never -- \
      /app/cli lag lake-parquet wikimedia.recentchange.validated

Induce lag first with `--set slowConsumerMs=500` on the projector to make the
drain visible.

## Cloud deploy

    helm install wsl deploy/k8s/chart \
      --post-renderer deploy/k8s/overlays/cloud/kustomize.sh \
      --set image.repository=REGISTRY/wiki-stream-lab \
      --set image.tag=GIT_SHA \
      --set storage.className=gp3 \
      --set s3.accessKey=... --set s3.secretKey=...

Edit `overlays/cloud/ingress.yaml` host before installing.

## Tools (DuckDB web UI)

    podman build -t wiki-stream-lab-duckdb-ui:local deploy/duckdb-ui
    kind load docker-image wiki-stream-lab-duckdb-ui:local
    helm upgrade wsl deploy/k8s/chart --set tools.enabled=true \
      --post-renderer deploy/k8s/overlays/local/kustomize.sh
    kubectl port-forward svc/duckdb-ui 4213:4213
```

- [ ] **Step 5: Verify both overlays render**

```bash
chmod +x deploy/k8s/overlays/local/kustomize.sh deploy/k8s/overlays/cloud/kustomize.sh
helm template wsl deploy/k8s/chart --post-renderer deploy/k8s/overlays/local/kustomize.sh | head
helm template wsl deploy/k8s/chart --post-renderer deploy/k8s/overlays/cloud/kustomize.sh | grep -A2 "kind: Ingress"
helm template wsl deploy/k8s/chart --set tools.enabled=true | grep duckdb-ui
```
Expected: local renders cleanly; cloud render includes the Ingress; `tools.enabled=true` includes duckdb-ui; default (false) omits it.

- [ ] **Step 6: Live verify the scaling demo on kind**

```bash
helm upgrade wsl deploy/k8s/chart --post-renderer deploy/k8s/overlays/local/kustomize.sh
kubectl scale deploy/laker --replicas=4
kubectl get pods -l app=laker -o wide
kubectl run lag --rm -it --image=wiki-stream-lab:local --restart=Never -- \
  /app/cli lag lake-parquet wikimedia.recentchange.validated
```
Expected: 4 laker pods Running across nodes; `cli lag` shows the 6 partitions' committed offsets advancing across the group. Paste real output — this is the headline evidence.

- [ ] **Step 7: Commit (only if Mike asks)**

```bash
git add deploy/k8s/chart/templates/duckdb-ui.yaml deploy/k8s/overlays deploy/k8s/README.md
git commit -m "feat(k8s): kustomize overlays, ingress, tools, scaling demo docs"
```

- [ ] **Step 8: STOP — hand off PR-k5 for review. Migration complete; Compose remains intact.**

---

## Self-Review

**Spec coverage:** redpanda (T1), topics-init hook (T1), producer/validator/projector+sidecar (T2), rustfs/secret/archiver/laker (T3), clickhouse/grafana/console + configmaps (T4), Helm+Kustomize post-renderer overlays / ingress / tools.enabled / scaling demo (T5), kind multi-node (T1), image load flow (T1/T5), networking via Services (all), no-secrets via Secret + external ref (T3/T5). All spec sections mapped.

**Placeholder scan:** No TBD/TODO. Two spots require the implementer to confirm an existing value against real files — the ClickHouse table name in T4 Step 7 (`adjust to actual table name in init.sql`) and the grafana dashboard provider path (T4 Step 4) — both flagged explicitly with how to confirm, not left vague.

**Type/name consistency:** Service names (`redpanda`, `rustfs`, `clickhouse`, `grafana`, `console`, `sqlite-web`, `duckdb-ui`) match across producer/consumer env vars and port-forwards. `KAFKA_BROKERS=redpanda:9092`, `S3_ENDPOINT=http://rustfs:9000`, Secret `s3-creds` keys `accessKey`/`secretKey`, helper `wsl.image`/`wsl.storageClass` used consistently. Consumer group names (`lake-parquet`, `archiver-raw`) referenced in demo match the apps' existing groups — implementer confirms against source in T3/T5.
