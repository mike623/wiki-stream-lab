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
