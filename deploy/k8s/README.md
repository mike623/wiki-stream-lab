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
