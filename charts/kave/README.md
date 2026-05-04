### Local Development (Kind)

```bash
kind create cluster
make test-smoke IMAGE_TAG=dev
```

### Production (Civo / GKE / AKS)

```bash
IMAGE_TAG=$(git rev-parse --short HEAD)
yq eval ".appVersion = \"$IMAGE_TAG\"" -i charts/kave/Chart.yaml

helm upgrade --install kave ./charts/kave \
  --namespace kave --create-namespace \
  --set image.tag=$IMAGE_TAG \
  --set config.adminAuthToken=$(openssl rand -hex 16) \
  --wait --timeout 15m
```

## NOTES

### Image tag must be explicit

`image.tag` defaults to `""` in `values.yaml`.
This prevents accidental `:latest` deploys where the binary could differ on each restart.

### Chart.yaml appVersion is patched by CI

The `appVersion` field in `Chart.yaml` is empty by default. The CI pipeline patches it with `yq` before `helm upgrade` so that `helm list`, kubectl annotations, and the post-install NOTES all show the correct git SHA. This is not a standard Helm pattern — the value is intentionally mutated in-place during deploy.

### Admin token validation

If `config.adminAuthToken` is left at its default `"-"`, the chart refuses to install. This prevents silent unauthenticated deploys. CI always overrides this via `--set`.

### Voters must be odd

`voters.replicas` must be an odd number. Raft requires this to avoid split-brain. The chart fails on even values:

```
voters.replicas must be odd for Raft quorum
```

### Raft bootstrap uses publishNotReadyAddresses

The headless service sets `publishNotReadyAddresses: true`. Without this, K8s only adds pods to DNS once their readiness probe passes. But readiness requires a leader, and a leader requires peers to find each other via DNS first — a catch-22. This setting means pods appear in DNS immediately.

### Raft mTLS is fully automated

Cert-manager creates the entire TLS chain automatically:
1. Self-signed issuer → internal CA → wildcard node cert
2. Each pod mounts the cert via the `kave-raft-tls` Secret
3. No manual cert management needed

### Let's Encrypt (Traefik path)

When `traefik.enabled=true`, a Let's Encrypt ClusterIssuer is created with DNS-01 challenge via Cloudflare. This requires a pre-created `cloudflare-api-token` Secret in the namespace:

```bash
kubectl create secret generic cloudflare-api-token \
  -n kave --from-literal=api-token=<YOUR_CF_TOKEN>
```

For local Kind development, Traefik is disabled by default so this isn't needed.

### Init container has no resource limits

The `wait-for-dns` busybox init container has no resource requests/limits. This is only an issue on clusters with strict LimitRange policies.
