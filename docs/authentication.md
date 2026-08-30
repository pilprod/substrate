# ate-api Authentication

`ate-api` accepts mTLS client certificates and bearer JWTs. JWT providers are
configured with the file passed to `--authentication-config`:

```yaml
actorIdentityJWTProvider: kubernetes
externalProviderEnrollmentAdmins:
- provider: kubernetes
  subjects:
  - system:serviceaccount:ate-system:ate-client
jwtProviders:
- name: kubernetes
  issuer: https://container.googleapis.com/v1/projects/PROJECT_ID/locations/LOCATION/clusters/CLUSTER_NAME
  audiences:
  - api.ate-system.svc
  discoveryURL: https://kubernetes.default.svc/.well-known/openid-configuration
  jwksURL: https://kubernetes.default.svc/openid/v1/jwks
  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
- name: google
  issuer: https://accounts.google.com
  audiences:
  - 32555940559.apps.googleusercontent.com
```

Provider names and issuers must be unique. `issuer` must be an HTTPS URL and
`audiences` must be non-empty; a token is accepted when any configured audience
matches. `certificateAuthorityFile` and `discoveryTokenFile` are optional and
are needed for OIDC discovery against some private Kubernetes API servers.

`discoveryURL` and `jwksURL` are an optional pair. Configure both or neither;
each must be an absolute HTTPS URL without userinfo, query, or fragment. The
pair lets a verifier keep the token's real issuer and audience while fetching
metadata and signing keys through a different trusted network path. The
discovery document's `issuer` must still exactly match `issuer`. When `jwksURL`
is configured, it replaces the document's `jwks_uri`; a document cannot redirect
key retrieval to another host. A discovery bearer token is sent only to URLs
within the configured issuer path or to the two exact override URLs, and the
token file is read again for every request so projected ServiceAccount token
rotation is honored.

### GKE Kubernetes ServiceAccount tokens

GKE ServiceAccount tokens use a cluster-specific issuer under
`https://container.googleapis.com/`. Use that value as `issuer`, but set the
paired overrides to the in-cluster Kubernetes API URLs shown above. This keeps
signature discovery inside the cluster and avoids adding public Google API
egress solely for JWT verification. The Kubernetes ServiceAccount CA and token
files authenticate those internal discovery requests.

OIDC keys are loaded lazily on the first bearer-token request. A healthy
`/readyz` response therefore does not prove that discovery, JWKS retrieval, or
the configured audience works. A release smoke test must mint a fresh token
for the expected audience and make an authenticated API call, for example:

```sh
kubectl -n ate-system create token ate-client \
  --audience=api.ate-system.svc \
  --duration=10m \
  | kubectl ate --token-file=- get atespaces
```

`actorIdentityJWTProvider` identifies the provider allowed to call
`ActorIdentity.MintJWT`. General authorization and RBAC are not implemented
yet, so only configure providers whose users should have full control-plane
access, including `DebugClear`.

`externalProviderEnrollmentAdmins` is a narrow exception: it authorizes only
`ExternalProviderAdmin.CreateExternalProviderEnrollment` and
`ExternalProviderAdmin.RevokeExternalProviderRegistration`, and only for an
exact tuple of configured provider name, that provider's verified issuer, and
JWT `sub`. An omitted or empty list authorizes nobody. mTLS identities are not
implicitly external-provider administrators. The default installation helper
grants the `ate-client` ServiceAccount because `kubectl-ate` mints a short-lived
token for that exact subject; Kubernetes RBAC still controls who may request
that ServiceAccount token.

## Google Cloud CLI tokens

The Google Cloud CLI currently issues user identity tokens with issuer
`https://accounts.google.com` and audience
`32555940559.apps.googleusercontent.com`, the Cloud SDK's shared client ID.
These values are examples rather than built-in defaults; verify the claims
issued by your identity provider and configure them explicitly.

With the provider configured, pipe the token to `kubectl-ate`:

```sh
gcloud auth print-identity-token | kubectl ate --token-file=- get actors
```

`--token-file` accepts either a file path or `-` for stdin and only replaces the
credential sent to `ate-api`. `kubectl-ate`
still uses kubeconfig access to establish its port-forward and obtain the
server trust bundle.

For a manifest-based installation, replace the authentication ConfigMap and
restart the deployment:

```sh
kubectl -n ate-system create configmap ate-api-authentication \
  --from-file=authentication.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ate-system rollout restart deployment/ate-api-server
```

Configuration is read at process startup. Restart `ate-api` pods after changing
the ConfigMap. OIDC signing keys are cached and refreshed when an unknown key ID
is encountered.
