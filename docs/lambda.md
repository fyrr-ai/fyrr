# Run the reader on AWS Lambda

> **In short**
>
> - **Experimental.** The recipe was tried by hand through a Lambda Function
>   URL with synthetic identity and embedding services; it is not tested in CI.
> - Use a reader container with the AWS Lambda Web Adapter, S3 read access
>   and a private mirror in `/tmp`.
> - Keep the writer scheduled separately and publish the namespace first.

## Prepare the namespace

Use Linux x86-64 images from the same release. The example uses the `0.1.1`
release image names. Resolve and pin their digests before deploying. Replace
`ACCOUNT_ID`, `KEY_ID`, bucket, prefix, namespace and service URLs throughout.
Use one region, here `eu-central-1`, and a private SSE-S3 bucket.

Publish content and an identity directory binding your OIDC subjects with the
external writer. The reader takes the embedding profile and the retrieval
settings from the namespace; the file supplies the embedding endpoint,
credentials and spend.
See [upgrades](operations.md#upgrades) and [HTTP users](http-users.md).

Save this as `reader.toml` in the container build context. `FUNCTION_URL` is
replaced at startup. Choose provider spend limits for your own workload. Set the AWS CLI region
to `eu-central-1` for the commands below.

```toml
[namespace]
name = "lambda-reader"
[storage]
uri = "s3://example-bucket/fyrr"
region = "eu-central-1"
[embedding]
endpoint = "https://embeddings.example.com/v1"
model = "BAAI/bge-m3"
dimensions = 1024
api_key = { env = "FYRR_EMBEDDING_API_KEY" }
reader_live = true
[embedding.spend]
limit_microusd = 1000000
max_call_microusd = 1000
[identity]
hashing_key = { env = "FYRR_IDENTITY_KEY" }
[identity.oidc]
issuer = "https://issuer.example.com"
audience = "fyrr"
jwks_url = "https://issuer.example.com/jwks"
[reader]
mirror_path = "/tmp/fyrr-mirror"
mirror_max_bytes = 402653184
memory_max_bytes = "768MiB"
memory_headroom_bytes = "64MiB"
concurrency = 1
max_response_bytes = 1048576
[reader.http]
listen = "127.0.0.1:8081"
public_url = "FUNCTION_URL"
```

This file sets no `[identity.directory]`. The reader needs none: it has no
clock and serves the last published directory until a newer publication
replaces it, so the external writer's directory sync decides how fresh it is.

## Container and secret bootstrap

Save this as `Dockerfile`. The matching full image supplies Python's standard
library for secret retrieval. The final image runs the reader executable.

```dockerfile
FROM ghcr.io/fyrr-ai/fyrr:0.1.1 AS runtime
USER root
RUN mkdir -p /runtime/bin /runtime/lib && \
    cp /usr/local/bin/python3.13 /runtime/bin/python3 && \
    cp /usr/local/lib/libpython3.13.so.1.0 /runtime/lib/ && \
    cp -a /usr/local/lib/python3.13 /runtime/lib/ && \
    rm -rf /runtime/lib/python3.13/site-packages
FROM public.ecr.aws/awsguru/aws-lambda-adapter:0.9.1@sha256:46d6625e68cbbdd2efab4a20245977664513f13ffef47915b000d431adcea0b4 AS adapter
FROM ghcr.io/fyrr-ai/fyrr-reader:0.1.1
COPY --from=runtime /runtime /usr/local
COPY --from=runtime /usr/lib/x86_64-linux-gnu/libssl.so.3 /usr/lib/x86_64-linux-gnu/libcrypto.so.3 /usr/lib/x86_64-linux-gnu/libz.so.1 /usr/lib/x86_64-linux-gnu/
COPY --from=adapter /lambda-adapter /opt/extensions/lambda-adapter
COPY reader.toml /app/reader.toml
COPY --chmod=755 bootstrap.py /app/bootstrap.py
ENV AWS_LWA_PORT=8081 AWS_LWA_READINESS_CHECK_PATH=/readyz \
    AWS_LWA_ASYNC_INIT=true AWS_LWA_INVOKE_MODE=buffered \
    AWS_LWA_AUTHORIZATION_SOURCE=x-fyrr-authorization \
    PYTHONDONTWRITEBYTECODE=1 LD_LIBRARY_PATH=/usr/local/lib
USER 65532:65532
WORKDIR /tmp
ENTRYPOINT ["/app/bootstrap.py"]
CMD []
```

Save this as `bootstrap.py`. It retrieves one SecureString using the execution
role's temporary credentials, sets the child environment and starts the reader.

```python
#!/usr/local/bin/python3
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import urllib.request

region = os.environ["AWS_REGION"]
host = f"ssm.{region}.amazonaws.com"
body = json.dumps({"Name": os.environ["FYRR_SECRET_PARAMETER"],
                   "WithDecryption": True}).encode()
now = datetime.datetime.now(datetime.timezone.utc)
date = now.strftime("%Y%m%d")
stamp = now.strftime("%Y%m%dT%H%M%SZ")
headers = {
    "content-type": "application/x-amz-json-1.1",
    "host": host,
    "x-amz-date": stamp,
    "x-amz-security-token": os.environ["AWS_SESSION_TOKEN"],
    "x-amz-target": "AmazonSSM.GetParameter",
}
names = ";".join(sorted(headers))
canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
canonical = f"POST\n/\n\n{canonical_headers}\n{names}\n{hashlib.sha256(body).hexdigest()}"
scope = f"{date}/{region}/ssm/aws4_request"
to_sign = f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
key = ("AWS4" + os.environ["AWS_SECRET_ACCESS_KEY"]).encode()
for part in (date, region, "ssm", "aws4_request"):
    key = hmac.new(key, part.encode(), hashlib.sha256).digest()
signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
headers["authorization"] = (
    f"AWS4-HMAC-SHA256 Credential={os.environ['AWS_ACCESS_KEY_ID']}/{scope}, "
    f"SignedHeaders={names}, Signature={signature}"
)
request = urllib.request.Request(f"https://{host}", body, headers)
with urllib.request.urlopen(request, timeout=10) as response:
    secret = json.loads(json.load(response)["Parameter"]["Value"])
for name in ("FYRR_IDENTITY_KEY", "FYRR_EMBEDDING_API_KEY"):
    os.environ[name] = secret[name]
os.environ["HOME"] = "/tmp"
os.environ["XDG_CACHE_HOME"] = "/tmp/cache"
config = Path("/app/reader.toml").read_text().replace(
    '"FUNCTION_URL"', json.dumps(os.environ["FYRR_PUBLIC_URL"].rstrip("/") + "/mcp")
)
with open("/tmp/fyrr.toml", "w", opener=lambda path, flags: os.open(path, flags, 0o600)) as output:
    output.write(config)
os.execv("/usr/local/bin/fyrr", ["fyrr", "--config", "/tmp/fyrr.toml", "reader", "serve"])
```

Create a SecureString named `/fyrr/lambda-reader` through a protected secret
workflow. Its decrypted JSON contains `FYRR_IDENTITY_KEY` (the writer's
namespace HMAC key) and `FYRR_EMBEDDING_API_KEY`. Keep secret files outside
the build context. Bootstrap fetches the secret on each cold start. Recycle
execution environments after rotating it.

## IAM and image access

Create the execution role with this trust policy.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
```

Pre-create `/aws/lambda/fyrr-reader` with your log retention policy. Attach
this execution policy, substituting the actual SSM KMS key ARN for `KEY_ID`.
For a customer-managed key, its key policy must also permit decryption.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow", "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::example-bucket/fyrr/namespaces/lambda-reader/*"
    },
    {
      "Effect": "Allow", "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::example-bucket",
      "Condition": {"StringLike": {"s3:prefix": ["fyrr/namespaces/lambda-reader", "fyrr/namespaces/lambda-reader/*"]}}
    },
    {
      "Effect": "Allow", "Action": "ssm:GetParameter",
      "Resource": "arn:aws:ssm:eu-central-1:ACCOUNT_ID:parameter/fyrr/lambda-reader"
    },
    {
      "Effect": "Allow", "Action": "kms:Decrypt",
      "Resource": "arn:aws:kms:eu-central-1:ACCOUNT_ID:key/KEY_ID",
      "Condition": {"StringEquals": {
        "kms:ViaService": "ssm.eu-central-1.amazonaws.com",
        "kms:EncryptionContext:PARAMETER_ARN": "arn:aws:ssm:eu-central-1:ACCOUNT_ID:parameter/fyrr/lambda-reader"
      }}
    },
    {
      "Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:eu-central-1:ACCOUNT_ID:log-group:/aws/lambda/fyrr-reader:log-stream:*"
    }
  ]
}
```

The reader uses object GETs, range GETs, HEADs and prefix listings.
`s3:GetObject` covers HEAD and conditional reads as well as GET, while
`s3:ListBucket` covers listings. This policy assumes SSE-S3 content.
SSE-KMS content also requires decrypt access to its own key.
[AWS S3 permission mapping](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html).

Push a single-platform `linux/amd64` image to ECR in the function's region.
Use this repository policy for Lambda image retrieval.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
    "Condition": {"ArnLike": {
      "aws:sourceArn": "arn:aws:lambda:eu-central-1:ACCOUNT_ID:function:fyrr-reader"
    }}
  }]
}
```

The deployment identity needs separate permissions to push images, create
resources and pass the execution role.

## Create the function and URL

Create a container-image function named `fyrr-reader` using the ECR image.
Use these settings and replace the two deployment environment values.

| Setting | Value |
| --- | --- |
| Architecture | `x86_64` |
| Memory | 1024 MB (1 GiB) |
| Timeout | 60 seconds |
| Ephemeral storage | 512 MiB |
| Reserved concurrency | 2 |
| `FYRR_SECRET_PARAMETER` | `/fyrr/lambda-reader` |
| `FYRR_PUBLIC_URL` | Initially `https://placeholder.example.com/` |

Lambda supplies `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
and `AWS_SESSION_TOKEN`. Create the URL and set the concurrency limit.

```sh
aws lambda create-function-url-config --function-name fyrr-reader --auth-type AWS_IAM --invoke-mode BUFFERED
aws lambda put-function-concurrency --function-name fyrr-reader --reserved-concurrent-executions 2
```

Before invoking, replace `FYRR_PUBLIC_URL` with the returned Function URL,
preserving all other environment entries, and wait for the configuration
update to finish. Bootstrap forms the exact `/mcp` URL so the forwarded
`Host` matches the reader's configuration.

Callers need `lambda:InvokeFunctionUrl` and `lambda:InvokeFunction` scoped
to this function. They sign requests with SigV4 for service `lambda` and
send their OIDC token in `x-fyrr-authorization: Bearer <token>`. The adapter
maps that header to application authentication. MCP clients need this
signing path. See [AWS Function URL authentication](https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html).

## Limitations

- **Experimental and outside CI.** The trial used synthetic services.
  External identity and embedding services need deployment validation.
- Cold starts retrieve secrets and prepare the publication. Each execution
  environment has its own `/tmp` mirror, rebuilt on a fresh cold start.
- Lambda handles one invocation per execution environment. Additional
  environments create independent mirrors, caches and process-level rate
  and spend limits. Reserved concurrency throttles excess invocations with 429.
- Embedding-provider latency adds to searches requiring a live embedding.
  Query cache hits can reuse an embedding within an environment.
- The trial successfully refreshed JWKS after idle reuse. Validate your
  issuer's key rotation and availability across idle periods.
- A frozen environment runs no background version check. The reader notices
  that it did not run and, when a check is due
  (`reader.version_check_interval`, 30 s), runs it before it answers the
  request that woke it; the request waits up to 2 s for it. An answer can
  therefore lag a publication by up to one interval, as on any other host,
  or longer when the check takes more than 2 s; once the reader has read
  the newer head, such answers are flagged `stale_version`.
  Lambda provides no operator SIGHUP path. The reader's
  `/admin/refresh` needs a configured operator credential and an active
  invocation, including the adapter's authorization-header mapping.
- Keep the writer as a normal scheduled job publishing to S3. The trial
  covered finite buffered MCP calls. Long-lived SSE and response streaming
  require separate validation.
