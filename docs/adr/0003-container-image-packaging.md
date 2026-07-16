# Package the Lambda as a container image

The dependency set includes four native-compiled wheels (`pyiceberg`, `fastavro`,
`pydantic-core`, `orjson`) and the unzipped bundle approaches Lambda's 250 MB zip ceiling.
We package the function as a **container image** `FROM public.ecr.aws/lambda/python:3.12`
targeting **x86_64**, installing dependencies from a **`uv export`-generated
`requirements.txt`**, and push it to ECR. This sidesteps cross-building manylinux wheels on
macOS (the dev platform) and removes the zip size cap. The team is already container-native
(EKS/Docker), so this adds no new operational concept.

## Considered Options

- **Container image from the AWS base image** (chosen) — native wheels resolve for the correct platform automatically; 10 GB image limit.
- **Zip + Lambda layer** — needs a separate manylinux layer build and risks the 250 MB unzipped cap with `pyiceberg`.
- **Single cross-built zip** — simplest mentally, most fragile: must reproduce Lambda's platform exactly and stay under the size cap.

## Consequences

Dependencies are pinned via `uv export` (respecting `uv.lock`) rather than `pip install .`,
keeping the image build reproducible against the lockfile. Arch is x86_64 by request, so
image builds on Apple-Silicon dev machines cross-compile.
