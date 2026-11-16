# A derived image the fyrr image plus provider wheels, installed with
# pip; fyrr itself is not rebuilt. Build with the wheels as the context:
#   docker build -f examples/derived-image/Dockerfile \
#     --build-arg FYRR_IMAGE=fyrr:<tag> -t my-fyrr target/example-wheels
ARG FYRR_IMAGE=fyrr:local
FROM ${FYRR_IMAGE}
USER root
RUN --mount=type=bind,source=.,target=/wheels \
    pip install --no-cache-dir --no-index --disable-pip-version-check /wheels/*.whl
USER 10001:10001
