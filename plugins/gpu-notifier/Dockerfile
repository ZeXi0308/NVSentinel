# Copyright (c) 2026, Meshy. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build with `make local-build` to read tool versions from .versions.yaml.
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim AS build
ARG POETRY_VERSION
RUN test -n "$POETRY_VERSION" && pip install --no-cache-dir "poetry==$POETRY_VERSION"

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN poetry sync --only main --no-root

FROM python:${PYTHON_VERSION}-slim AS runtime
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY app ./app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
USER 65532:65532
EXPOSE 8080
CMD ["/app/.venv/bin/python", "-m", "app.gpunotify", "run", "--send"]
