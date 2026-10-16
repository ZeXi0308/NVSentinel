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

.DEFAULT_GOAL := lint-test
IS_GO_MODULE := 0
HAS_DOCKER := 1
PYTHON_PACKAGE_NAME := app.gpunotify

include ../../make/common.mk

POETRY ?= poetry
PYTHON_VERSION := $(call get-version,languages.python)
POETRY_VERSION := $(call get-version,build_tools.poetry)
BLACK_VERSION := $(call get-version,linting.black)
IMAGE ?= meshy-gpu-notifier:local
PLATFORM ?= linux/amd64

.PHONY: all setup lint format test coverage lint-test ci-test build local-build docker-build clean
all: lint-test

setup:
	POETRY_VIRTUALENVS_IN_PROJECT=true $(POETRY) sync

lint: setup
	@test -n "$(BLACK_VERSION)" || (echo "Install yq to read .versions.yaml"; exit 1)
	$(POETRY) run python -c 'from importlib.metadata import version; assert version("black") == "$(BLACK_VERSION)", "Black must match .versions.yaml; update the module lock file"'
	$(POETRY) run black --check app tests

format: setup
	$(POETRY) run black app tests

test: setup
	$(POETRY) run coverage run --source=$(PYTHON_PACKAGE_NAME) -m pytest -vv --junitxml=report.xml

coverage: test
	$(POETRY) run coverage report
	$(POETRY) run coverage xml

lint-test: lint coverage
ci-test: lint-test

build: setup
	$(POETRY) build

local-build:
	@test -n "$(PYTHON_VERSION)" -a -n "$(POETRY_VERSION)" || (echo "Install yq to read .versions.yaml"; exit 1)
	docker build --platform $(PLATFORM) \
		--build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
		--build-arg POETRY_VERSION=$(POETRY_VERSION) \
		-t $(IMAGE) .

docker-build: local-build

clean:
	rm -f .coverage coverage.xml report.xml
	rm -rf dist/ build/
