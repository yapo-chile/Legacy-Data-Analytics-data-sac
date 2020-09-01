#!/usr/bin/env bash
export UNAMESTR=$(uname)

# GIT variables
export GIT_BRANCH=$(shell git name-rev --name-only HEAD | sed "s/~.*//" | sed 's/\//_/;')
export GIT_COMMIT=$(shell git rev-parse HEAD)
export GIT_COMMIT_SHORT=$(shell git rev-parse --short HEAD)
export GIT_TAG=$(shell git tag -l --points-at HEAD | tr '\n' '_' | sed 's/_$$//;')
export BUILD_CREATOR=$(shell git log --format=format:%ae | head -n 1)
export GIT_BRANCH_LOWERCASE=$(shell echo "${GIT_BRANCH}" | sed 's/\//_/;')

export SERVER_ROOT=${PWD}

#DOCKER variables
export DOCKER_REGISTRY=containers.mpi-internal.com

export BUILD_NAME=$(shell if [ -n "${GIT_TAG}" ]; then echo "${GIT_TAG}"; else echo "${GIT_BRANCH}"; fi;)
export BUILD_TAG=$(shell echo "${BUILD_NAME}" | tr '[:upper:]' '[:lower:]' | sed 's,/,_,g')

# TRAVIS variables
export REPORT_ARTIFACTS=reports