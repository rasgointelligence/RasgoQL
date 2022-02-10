#!/bin/sh

set -e

if [ -d ~/virtenv/bin/activate ]; then
    source ~/virtenv/bin/activate;
fi

if [ -f ~/.pypirc.template ]; then
    envsubst '${PYPI_USER} ${PYPI_PASSWORD} ${PYPITEST_USER} ${PYPITEST_PASSWORD}' < ~/.pypirc.template > ~/.pypirc
fi
exec $*;
