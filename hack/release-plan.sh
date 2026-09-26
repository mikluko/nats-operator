#!/bin/sh
# Decides from mikluko/action-changelog's outputs whether a release is due.
#
# Reads VALID, VERSION, TAGGED and PRERELEASE (the action's valid, version,
# already-tagged and prerelease) and appends to $GITHUB_OUTPUT either due=false,
# or due=true with version, tag (v<version>) and prerelease. An invalid
# changelog or a missing input fails and writes nothing.
set -eu

: "${GITHUB_OUTPUT:?}"
valid=${VALID?VALID is unset}
version=${VERSION?VERSION is unset}
tagged=${TAGGED?TAGGED is unset}
prerelease=${PRERELEASE?PRERELEASE is unset}

if [ "$valid" != true ]; then
    echo "::error::CHANGELOG.md is not valid; nothing is released from it" >&2
    exit 1
fi

if [ -z "$version" ] || [ "$tagged" != false ]; then
    echo "nothing to release: version=${version:-none} already-tagged=$tagged"
    echo "due=false" >>"$GITHUB_OUTPUT"
    exit 0
fi

echo "releasing v$version (prerelease=$prerelease)"
{
    echo "due=true"
    echo "version=$version"
    echo "tag=v$version"
    echo "prerelease=$prerelease"
} >>"$GITHUB_OUTPUT"
