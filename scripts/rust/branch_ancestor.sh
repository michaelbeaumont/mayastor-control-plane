#!/usr/bin/env bash

die()
{
  local _return="${2:-1}"
  echo "$1" >&2
  exit "${_return}"
}

SCRIPTDIR="$(dirname "$(realpath "${BASH_SOURCE[0]:-"$0"}")")"
ROOTDIR="$SCRIPTDIR/../.."
TEST_CONSTANTS="$ROOTDIR/utils/utils-lib/src/test_constants.rs"

set -euo pipefail

if [ ! -d "$ROOTDIR/.git" ]; then
  [ -f "$TEST_CONSTANTS" ] && exit 0 || die "$TEST_CONSTANTS not present"
fi

BRANCH=${BRANCH:-$(git branch --show-current)}
if [[ "$BRANCH" =~ ^release/.* ]] || [ "$BRANCH" = "develop" ]; then
  BRANCH="$BRANCH"
else
  source "$ROOTDIR/scripts/git/branch_ancestor.sh"

  BRANCH="$(findBranchesSharingFirstCommonCommit . origin/develop$ origin/release/ | cut -d' ' -f1)"
  BRANCH=${BRANCH##origin/}

  if [ "$BRANCH" == "" ]; then
    BRANCH="develop"
  fi
fi

REGISTRY=${TARGET_REGISTRY:-}
if [ -z "$REGISTRY" ]; then
  REGISTRY="$("$ROOTDIR/utils/dependencies/scripts/git-org-name.sh" -r origin)"
fi

CONTENT=$(cat <<EOF
/// Automatically generated by ROOT/scripts/rust/branch_ancestor.sh
pub(crate) const TARGET_BRANCH: &str = "$BRANCH";
pub(crate) const TARGET_REGISTRY: &str = "${REGISTRY,,}";
EOF
)

if [ "$CONTENT" != "$(cat $TEST_CONSTANTS 2>/dev/null)" ]; then
  echo "Writing To $TEST_CONSTANTS"
  echo "$CONTENT"
  echo "$CONTENT" > $TEST_CONSTANTS
fi
