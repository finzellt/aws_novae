#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_sf.sh [CANDIDATE_NAME]
  run_sf.sh --u EXEC_ARN

Description:
  • Without --u:
      - Finds the State Machine ARN from CloudFormation stack "nova-ingest-dev"
      - Starts an execution with candidate_name set to CANDIDATE_NAME (default: "V1324 Sco")
      - Describes the execution started
  • With --u EXEC_ARN:
      - Only runs: aws stepfunctions describe-execution --execution-arn EXEC_ARN

Examples:
  ./run_sf.sh "M81N 2006-02b"
  ./run_sf.sh
  ./run_sf.sh --u arn:aws:states:us-east-1:123456789012:execution:my-sm:my-exec
EOF
}

# If user passed --help or -h
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

# If user passed the --u mode: only run the last line with given EXEC_ARN
if [[ "${1:-}" == "--u" ]]; then
  if [[ $# -lt 2 ]]; then
    echo "Error: --u requires an EXEC_ARN argument." >&2
    echo
    usage
    exit 1
  fi
  EXEC_ARN="$2"
  echo "Describing execution: $EXEC_ARN"
  aws stepfunctions describe-execution --execution-arn "$EXEC_ARN"
  exit 0
fi

# Otherwise, normal mode: candidate name is positional (default is V1324 Sco)
CANDIDATE="${1:-V1324 Sco}"

# Helper to build JSON input safely; uses jq if available, falls back otherwise.
json_input() {
  local candidate="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n --arg c "$candidate" '{candidate_name:$c}'
  else
    # Naive fallback (OK for simple names without quotes/backslashes)
    printf '{"candidate_name":"%s"}' "$candidate"
  fi
}

echo "Resolving Step Functions state machine ARN from CloudFormation stack: nova-ingest-dev"
SM_ARN=$(aws cloudformation describe-stack-resources \
  --stack-name nova-ingest-dev \
  --query "StackResources[?ResourceType=='AWS::StepFunctions::StateMachine'].PhysicalResourceId" \
  --output text)

if [[ -z "$SM_ARN" || "$SM_ARN" == "None" ]]; then
  echo "Error: Could not resolve State Machine ARN from stack 'nova-ingest-dev'." >&2
  exit 2
fi

echo "Starting execution for candidate: $CANDIDATE"
EXEC_ARN=$(aws stepfunctions start-execution \
  --state-machine-arn "$SM_ARN" \
  --input "$(json_input "$CANDIDATE")" \
  --query executionArn --output text)

echo "Execution started: $EXEC_ARN"
sleep 3
echo "Describing execution..."
aws stepfunctions describe-execution --execution-arn "$EXEC_ARN"
