#!/usr/bin/env bash
set -euo pipefail

# -------------------------------
# Config you might tweak
STACK_NAME="nova-ingest-dev"
CAPS="CAPABILITY_IAM CAPABILITY_NAMED_IAM"
REGION_DEFAULT="${AWS_DEFAULT_REGION:-us-east-1}"
# -------------------------------

usage() {
  cat <<'EOF'
Usage:
  run_sf.sh [CANDIDATE_NAME]
  run_sf.sh --u EXEC_ARN
  run_sf.sh [--skip-sam] [--region REGION] [--guided] [CANDIDATE_NAME]

Description:
  • Default mode (no --u):
      - Runs `sam build` and `sam deploy` (unless --skip-sam)
      - Finds the Step Functions State Machine ARN from CloudFormation stack "nova-ingest-dev"
      - Starts an execution with candidate_name set to CANDIDATE_NAME (default: "V1324 Sco")
      - Describes the execution started

  • Update-only mode:
      - --u EXEC_ARN → only runs: aws stepfunctions describe-execution --execution-arn EXEC_ARN

Options:
  --skip-sam        Skip `sam build` and `sam deploy`
  --guided          Use `sam deploy --guided`
  --region REGION   AWS region to use (default: $REGION_DEFAULT)
  -h, --help        Show this help

Examples:
  ./run_sf.sh "M81N 2006-02b"
  ./run_sf.sh
  ./run_sf.sh --u arn:aws:states:us-east-1:123456789012:execution:my-sm:my-exec
  ./run_sf.sh --skip-sam "V1324 Sco"
EOF
}

# Parse args
SKIP_SAM=false
GUIDED=false
REGION="$REGION_DEFAULT"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage; exit 0
fi

if [[ "${1:-}" == "--u" ]]; then
  if [[ $# -lt 2 ]]; then
    echo "Error: --u requires an EXEC_ARN argument." >&2
    echo; usage; exit 1
  fi
  EXEC_ARN="$2"
  echo "Describing execution: $EXEC_ARN"
  aws stepfunctions describe-execution --execution-arn "$EXEC_ARN"
  exit 0
fi

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-sam) SKIP_SAM=true; shift ;;
    --guided)   GUIDED=true; shift ;;
    --region)   REGION="$2"; shift 2 ;;
    *)          ARGS+=("$1"); shift ;;
  esac
done
set -- "${ARGS[@]:-}"

# Candidate (default preserved)
CANDIDATE="${1:-V1324 Sco}"

# --- Helper: JSON input safely via jq if available
json_input() {
  local candidate="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -n --arg c "$candidate" '{candidate_name:$c}'
  else
    printf '{"candidate_name":"%s"}' "$candidate"
  fi
}

# --- New: SAM build & deploy (unless skipped)
if ! $SKIP_SAM; then
  command -v sam >/dev/null 2>&1 || {
    echo "Error: 'sam' CLI not found on PATH. Install AWS SAM CLI or re-run with --skip-sam." >&2
    exit 3
  }
  echo "==> Running: sam build"
  sam build

  echo "==> Running: sam deploy (stack: $STACK_NAME, region: $REGION)"
  if $GUIDED; then
    sam deploy --guided
  else
    sam deploy \
      --stack-name "$STACK_NAME" \
      --region "$REGION" \
      --resolve-s3 \
      --capabilities $CAPS \
      --no-confirm-changeset \
      --no-fail-on-empty-changeset
  fi
else
  echo "==> Skipping SAM build/deploy as requested."
fi

# --- Your original flow continues below ---

echo "Resolving Step Functions state machine ARN from CloudFormation stack: $STACK_NAME"
SM_ARN=$(aws cloudformation describe-stack-resources \
  --region "$REGION" \
  --stack-name "$STACK_NAME" \
  --query "StackResources[?ResourceType=='AWS::StepFunctions::StateMachine'].PhysicalResourceId" \
  --output text)

if [[ -z "$SM_ARN" || "$SM_ARN" == "None" ]]; then
  echo "Error: Could not resolve State Machine ARN from stack '$STACK_NAME' in region '$REGION'." >&2
  exit 2
fi

echo "Starting execution for candidate: $CANDIDATE"
EXEC_ARN=$(aws stepfunctions start-execution \
  --region "$REGION" \
  --state-machine-arn "$SM_ARN" \
  --input "$(json_input "$CANDIDATE")" \
  --query executionArn --output text)

echo "Execution started: $EXEC_ARN"
sleep 21
echo "Describing execution..."
aws stepfunctions describe-execution --region "$REGION" --execution-arn "$EXEC_ARN"
