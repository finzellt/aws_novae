# # Start an execution
# EXEC_ARN=$(aws stepfunctions start-execution \
#   --state-machine-arn <YOUR_SFN_ARN> \
#   --input '{"candidate_name":"V606 Aql"}' \
#   --query executionArn --output text)

# # Wait and fetch the result/history
# aws stepfunctions describe-execution --execution-arn "$EXEC_ARN"
# aws stepfunctions get-execution-history --execution-arn "$EXEC_ARN" --max-results 50

sam deploy --guided

# Start an execution (replace with your State Machine ARN if different)
aws stepfunctions start-execution \
  --state-machine-arn "$(aws cloudformation describe-stack-resources \
       --stack-name nova-ingest \
       --query "StackResources[?LogicalResourceId=='NovaIngestStateMachine'].PhysicalResourceId" \
       --output text)" \
  --input '{"candidate_name":"V606 Aql"}'
