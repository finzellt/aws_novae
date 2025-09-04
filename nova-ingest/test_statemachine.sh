# # Start Lambda emulator in one terminal
# sam local start-lambda --port 3001

# # Start SFN Local in another (point it at Lambda endpoint)
# java -jar StepFunctionsLocal.jar --lambda-endpoint http://localhost:3001

# Substitute placeholders with local names and create the state machine
export ResolveSimbadArn=ResolveSimbadMetadataFunction
export ValidateArn=ValidateNovaAndCoordsFunction
export DetermineHostArn=DetermineHostGalaxyFunction
envsubst < statemachines/nova_ingest.asl.json > /tmp/nova_ingest.local.asl.json

aws stepfunctions --endpoint-url http://localhost:8083 create-state-machine \
  --name nova-ingest-local \
  --definition file:///tmp/nova_ingest.local.asl.json \
  --role-arn arn:aws:iam::123456789012:role/DummyRole
