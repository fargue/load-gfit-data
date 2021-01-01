authToken=$1

curl -X POST \
	-H "Content-Type: application/json" \
	-H "Authorization: Bearer ${authToken}" \
	-d '{ "aggregateBy": [ { "dataTypeName": "com.google.sleep.segment" } ], "endTimeMillis": "1606820460000", "startTimeMillis": "1606795020000" }' \
	https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate
