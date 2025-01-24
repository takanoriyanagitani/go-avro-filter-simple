#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc


export ENV_TARGET_COL_NAME=height
export ENV_TARGET_VALUE=3.776

export ENV_TARGET_COL_NAME=name
export ENV_TARGET_VALUE=fuji

export ENV_TARGET_COL_NAME=active
export ENV_TARGET_VALUE=false

cat ./sample.d/sample.avro |
	./avro-filter-simple |
	rq -aJ |
	jq -c
