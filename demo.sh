#!/usr/bin/env bash

BIS_FILE=$1
if [ -z "$BIS_FILE" ]
then
    BIS_FILE="$(pwd)/yelp_academic_dataset_business.json.zip"
fi

REVIEW_FILE=$2
if [ -z "$REVIEW_FILE" ]
then
    REVIEW_FILE="$(pwd)/yelp_academic_dataset_review.json.zip"
fi

echo "using  $BIS_FILE $REVIEW_FILE"

docker run -it \
    -v "$BIS_FILE:/go/data/yelp_academic_dataset_business.json.zip" \
    -v "$REVIEW_FILE:/go/data/yelp_academic_dataset_review.json.zip" \
    reviews ./feeder 100
