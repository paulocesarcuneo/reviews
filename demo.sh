#!/usr/bin/env bash

NETWORK=$1
if [ -z "$NETWORK" ]
then
    NETWORK="reviews_default"
fi
BIS_FILE=$2
if [ -z "$BIS_FILE" ]
then
    BIS_FILE="$(pwd)/yelp_academic_dataset_business.json.zip"
fi

REVIEW_FILE=$3
if [ -z "$REVIEW_FILE" ]
then
    REVIEW_FILE="$(pwd)/yelp_academic_dataset_review.json.zip"
fi

echo "using  $BIS_FILE $REVIEW_FILE"

docker run -it \
    --network="$NETWORK" \
    -v "$BIS_FILE:/go/yelp_academic_dataset_business.json.zip" \
    -v "$REVIEW_FILE:/go/yelp_academic_dataset_review.json.zip" \
    reviews ./feeder 100
