#!/usr/bin/env bash
set -x

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
    reviews ./feeder 100 business yelp_academic_dataset_business.json.zip

docker run -it \
    --network="$NETWORK" \
    -v "$REVIEW_FILE:/go/yelp_academic_dataset_review.json.zip" \
    reviews ./feeder 100 reviews yelp_academic_dataset_review.json.zip
