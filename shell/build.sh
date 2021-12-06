# create an ECR repository in AWS
aws ecr create-repository --repository-name $2


# build and export this image to AWS ECR
export REGION=$1
export ACCOUNT_ID=`aws sts get-caller-identity --query Account --output text`
docker build -t $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$2:$3 -f Dockerfile .
aws ecr get-login-password | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$2:$3
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$2:$3

# Run RESTful server




if [ $5 == '0' ]; then
    docker run --name $2 -d -p $4:$4 $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$2
else
    docker run -v $6:$7 --name $2 -d -p $4:$4 $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$2
fi
