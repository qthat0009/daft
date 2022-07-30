import base64
import io
import pathlib
import re
import tarfile
import tempfile
from typing import Any, Callable, Dict, List, Literal, Optional

import boto3
import cloudpickle
import docker
import pydantic
import yaml
from loguru import logger

from daft.env import DaftEnv, get_docker_client
from daft.serving.backend import AbstractEndpointBackend
from daft.serving.definitions import Endpoint

CONFIG_TYPE_ID = Literal["aws_lambda"]

AWS_LAMBDA_DOCKER_BUILD_PLATFORM = "linux/amd64"
CONDA_ENV_PATH = "conda_environment.yml"
ENDPOINT_PKL_FILENAME = "endpoint_file.pkl"
ENTRYPOINT_FILE_NAME = "entrypoint.py"


class AWSLambdaBackendConfig(pydantic.BaseModel):
    type: CONFIG_TYPE_ID
    execution_role_arn: str
    ecr_repository: str

    def get_ecr_repo_name(self) -> str:
        return self.ecr_repository.split("/")[-1]

    def get_ecr_repo_account_id(self) -> str:
        return self.ecr_repository.split("/")[-2]


class AWSLambdaEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints on AWS Lambda

    Limitations:

        1. Only deploys public endpoints and no auth is performed when requesting
        2. If running a custom environment, we require access to Docker and an ECR repository
    """

    DAFT_REQUIRED_DEPS = ["cloudpickle"]

    ENDPOINT_VERSION_TAG = "endpoint_version"
    FUNCTION_NAME_PREFIX = "daft-serving-"

    def __init__(self, config: AWSLambdaBackendConfig):
        self.docker_client = get_docker_client()
        self.ecr_client = boto3.client("ecr")
        self.lambda_client = boto3.client("lambda")
        self.role_arn = config.execution_role_arn
        self.ecr_repository = config.ecr_repository

    @staticmethod
    def config_type_id() -> str:
        return "aws_lambda"

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> AbstractEndpointBackend:
        return cls(AWSLambdaBackendConfig.parse_obj(config))

    def _list_daft_serving_lambda_functions(self) -> List[dict]:
        aws_lambda_functions = []
        function_paginator = self.lambda_client.get_paginator("list_functions")
        for page in function_paginator.paginate():
            for aws_lambda_function in page["Functions"]:
                if not aws_lambda_function["FunctionName"].startswith(AWSLambdaEndpointBackend.FUNCTION_NAME_PREFIX):
                    continue

                # API does not provide tags by default on list operations, we do it manually here
                aws_lambda_function["Tags"] = self.lambda_client.list_tags(
                    Resource=self._strip_function_arn_version(aws_lambda_function["FunctionArn"])
                )["Tags"]

                aws_lambda_functions.append(aws_lambda_function)
        return aws_lambda_functions

    def _strip_function_arn_version(self, function_arn: str) -> str:
        if re.match(
            r"arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}(-gov)?-[a-z]+-\d{1}:\d{12}:function:[a-zA-Z0-9-_]+:(\$LATEST|[a-zA-Z0-9-_]+)",
            function_arn,
        ):
            return function_arn.rsplit(":", 1)[0]
        return function_arn

    def list_endpoints(self) -> List[Endpoint]:
        aws_lambda_functions = self._list_daft_serving_lambda_functions()

        # Each function should have been created with a corresponding URL config, but if it hasn't we will
        # return None for the URL instead.
        aws_lambda_url_configs = []
        for f in aws_lambda_functions:
            try:
                aws_lambda_url_configs.append(
                    self.lambda_client.get_function_url_config(FunctionName=f["FunctionName"])
                )
            except self.lambda_client.exceptions.ResourceNotFoundException:
                aws_lambda_url_configs.append(None)

        return [
            Endpoint(
                name=f["FunctionName"],
                version=f["Tags"][AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG],
                addr=url_config["FunctionUrl"] if url_config else None,
            )
            for f, url_config in zip(aws_lambda_functions, aws_lambda_url_configs)
        ]

    def deploy_endpoint(
        self,
        endpoint_name: str,
        endpoint: Callable[[Any], Any],
        custom_env: Optional[DaftEnv] = None,
    ) -> Endpoint:
        lambda_function_name = f"{AWSLambdaEndpointBackend.FUNCTION_NAME_PREFIX}{endpoint_name}"
        lambda_function_version = 1

        # Check for existing function
        try:
            old_function = self.lambda_client.get_function(FunctionName=lambda_function_name)
            lambda_function_version = int(old_function["Tags"][AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG]) + 1
        except self.lambda_client.exceptions.ResourceNotFoundException:
            pass

        # Build and push image to ECR
        image_tag = f"{endpoint_name}-v{lambda_function_version}"
        docker_tag = f"{self.ecr_repository}:{image_tag}"
        docker_image = build_aws_lambda_docker_image(
            docker_client=self.docker_client,
            env=custom_env if custom_env is not None else DaftEnv(),
            docker_tag=docker_tag,
            endpoint=endpoint,
        )
        resp = self.ecr_client.get_authorization_token()
        token = base64.b64decode(resp["authorizationData"][0]["authorizationToken"]).decode()
        username, password = token.split(":")
        try:
            for line in self.docker_client.images.push(
                repository=self.ecr_repository,
                tag=image_tag,
                auth_config={"username": username, "password": password},
                stream=True,
                decode=True,
            ):
                if "status" in line and not line["status"] == "Pushing":
                    logger.debug(f"Pushing image: {line['status']}")
        except docker.errors.APIError as e:
            logger.error(
                f"Failed to push image, ensure that you have the correct credentials to be pushing to {self.ecr_repository}: {e}"
            )

        # Create Lambda function
        if lambda_function_version > 1:
            response = self.lambda_client.update_function_code(
                FunctionName=lambda_function_name,
                ImageUri=docker_tag,
                Publish=True,
            )
            self.lambda_client.tag_resource(
                Resource=self._strip_function_arn_version(response["FunctionArn"]),
                Tags={
                    AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG: str(lambda_function_version),
                },
            )
        else:
            self.lambda_client.create_function(
                FunctionName=lambda_function_name,
                PackageType="Image",
                Code={"ImageUri": docker_tag},
                Description="Daft serving endpoint",
                Environment={
                    "Variables": {
                        "ENDPOINT_PKL_FILEPATH": "endpoint.pkl",
                    },
                },
                Architectures=["x86_64"],
                Tags={
                    "owner": "daft-serving",
                    AWSLambdaEndpointBackend.ENDPOINT_VERSION_TAG: str(lambda_function_version),
                },
                Role=self.role_arn,
                Publish=True,
            )

        # Add permission for anyone to invoke the lambda function
        try:
            self.lambda_client.add_permission(
                FunctionName=lambda_function_name,
                StatementId="public-invoke",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
        except self.lambda_client.exceptions.ResourceConflictException:
            pass

        # Create an endpoint with Lambda URL
        try:
            url_config = self.lambda_client.get_function_url_config(FunctionName=lambda_function_name)
        except self.lambda_client.exceptions.ResourceNotFoundException:
            url_config = self.lambda_client.create_function_url_config(
                FunctionName=lambda_function_name,
                AuthType="NONE",
            )

        return Endpoint(
            name=endpoint_name,
            version=lambda_function_version,
            addr=url_config["FunctionUrl"],
        )


def build_aws_lambda_docker_image(
    docker_client: docker.DockerClient, env: DaftEnv, docker_tag: str, endpoint: Callable[[Any], Any]
) -> docker.models.images.Image:
    """Builds the image to be served locally"""

    # Extend the base conda environment with serving dependencies
    conda_env = env.get_conda_environment()
    conda_env["dependencies"].extend(["cloudpickle"])

    tarbytes = io.BytesIO()
    with tarfile.open(fileobj=tarbytes, mode="w") as tar, tempfile.TemporaryDirectory() as td:
        tmpdir = pathlib.Path(td)

        # Add conda env as a YAML file
        conda_env_file = tmpdir / CONDA_ENV_PATH
        conda_env_file.write_text(yaml.dump(conda_env))
        tar.add(conda_env_file, arcname=CONDA_ENV_PATH)

        # Add the endpoint function to tarfile as a pickle
        pickle_file = tmpdir / ENDPOINT_PKL_FILENAME
        pickle_file.write_bytes(cloudpickle.dumps(endpoint))
        tar.add(pickle_file, arcname=ENDPOINT_PKL_FILENAME)

        # Add entrypoint file to tarfile
        tar.add(pathlib.Path(__file__).parent.parent / "static" / "docker-entrypoint.py", arcname=ENTRYPOINT_FILE_NAME)

        # Add Dockerfile to tarfile
        dockerfile = pathlib.Path(__file__).parent.parent / "static" / "Dockerfile.aws_lambda"
        tar.add(str(dockerfile), arcname="Dockerfile")

        # Build the image
        tarbytes.seek(0)
        response = docker_client.api.build(
            fileobj=tarbytes,
            custom_context=True,
            tag=docker_tag,
            decode=True,
            buildargs={
                "PYTHON_VERSION": env.python_version[: env.python_version.rfind(".")],
                "CONDA_ENV_PATH": CONDA_ENV_PATH,
                "ENDPOINT_PKL_FILENAME": ENDPOINT_PKL_FILENAME,
                "ENTRYPOINT_FILE_NAME": ENTRYPOINT_FILE_NAME,
            },
            platform=AWS_LAMBDA_DOCKER_BUILD_PLATFORM,
            target="serving",
        )
        for msg in response:
            if "stream" in msg:
                logger.debug(msg["stream"])
                match = re.search(r"(^Successfully built |sha256:)([0-9a-f]+)$", msg["stream"])
                if match:
                    image_id = match.group(2)
                    print(f"Built image: {image_id}")
                    return docker_client.images.get(image_id)
            if "aux" in msg:
                logger.debug(msg["aux"])
            if "errorDetail" in msg:
                logger.debug(msg["errorDetail"]["message"])
        raise RuntimeError("Failed to build base Docker image, check debug logs for more info")
