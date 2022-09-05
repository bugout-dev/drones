"""
Collect secrets from AWS SSM Parameter Store and output as environment variable exports.
"""
import argparse
from dataclasses import dataclass
import json
import sys
from typing import Any, Dict, Iterable, List, Optional

import boto3


@dataclass
class EnvironmentVariable:
    name: str
    value: str


env_param_mapping = {
    "SPIRE_CORS_ALLOWED_ORIGINS": "/spire/prod/SPIRE_CORS_ALLOWED_ORIGINS",
    "BROOD_CORS_ALLOWED_ORIGINS": "/brood/prod/BROOD_CORS_ALLOWED_ORIGINS",
    "SPIRE_DB_URI": "/spire/prod/SPIRE_DB_URI",
    "SPIRE_DB_URI_READ_ONLY": "/spire/prod/SPIRE_DB_URI_READ_ONLY",
    "BROOD_DB_URI": "/brood/prod/BROOD_DB_URI",
    "BROOD_DB_URI_READ_ONLY": "/brood/prod/BROOD_DB_URI_READ_ONLY",
    "AWS_S3_DRONES_BUCKET": "/spire/prod/AWS_S3_DRONES_BUCKET",
    "AWS_S3_DRONES_BUCKET_STATISTICS_PREFIX": "/spire/prod/AWS_S3_DRONES_BUCKET_STATISTICS_PREFIX",
    "BUGOUT_AUTH_URL": "/spire/prod/BUGOUT_AUTH_URL",
    "BUGOUT_CLIENT_ID_HEADER": "/spire/prod/BUGOUT_CLIENT_ID_HEADER",
    "BUGOUT_BOT_INSTALLATION_TOKEN_HEADER": "/spire/prod/BUGOUT_BOT_INSTALLATION_TOKEN_HEADER",
}


def get_parameters(path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve parameters from AWS SSM Parameter Store. Decrypts any encrypted parameters.

    This function gets the parameters from the specified path (if provided), but ALSO uses the
    `env_param_mapping` to get necessary parameters that don't live on the specified path.

    Relies on the appropriate environment variables to authenticate against AWS:
    https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
    """
    ssm = boto3.client("ssm")
    parameters: List[Dict[str, Any]] = []
    for _, value in env_param_mapping.items():
        kwargs = {"Name": value, "WithDecryption": True}
        response = ssm.get_parameter(**kwargs)
        new_parameter = response.get("Parameter", {})
        parameters.append(new_parameter)

    if path is not None:
        next_token: Optional[bool] = True
        while next_token is not None:
            kwargs = {"Path": path, "Recursive": False, "WithDecryption": True}
            if next_token is not True:
                kwargs["NextToken"] = next_token
            response = ssm.get_parameters_by_path(**kwargs)
            new_parameters = response.get("Parameters", [])
            parameters.extend(new_parameters)
            next_token = response.get("NextToken")

    return parameters


def parameter_to_env(parameter_object: Dict[str, Any]) -> EnvironmentVariable:
    """
    Transforms parameters returned by the AWS SSM API into EnvironmentVariables.
    """
    parameter_path = parameter_object.get("Name")
    if parameter_path is None:
        raise ValueError('Did not find "Name" in parameter object')
    name = parameter_path.split("/")[-1].upper()

    value = parameter_object.get("Value")
    if value is None:
        raise ValueError('Did not find "Value" in parameter object')

    return EnvironmentVariable(name, value)


def env_string(env_vars: Iterable[EnvironmentVariable], with_export: bool) -> str:
    """
    Produces a string which, when executed in a shell, exports the desired environment variables as
    specified by env_vars.
    """
    prefix = "export " if with_export else ""
    return "\n".join([f'{prefix}{var.name}="{var.value}"' for var in env_vars])


def extract_handler(args: argparse.Namespace) -> None:
    """
    Save environment variables to file.
    """
    result = env_string(map(parameter_to_env, get_parameters(args.path)), args.export)
    with args.outfile as ofp:
        print(result, file=ofp)


def print_handler(args: argparse.Namespace) -> None:
    """
    Print out mapping dictionary with variables.
    """
    print(json.dumps(env_param_mapping))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Materialize environment variables from AWS SSM Parameter Store"
    )
    parser.set_defaults(func=lambda _: parser.print_help())
    subcommands = parser.add_subparsers(description="Parameters commands")

    parser_print = subcommands.add_parser(
        "print", description="Parameters print commands"
    )
    parser_print.set_defaults(func=lambda _: parser_print.print_help())
    parser_print.set_defaults(func=print_handler)

    parser_extract = subcommands.add_parser(
        "extract", description="Parameters extract commands"
    )
    parser_extract.set_defaults(func=lambda _: parser_extract.print_help())
    parser_extract.add_argument(
        "-o", "--outfile", type=argparse.FileType("w"), default=sys.stdout
    )
    parser_extract.add_argument(
        "--export",
        action="store_true",
        help="Set to output environment strings with export statements",
    )
    parser_extract.add_argument(
        "-p",
        "--path",
        default=None,
        help="SSM path from which to pull environment variables (pull is NOT recursive)",
    )
    parser_extract.set_defaults(func=extract_handler)

    args = parser.parse_args()
    args.func(args)
