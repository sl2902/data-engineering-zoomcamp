from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow

github_block = GitHub.load("de-github")
github_deploy = Deployment.build_from_flow(
     flow=etl_parent_flow,
     name="github-flow",
     storage=github_block,
     entrypoint="hw2/03_deployment/parameterized_flow.py:etl_parent_flow")

if __name__ == "__main__":
    github_deploy.apply()