Log output for Q2

<img width="1110" alt="image" src="https://user-images.githubusercontent.com/7212518/217333598-ea392c42-26e6-43e5-a383-5d618b2344c5.png">

Command to build deployment from GitHub Storage Block

Run from the root of the repo

prefect deployment build hw2/03_deployment/parameterized_flow.py:etl_parent_flow --name github-flow -sb github/de-github/hw2/03_deployment/ -o hw2/03_deployment/etl_parent_flow-deployment.yaml --apply

Output of deployment for Q4

<img width="1511" alt="image" src="https://user-images.githubusercontent.com/7212518/217326585-b4fac6d5-6c36-40a4-bf3b-5c1098787e71.png">

Output of deployment for Q5 with Slack notification enabled

<img width="1480" alt="image" src="https://user-images.githubusercontent.com/7212518/217328122-0eee8e1b-3ec1-4625-9655-2b71246266e1.png">

<img width="1521" alt="image" src="https://user-images.githubusercontent.com/7212518/217328194-87047531-8057-41c0-8c0c-eac97d7fdbf7.png">


[Create Slack app to get the webhook](https://api.slack.com/messaging/webhooks)

Successful notification on Slack

<img width="1137" alt="image" src="https://user-images.githubusercontent.com/7212518/217327922-f1346337-d379-4094-a99f-1d61410fd2f6.png">

