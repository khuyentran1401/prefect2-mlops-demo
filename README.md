[![View on Medium](https://img.shields.io/badge/Medium-View%20on%20Medium-blueviolet?logo=medium)](https://medium.com/the-prefect-blog/sending-slack-notifications-in-python-with-prefect-840a895f81c)
# Orchestrate Your Data Science Project with Prefect 2.0

## What is Prefect?
Prefect is an open-sourced library that allows you to orchestrate your data workflow in Python. Prefect mission is to eliminate negative engineering so that you can focus on positive engineering.
## Set up
### Download requirements
```bash
pip install -r requirements.txt
```

### Download data
```bash
dvc pull -r origin 
```

## Run the flows
To run all of the development flows, run:
```bash
cd src
python development.py
```

## Create notifications
Now that you have set up the environment, you are ready to create notifications! Follow [this article](https://medium.com/the-prefect-blog/sending-slack-notifications-in-python-with-prefect-840a895f81c) for further instructions on how to do that.