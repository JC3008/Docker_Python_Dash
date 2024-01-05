# Project Outline
This repository keeps a Python script set which aims to get open data from government sites. The main goal is to explore the brazilian economic landscape and the evolution of the companies that are listed on B3 (Brasil, Bolsa, Balcão).

# Technologies.
The whole project is wrapped in Docker container, in order to keep it from configuration issues. Then it would be nice to have basic knowledge in Docker and its basic commands. Besides it, some features are listed bellow:
* Object Oriented Python
* AWS S3
* Boto3
* Dash

# Basic configuration
### AWS credentials
It is required to have aws access_key_id and aws_secret_access_key to read and write files on S3 buckets. Once you have it, just insert your keys in Python\src\.env file.
### Objects.py
This file has all the classes that are used for managing folder structures, fetching credentials, getting data from API, writting files on S3 and Logging. 
The first task you ll have to do is to change path_name().define() in order to replace the folder names, projects, sufixes and prefixes.
After doing this, just check the last line of analysis.py for adjust the port which will be used by Dash app.

# Docker
To run this project it is required to install docker desktop. Once you have it installed, follow the forward steps: 
Locally in your cloned repository, access Python folder:
In the terminal, run ['docker build -t {image_name}:{tag_name} .']
This command will build your dockerfile and you will have a docker image as a result.
Once the image is builded, you can run this to get data into S3 and plot charts.

Running steps:
docker run {image_name}:{tag_name} python {file_name}

To plot the chart, run this:
docker run -it -p {local_port}:{container_port} {image_name}:{tag_name} bash
flags -it and -p are for:
-it is to keep the container running and available to interation by bash commands.
-p is for set the mapping of the ports.

Once you have entered in container, you have to execute ['python analysis.py'] command.

### Example of how to do it
* docker build -t okkus:okkus .
* docker run okkus:okkus python fundamentus_elt.py
* docker run okkus:okkus python CADgov.py
* docker run okkus:okkus python DFPgov.py
* docker run -it -p 8888:8888 okkus:okkus bash


