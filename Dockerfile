FROM python:3.8.1

# Don't cache Python packages
# ENV PIP_NO_CACHE=yes

# Keeps python from generation .pyc files in the container
# ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
# ENV PYTHONPATH "${PYTHONPATH}:/code/"


# initializing new working directory
RUN mkdir /code
WORKDIR /code

# install all requirements from requirements.txt
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Transferring the code and essential data
COPY xetra ./xetra
COPY run.py ./run.py