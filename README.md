# Leaflink Takehome

## Setup

**Install OS level dependencies**

- Python 3.7.2
- PostgreSQL

**Create .env file**

Create .env file using the sample.env file as a guide. Db credential are to Redshift database,

**Install app requirements**

```
pytohm -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## Running Code

```
python main.py
```

**Note about code**

I did NOT use the COPY SQL command (wrapping the commands in a psycopg2 execute statement) to transfer the data from the S3 bucket to Redshift. I imagine this would be the fastest way of doing things. I did not do this for two reasons.

1. Most importantly, I tested out the command and I got an error b/c the region I set up my Redshift cluster in (set one up b/c I have not done it before) was different than the region the S3 bucket in. I then did attempt to run ```aws s3api get0bucket-location --bucket leaflink-data-interview-exercise``` to get the region but got a permission denied error. 
2. Notwithstanding that this error made the use of the COPY command much more difficult/not possible, the instructions to me seemed to indicate a more Python centric solution was preferrable.  