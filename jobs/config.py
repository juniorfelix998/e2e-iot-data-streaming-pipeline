import os

from dotenv import load_dotenv


load_dotenv()


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

configuration = {
    "AWS_SECRET_KEY": AWS_SECRET_KEY,
    "AWS_ACCESS_KEY": AWS_ACCESS_KEY,
}
