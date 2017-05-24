#!/usr/bin/python
__author__ = 'robertsanders'

from peewee import *
from random import randrange
import sys
import getopt
import random
import string
import datetime

"""
Pre-run Instructions:
    Install PIP
        Follow the instructions specified here: https://pip.pypa.io/en/stable/installing/

    Install Python Dependencies:
        1. Install dependant python packages using PIP:
            $ pip install peewee
            $ pip install PyMySQL
        2. Update the DATABASE_HOST, DATABASE_SCHEMA, DATABASE_USERNAME and DATABASE_PASSWORD entries
        3. Create the database you define in the DATABASE_SCHEMA variable before you run the script


Run Instructions:

python practical_exercise_data_generator.py --help

    This will print out the operations that you can perform with the script


python practical_exercise_data_generator.py --load_data

    This will create the tables (if they dont already exist), delete users, insert new and update existing data in the MySQL database you have specified


python practical_exercise_data_generator.py --create_csv

    This will generate the CSV file based off the data that exists in the MySQL Database

"""



DATABASE_HOST = "127.0.0.1"
DATABASE_SCHEMA = "practical_exercise_1"
DATABASE_USERNAME = "root"
DATABASE_PASSWORD = "cloudera"

NUM_OF_USERS_TO_INSERT = 10
NUM_OF_USERS_TO_UPDATE = 2
NUM_OF_USERS_TO_DELETE = 2
NUM_OF_ACTIVITY_TO_INSERT = 50
NUM_OF_UPLOADS_IN_CSV = 50

db = MySQLDatabase(DATABASE_SCHEMA, host=DATABASE_HOST, user=DATABASE_USERNAME, passwd=DATABASE_PASSWORD)


class User(Model):
    id = PrimaryKeyField()
    name = TextField()

    class Meta:
        database = db


class ActivityLog(Model):
    id = PrimaryKeyField()
    user = ForeignKeyField(User, related_name='users', null=True)
    type = TextField()
    timestamp = TimestampField()

    types = ["INSERT", "UPDATE", "DELETE"]

    class Meta:
        database = db

User.create_table(True)
ActivityLog.create_table(True)


def generate_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(random.randint(1, length)))


def get_timestamp():
    return datetime.datetime.now() - datetime.timedelta(hours=randrange(24), minutes=randrange(60), seconds=randrange(60))


def create_csv():
    print("Running create_csv")

    users = User.select()
    user_count = len(users)
    csv_filename = "user_upload_dump." + str(datetime.datetime.now()).replace(" ", "") + ".csv"
    f = open(csv_filename, 'w')
    csv_header = "user_id,file_name,timestamp"
    f.write(csv_header + "\n")
    for i in range(0, NUM_OF_UPLOADS_IN_CSV):
        user_id = users[random.randint(0, user_count - 1)].id
        file_name = generate_random_string(10) + "." + generate_random_string(3)
        timestamp = get_timestamp()
        row = str(user_id) + "," + str(file_name) + "," + str(timestamp.strftime('%s'))
        f.write(row + "\n")
    f.close()

    print("Finished running create_csv")


def load_data():
    print("Running load_data")
    print("")

    # delete random users
    users = User.select()
    user_count = len(users)
    if user_count:
        for i in range(0, NUM_OF_USERS_TO_DELETE):
            users = User.select()
            user_count = len(users)
            user_to_delete = users[random.randint(0, user_count - 1)]
            print("Deleting user with ID: " + str(user_to_delete.id))
            # setting user_id to null in activitylog so that the foreign key constraint doesn't get effected from deleting the user
            ActivityLog.update(user=None).where(ActivityLog.user == user_to_delete).execute()
            # deleting the actual user
            user_to_delete.delete_instance()
    else:
        print("No users to delete. Skipping the delete step.")

    print("")

    # add users
    for i in range(0, NUM_OF_USERS_TO_INSERT):
        new_user_name = generate_random_string(10)
        new_user = User.create(name=new_user_name)
        new_user.save()
        print("Created user with ID: " + str(new_user.id))

    print("")

    # update users
    users = User.select()
    user_count = len(users)
    for i in range(0, NUM_OF_USERS_TO_UPDATE):
        user = users[random.randint(0, user_count - 1)]
        user.name = generate_random_string(10)
        user.save()
        print("Updated user with ID: " + str(user.id))

    print("")

    # add activity
    users = User.select()
    user_count = len(users)
    for i in range(0, NUM_OF_ACTIVITY_TO_INSERT):
        user = users[random.randint(0, user_count - 1)]
        new_activity_log_entry = ActivityLog.create(
            user=user,
            type=ActivityLog.types[random.randint(0, len(ActivityLog.types) - 1)],
            timestamp=get_timestamp()
        )
        new_activity_log_entry.save()
        print("Created activity_log with ID: " + str(new_activity_log_entry.id) + ", for User with ID: " + str(user.id))

    print("")

    print("Finished running load_data")


def main(argv):
    executed = False
    help_text = 'python practical_exercise_data_generator.py [--create_csv or --load_data]'
    try:
        opts, args = getopt.getopt(argv, "h", ["help", "create_csv", "load_data"])
    except getopt.GetoptError:
        print(help_text)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(help_text)
            sys.exit(0)
        elif opt == "--create_csv":
            create_csv()
            executed = True
        elif opt == "--load_data":
            load_data()
            executed = True
    if not executed:
        print("No options provided")
        print(help_text)
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv[1:])
