from datetime import datetime
from kafka import KafkaConsumer
from SimpleLogEngine import log
import mysql.connector


class MySQLKafkaConsumer():
    """
    Engine for extracting data from Kafka, transforming, and loading into a MySQL database.

    Methods
    _______
    etl():
        Creates a KafkaConsumer to the specified topic and ETLs each message into MySQL using a for loop.
    """

    def __init__ (self, topic, database):
        """
        Attempts connection to the MySQL database to initialize mysql.connector for 'etl' class method.

        Parameters
        __________
        topic : str
            String of Kafka topic.
        database : str
            MySQL database to insert data into.

        """

        # redundant checks to ensure provided parameters are string data type.
        if not isinstance(topic, (str)):
            raise TypeError('Topic input must be string of topic name.')

        else:
            # stores topic in class's self.topic attribute.
            self.topic = topic

            if not isinstance(database, (str)):
                raise TypeError('Database input must be string of database name.')

            else:
                try:
                    # MySQL connection:
                    print(f"Attempting connection to MySQL DB '{database}'")
                    self.connection = mysql.connector.connect(host='host_here', database=database, user='root', password='pswrd_here')

                    log(f'MySQLKafkaConsumer.__init__,{topic} {database},MySQLKafkaConsumer Object,{__file__}')

                except Exception:
                    print("Could not connect to database. Please check credentials")

                else:
                    print("Successfully connected to database")

    def etl():
        """
        Creates a KafkaConsumer to the specified topic and ETLs each message into MySQL using a for loop.
        """

        # creates MySQL cursor.
        cursor = self.connection.cursor()

        # connects to Kafka.
        print("Connecting to Kafka")
        consumer = KafkaConsumer(self.topic)
        print("Connected to Kafka")
        print(f"Reading messages from the topic {self.topic}")

        log(f'MySQLKafkaConsumer.etl,None,None,{__file__}')

        # For loop ETL for Kafka streaming data:
        for msg in consumer:

            # Extracts information from kafka.
            message = msg.value.decode("utf-8")

            # Transforms the date format to suit the database schema.
            (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")
            dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
            timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

            # Loads data into the database table.
            sql = "insert into livetolldata values(%s,%s,%s,%s)"
            result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
            print(f"A {vehicle_type} was inserted into the database")
            
            connection.commit()

            connection.close()


# Execution:

consumer = MySQLKafkaConsumer('toll', 'tolldata')
consumer.etl()