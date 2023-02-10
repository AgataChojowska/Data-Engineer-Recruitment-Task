import sys

import pandas as pd
import quandl as quandl
import boto3

NASDAQ_DATA_LINK_API_KEY = 'G2BXsCzZapym8Rn5d8P_'
quandl.ApiConfig.api_key = NASDAQ_DATA_LINK_API_KEY
BUCKET_NAME = 'big-mac-data'


class NotifyException(Exception):
    pass


class BigMac:
    def __init__(self, data_provider, CC_path):
        self.provider = data_provider
        self._country_codes_url = CC_path

    def get_country_codes(self):
        country_codes = pd.read_csv(self._country_codes_url)
        country_codes[['Country', 'Code']] = country_codes["COUNTRY|CODE"].apply(lambda x: pd.Series(str(x).split("|")))
        return country_codes

    def fetch_data(self, codes):
        data_df = pd.DataFrame()
        for code in codes:
            temp_df = self.provider.get(f'ECONOMIST/BIGMAC_{code}', start_date='2021-07-31', end_date='2021-07-31')
            temp_df['Country'] = code
            data_df = pd.concat([temp_df, data_df], axis=0)
        return data_df

    def check_if_empty(self, all_codes, fetched_countries: pd.Series):
        if len(all_codes) > len(fetched_countries.unique()):
            rest = list(set(all_codes).symmetric_difference(set(fetched_countries)))  # Get missing countries.
            missing_data = self.fetch_data(rest)
            return missing_data.empty
        return True


class Notification:
    def __init__(self, client):
        self.client = client

    def create_topic(self, topic_name):
        try:
            response = self.client.create_topic(
                Name=topic_name
            )
            return response["TopicArn"]
        except Exception as e:
            print(f"Failed to create SNS topic: {e}")
            raise NotifyException

    def create_subscription(self, arn_topic, protocol, email):
        try:
            response = self.client.subscribe(
                TopicArn=arn_topic,
                Protocol=protocol,
                Endpoint=email
            )
            return response["SubscriptionArn"]
        except Exception:
            print(f"Failed to create subscription: {e}")
            raise NotifyException

    def notify(self, topic_arn, file_name):
        try:
            self.client.publish(
                TopicArn=topic_arn,
                Message=f"The file {file_name} has been uploaded to the {BUCKET_NAME} S3 bucket.",
                Subject="Big Mac File Upload Notification",
            )
        except Exception as e:
            print(f"Failed to publish to SNS topic: {e}")
            raise NotifyException


class DataManager:
    def __init__(self, data_provider, notifier):
        self.provider = data_provider
        self.notifier = notifier

    def get_data(self):
        codes = self.provider.get_country_codes()
        big_mac_df = self.provider.fetch_data(codes['Code'])

        if big_mac.check_if_empty(codes['Code'], big_mac_df['Country']) is False:
            return None

        return big_mac_df

    def notify_data_processed(self, name):
        try:
            self.notifier.notify(topic_arn, name)
        except Exception as e:
            raise e

    @staticmethod
    def upload_to(df, destination):
        df.to_csv(
            destination,
            index=False,
            # These can be provided as constants if one is not logged in through AWS CLI
            # storage_options={
            #     "key": AWS_ACCESS_KEY_ID,
            #     "secret": AWS_SECRET_ACCESS_KEY,
            #     "token": AWS_SESSION_TOKEN,
            # },
        )


if __name__ == '__main__':
    client = boto3.client("sns")
    notifier = Notification(client)
    big_mac = BigMac(quandl, 'https://static.quandl.com/ECONOMIST_Descriptions/economist_country_codes.csv')
    manager = DataManager(big_mac, notifier)

    big_mac_df = manager.get_data()
    if big_mac_df is None:
        print("cannot fetch data")
        sys.exit(1)
    destination = f"s3://{BUCKET_NAME}/big_mac.csv"
    manager.upload_to(big_mac_df, destination)
    try:
        topic_arn = notifier.create_topic('big-mac-topic')
        subscription_arn = notifier.create_subscription(topic_arn, 'email', 'example@gmail.com')
        manager.notify_data_processed('filename')
    except NotifyException as e:
        print("cannot create notification for user")
        sys.exit(1)
