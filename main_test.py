import pandas as pd
import pytest
from main import Notification, BigMac


@pytest.fixture
def data_provider():
    class DataProviderMock:
        def get(self, endpoint, start_date, end_date):
            return pd.DataFrame({'Price': [3.86]})

    return DataProviderMock()


@pytest.fixture
def notification_provider():
    class SNSMock:
        def create_topic(self, Name):
            return {"TopicArn": Name}

        def subscribe(self, TopicArn, Protocol, Endpoint):
            return {"SubscriptionArn": f'{TopicArn}:{Protocol}:{Endpoint}'}

    return SNSMock()


def test_get_country_codes(data_provider):
    bm = BigMac(data_provider, 'testdata.csv')
    country_codes = bm.get_country_codes()
    assert isinstance(country_codes, pd.DataFrame)
    assert 'Country' in country_codes.columns
    assert 'Code' in country_codes.columns


def test_fetch_data(data_provider):
    bm = BigMac(data_provider, 'testdata.csv')
    fetched_data = bm.fetch_data(['USD'])
    assert isinstance(fetched_data, pd.DataFrame)
    assert 'Country' in fetched_data.columns
    assert 'Price' in fetched_data.columns


def test_check_if_empty(data_provider):
    bm = BigMac(data_provider, 'testdata.csv')
    country_codes = ['USD', 'EUR']
    fetched_countries = pd.Series(['USD'])
    result = bm.check_if_empty(country_codes, fetched_countries)
    assert result is False


def test_create_topic(notification_provider):
    notification = Notification(client=notification_provider)
    topic_name = "my-topic"
    topic_arn = notification.create_topic(topic_name)
    assert topic_name in topic_arn


def test_create_subscription(notification_provider):
    notification = Notification(client=notification_provider)
    topic_name = "my-topic"
    topic_arn = notification.create_topic(topic_name)
    protocol = "email"
    endpoint = "chojowskaagata@gmail.com"
    subscription_arn = notification.create_subscription(topic_arn, protocol, endpoint)

    assert topic_arn in subscription_arn
    assert protocol in subscription_arn
    assert endpoint in subscription_arn
