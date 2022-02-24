#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock, patch
from urllib.parse import urlparse

import pytest
from requests import Response
from requests.sessions import PreparedRequest
from source_snapchat_marketing.source import (
    Adaccounts,
    AdaccountStats,
    Organizations,
    SnapchatAdsOauth2Authenticator,
    get_depend_on_records,
)


@pytest.fixture
def config():
    fake_config = {
        "client_id": "s0m3-cl13nt-1d",
        "client_secret": "s0m3-cl13nt-s3cr3t",
        "refresh_token": "s0m3-v3ry-l0ng-r3fr3sh-t0ken",
        "start_date": "1970-01-01",
        "granularity": "DAY",
    }
    return fake_config


@pytest.fixture
def depends_on_stream_config(config):
    with patch.object(SnapchatAdsOauth2Authenticator, "refresh_access_token", return_value=("s0m3-l0ng-4cc3ss-t0k3n", 600)):
        yield {"authenticator": SnapchatAdsOauth2Authenticator(config), "start_date": config.get("start_date")}


def _mocked_organizations_resp():
    """
    Returns mocked JSON response for organizations endpoint in Snapchat API v1
    according to https://marketingapi.snapchat.com/docs/#get-all-organizations (from Nov. 2021)
    """
    json_resp = {
        "request_status": "success",
        "request_id": "57affee300ff0d91229fabb9710001737e616473617069736300016275696c642d35396264653638322d312d31312d3700010110",
        "organizations": [
            {
                "sub_request_status": "success",
                "organization": {
                    "id": "40d6719b-da09-410b-9185-0cc9c0dfed1d",
                    "updated_at": "2017-05-26T15:14:44.877Z",
                    "created_at": "2017-05-26T15:14:44.877Z",
                    "name": "My Organization",
                    "address_line_1": "101 Stewart St",
                    "locality": "Seattle",
                    "administrative_district_level_1": "WA",
                    "country": "US",
                    "postal_code": "98134",
                    "type": "ENTERPRISE",
                },
            },
            {
                "sub_request_status": "success",
                "organization": {
                    "id": "507d7a57-94de-4239-8a74-e93c00ca53e6",
                    "updated_at": "2016-08-01T15:14:44.877Z",
                    "created_at": "2017-08-01T15:14:44.877Z",
                    "name": "Hooli",
                    "address_line_1": "1100 Silicon Vallety Rd",
                    "locality": "San Francisco",
                    "administrative_district_level_1": "CA",
                    "country": "US",
                    "postal_code": "94110",
                    "type": "ENTERPRISE",
                },
            },
        ],
    }
    return json_resp


def _mocked_ad_accounts_resp():
    """
    Returns mocked JSON response for ad accounts endpoint in Snapchat API v1
    according to https://marketingapi.snapchat.com/docs/#get-all-ad-accounts (from Nov. 2021)
    """
    json_resp = {
        "request_status": "success",
        "request_id": "57b0015e00ff07d5c0c38928ad0001737e616473617069736300016275696c642d35396264653638322d312d31312d3700010107",
        "adaccounts": [
            {
                "sub_request_status": "success",
                "adaccount": {
                    "id": "8adc3db7-8148-4fbf-999c-8d2266369d74",
                    "updated_at": "2016-08-11T22:03:58.869Z",
                    "created_at": "2016-08-11T22:03:58.869Z",
                    "name": "Hooli Test Ad Account",
                    "type": "PARTNER",
                    "status": "ACTIVE",
                    "organization_id": "40d6719b-da09-410b-9185-0cc9c0dfed1d",
                    "funding_source_ids": ["e703eb9f-8eac-4eda-a9c7-deec3935222d"],
                    "currency": "USD",
                    "timezone": "UTC",
                    "advertiser": "Hooli",
                },
            },
            {
                "sub_request_status": "success",
                "adaccount": {
                    "id": "81cf9302-764c-429a-8561-e3bc329cf987",
                    "updated_at": "2016-08-12T13:21:47.645Z",
                    "created_at": "2016-08-11T22:03:58.869Z",
                    "name": "Awesome Ad Account",
                    "type": "DIRECT",
                    "status": "ACTIVE",
                    "organization_id": "40d6719b-da09-410b-9185-0cc9c0dfed1d",
                    "funding_source_ids": ["7abfb9c6-0258-4eee-9898-03a8c099695d"],
                    "currency": "USD",
                    "timezone": "UTC",
                    "advertiser": "Hooli",
                },
            },
        ],
    }
    return json_resp


def _mocked_empty_ad_accounts_resp():
    json_resp = {"request_status": "success", "adaccounts": []}
    return json_resp


def _mocked_ad_account_stats_resp():
    """
    Returns mocked JSON response for ad account stats endpoint in Snapchat API v1
    according to https://marketingapi.snapchat.com/docs/#get-ad-account-stats (from Nov. 2021)
    """
    json_resp = {
        "request_status": "success",
        "request_id": "57b24d9c00ff0d85622341e7c60001737e616473617069736300016275676c642b34326313636139312d332d31312d390001010d",
        "total_stats": [
            {
                "sub_request_status": "success",
                "total_stat": {
                    "id": "8adc3db7-8148-4fbf-999c-8d2266369d74",
                    "type": "AD_ACCOUNT",
                    "granularity": "TOTAL",
                    "stats": {"spend": 89196290},
                    "finalized_data_end_time": "2019-08-29T03:00:00.000-07:00",
                },
            }
        ],
    }
    return json_resp


def _mocked_ad_account_breakdown_stats_1_resp():
    json_resp = {
        "request_status": "SUCCESS",
        "request_id": "618917a200ff036a48b0197fee0001737e616473617069736300016275696c642d32313530366436352d312d3439342d3000010125",
        "timeseries_stats": [
            {
                "sub_request_status": "SUCCESS",
                "timeseries_stat": {
                    "id": "8adc3db7-8148-4fbf-999c-8d2266369d74",
                    "type": "AD_ACCOUNT",
                    "start_time": "2021-11-01T00:00:00.000Z",
                    "end_time": "2021-11-02T00:00:00.000Z",
                    "finalized_data_end_time": "2021-11-08T00:00:00.000Z",
                    "breakdown_stats": {
                        "campaign": [
                            {
                                "id": "4c7a87a4-633f-l33d-a5f6-91cfb3a8c199",
                                "type": "CAMPAIGN",
                                "granularity": "DAY",
                                "start_time": "2021-11-01T00:00:00.000Z",
                                "end_time": "2021-11-02T00:00:00.000Z",
                                "timeseries": [
                                    {
                                        "start_time": "2021-11-01T00:00:00.000Z",
                                        "end_time": "2021-11-02T00:00:00.000Z",
                                        "stats": {"spend": 50000000},
                                    }
                                ],
                            },
                            {
                                "id": "d4d8f5e7-3765-l33d-aa4b-f1eea6b85ccf",
                                "type": "CAMPAIGN",
                                "granularity": "DAY",
                                "start_time": "2021-11-01T00:00:00.000Z",
                                "end_time": "2021-11-02T00:00:00.000Z",
                                "timeseries": [
                                    {
                                        "start_time": "2021-11-01T00:00:00.000Z",
                                        "end_time": "2021-11-02T00:00:00.000Z",
                                        "stats": {"spend": 224387136},
                                    }
                                ],
                            },
                            {
                                "id": "f675e1d9-47fb-l33d-ade3-6d8659331c54",
                                "type": "CAMPAIGN",
                                "granularity": "DAY",
                                "start_time": "2021-11-01T00:00:00.000Z",
                                "end_time": "2021-11-02T00:00:00.000Z",
                                "timeseries": [
                                    {
                                        "start_time": "2021-11-01T00:00:00.000Z",
                                        "end_time": "2021-11-02T00:00:00.000Z",
                                        "stats": {"spend": 42241559},
                                    }
                                ],
                            },
                        ]
                    },
                },
            }
        ],
    }
    return json_resp


def _mocked_ad_account_breakdown_stats_2_resp():
    json_resp = {
        "request_status": "SUCCESS",
        "request_id": "618917a200ff036a48b0197fee0001737e616473617069736300016275696c642d32313530366436352d312d3439342d3000010125",
        "timeseries_stats": [
            {
                "sub_request_status": "SUCCESS",
                "timeseries_stat": {
                    "id": "8adc3db7-8148-4fbf-999c-8d2266369d74",
                    "type": "AD_ACCOUNT",
                    "start_time": "2021-11-02T00:00:00.000Z",
                    "end_time": "2021-11-03T00:00:00.000Z",
                    "finalized_data_end_time": "2021-11-03T00:00:00.000Z",
                    "breakdown_stats": {
                        "campaign": [
                            {
                                "id": "4c7a87a4-633f-l33d-a5f6-91cfb3a8c199",
                                "type": "CAMPAIGN",
                                "granularity": "DAY",
                                "start_time": "2021-11-02T00:00:00.000Z",
                                "end_time": "2021-11-03T00:00:00.000Z",
                                "timeseries": [
                                    {
                                        "start_time": "2021-11-02T00:00:00.000Z",
                                        "end_time": "2021-11-03T00:00:00.000Z",
                                        "stats": {"spend": 40000000},
                                    }
                                ],
                            },
                            {
                                "id": "d4d8f5e7-3765-l33d-aa4b-f1eea6b85ccf",
                                "type": "CAMPAIGN",
                                "granularity": "DAY",
                                "start_time": "2021-11-02T00:00:00.000Z",
                                "end_time": "2021-11-03T00:00:00.000Z",
                                "timeseries": [
                                    {
                                        "start_time": "2021-11-02T00:00:00.000Z",
                                        "end_time": "2021-11-03T00:00:00.000Z",
                                        "stats": {"spend": 124387136},
                                    }
                                ],
                            },
                            {
                                "id": "f675e1d9-47fb-l33d-ade3-6d8659331c54",
                                "type": "CAMPAIGN",
                                "granularity": "DAY",
                                "start_time": "2021-11-02T00:00:00.000Z",
                                "end_time": "2021-11-03T00:00:00.000Z",
                                "timeseries": [
                                    {
                                        "start_time": "2021-11-02T00:00:00.000Z",
                                        "end_time": "2021-11-03T00:00:00.000Z",
                                        "stats": {"spend": 32241559},
                                    }
                                ],
                            },
                        ]
                    },
                },
            }
        ],
    }
    return json_resp


def _mocked_empty_ad_account_stats_resp():
    return {
        "request_status": "SUCCESS",
        "request_id": "61891fbd00ff0f156c394b87c60001737e616473617069736300016275696c642d32313530366436352d312d3439342d300001010c",
    }


def _mocked_error_resp():
    return {
        "request_status": "ERROR",
        "request_id": "61891ca600ff0a49d9ca3e10cc0001737e616473617069736300016275696c642d32313530366436352d312d3439342d3000010141",
        "display_message": "We're sorry, but the request cannot be processed",
        "error_code": "E1004",
    }


# This method will be used by the mock to replace requests.get
def mocked_requests_send(request: PreparedRequest, **kwargs):
    resp = Mock(spec=Response)

    url = urlparse(request.url)

    if url.path == "/v1/me/organizations":
        resp.status_code = 200
        resp.json = _mocked_organizations_resp
    elif url.path == "/v1/organizations/40d6719b-da09-410b-9185-0cc9c0dfed1d/adaccounts":
        resp.status_code = 200
        resp.json = _mocked_ad_accounts_resp
    elif url.path == "/v1/organizations/507d7a57-94de-4239-8a74-e93c00ca53e6/adaccounts":
        resp.status_code = 200
        resp.json = _mocked_empty_ad_accounts_resp
    elif url.path == "/v1/adaccounts/8adc3db7-8148-4fbf-999c-8d2266369d74/stats":
        resp.status_code = 200
        if not url.params:
            resp.json = _mocked_ad_account_stats_resp
        elif "breakdown=campaign" in url.params and "start_time=2021-11-01" in url.params:
            resp.json = _mocked_ad_account_breakdown_stats_1_resp
        elif "breakdown=campaign" in url.params and "start_time=2021-11-02" in url.params:
            resp.json = _mocked_ad_account_breakdown_stats_2_resp
    else:
        resp.status_code = 404
        resp.json = _mocked_error_resp

    return resp


def test_get_depend_on_ids_none(depends_on_stream_config):
    """Testing the stream that has non parent dependency (like Organizations has no dependency)"""
    # sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    depends_on_stream = None
    slice_key_name = None
    ids = get_depend_on_records(depends_on_stream, depends_on_stream_config, slice_key_name)
    assert ids == [None]


def test_get_depend_on_ids_1(depends_on_stream_config):
    """Testing the stream that has 1 level parent dependency (like Adaccounts has dependency on Organizations)"""
    depends_on_stream = Organizations
    slice_key_names = ["id"]

    with patch("requests.sessions.Session.send", side_effect=mocked_requests_send):
        ids = get_depend_on_records(depends_on_stream, depends_on_stream_config, slice_key_names)

    expected_organization_ids = [
        {key: record.get("organization").get(key) for key in slice_key_names}
        for record in _mocked_organizations_resp().get("organizations")
    ]

    assert ids == expected_organization_ids


def test_get_depend_on_ids_2(depends_on_stream_config):
    """
    Testing the that has 2 level parent dependency on organization ids
    (like Media has dependency on Adaccounts and Adaccounts has dependency on Organizations)
    """
    depends_on_stream = Adaccounts
    slice_key_names = ["id", "timezone"]

    with patch("requests.sessions.Session.send", side_effect=mocked_requests_send):
        ids = get_depend_on_records(depends_on_stream, depends_on_stream_config, slice_key_names)

    expected_adaccount_ids = [
        {key: record.get("adaccount", {}).get(key) for key in slice_key_names}
        for record in _mocked_ad_accounts_resp().get("adaccounts", [])
    ]

    assert ids == expected_adaccount_ids


def test_get_updated_state_ad_account_stats(config):
    auth = SnapchatAdsOauth2Authenticator(config)
    kwargs = {"authenticator": auth, "start_date": "2021-11-01"}
    stats_kwargs = {"granularity": config["granularity"]}
    incremental_stream = AdaccountStats(**{**kwargs, **stats_kwargs})
    current_state_stream = {}
    latest_record = {incremental_stream.cursor_field: "2021-11-02T00:00:00+00:00"}

    new_stream_state = incremental_stream.get_updated_state(current_state_stream, latest_record)
    assert new_stream_state == {incremental_stream.cursor_field: "2021-11-01T00:00:00+00:00"}

    current_state_stream = {incremental_stream.cursor_field: "2021-11-01T00:00:00+00:00"}
    latest_record = {incremental_stream.cursor_field: "2021-11-03T00:00:00+00:00"}
    new_stream_state = incremental_stream.get_updated_state(current_state_stream, latest_record)
    assert new_stream_state == {incremental_stream.cursor_field: "2021-11-03T00:00:00+00:00"}
