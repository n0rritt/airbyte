#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import os.path

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer


API_REGIONS = {
    "United States": "api",
    "Europe": "api.eu",
    "India": "api.in",
    "Australia": "api.au"
}


# Basic full refresh stream
class FreshchatStream(HttpStream, ABC):
    url_base = "https://{api_region}.freshchat.com/v2/"
    order_field = None
    order_type = "asc"
    items_per_page = 100
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    # name of key in the JSON response that contains all desired data records
    records_key = None

    # name of key that is the primary identifier in each JSON record; this is usually the "id" field, but can be overridden in sub classes
    primary_key = "id"

    def __init__(self, api_region: str, **kwargs):
        super().__init__(**kwargs)
        self.url_base = self.url_base.format(api_region=api_region)

        # if no records_key is defined in the sub class we take the lowercased class name by default
        if not self.records_key:
            self.records_key = self.name.lower()

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        pagination = response.json().get("pagination", {})
        current_page = int(pagination.get("current_page"))
        total_pages = int(pagination.get("total_pages"))

        if current_page < total_pages:
            return {"page": current_page + 1}
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"page": next_page_token.get("page"), "items_per_page": self.items_per_page}
        if self.order_field:
            params["sort_by"] = self.order_field
            params["sort_order"] = self.order_type
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        records = response.json().get(self.records_key, [])
        yield from records

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        # by default we consider the lowercase of the class name as the API path
        # for special cases where this is not the case this method has to be overridden in the sub class
        return self.name.lower()


class Agents(FreshchatStream):
    """
    API docs: https://developers.freshchat.com/api/#agent
    """


class Channels(FreshchatStream):
    """
    API docs: https://developers.freshchat.com/api/#channel
    """


class Groups(FreshchatStream):
    """
    API docs: https://developers.freshchat.com/api/#group
    """


# Basic incremental stream
class IncrementalFreshchatStream(FreshchatStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Employees(IncrementalFreshchatStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


# Source
class SourceFreshchat(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        auth = TokenAuthenticator(token=config["api_key"], auth_method="Bearer").get_auth_header()
        url = f'https://{API_REGIONS.get(config["region"])}.freshchat.com/v2/channels'
        try:
            session = requests.get(url, headers=auth)
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"], auth_method="Bearer").get_auth_header()
        api_region = API_REGIONS.get(config["region"])
        args = {"authenticator": auth, "api_region": api_region}
        return [
            Agents(**args),
            Channels(**args),
            #Conversations(**args),
            Groups(**args),
            #OutboundMessages(**args),
            #Users(**args),
            ChatTranscriptReport(**args),
            ConversationCreatedReport(**args),
            ConversationResolvedReport(**args),
            ConversationResolutionLabelReport(**args),
            CsatScoreReport(**args),
            FirstResponseTimeReport(**args),
            ResponseTimeReport(**args),
            ResolutionTimeReport(**args),
            ConversationAgentAssignedReport(**args),
            ConversationGroupAssignedReport(**args),
            AgentActivityReport(**args),
            AgentIntelliassignActivityReport(**args)
        ]
